package cs523.hbase;

import cs523.model.CoronaTweeter;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringJoiner;

@Log4j
public class CoronaTweeterRepository implements Serializable {

	private static final long serialVersionUID = 1L;

	private static CoronaTweeterRepository instance;

	private static final String TABLE_NAME = "corona_tweeter";
	private static final String CF = "cf";

	public static final String C_USER_NAME = "userName";
	public static final String C_SCREEN_NAME = "screenName";
	public static final String C_LOCATION = "location";
	public static final String C_TWEET_AT = "tweetAt";
	public static final String C_ORIGINAL_TWEET = "originalTweet";
	public static final String C_SENTIMENT = "sentiment";

	private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");

	private CoronaTweeterRepository() {}

	public static CoronaTweeterRepository getInstance() {
		if (instance == null) {
			instance = new CoronaTweeterRepository();
		}
		return instance;
	}

	public void createTable() {
		Connection connection = HbaseConnection.getInstance();
		try (Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF).setCompressionType(Algorithm.NONE));
			if (!admin.tableExists(table.getTableName())) {
				log.info("Creating table ");
				admin.createTable(table);
				log.info("Table created");
			}
		} catch (IOException ex) {
			log.error(ex.getMessage());
		}
	}

	public void putAll(Map<String, CoronaTweeter> data) throws IOException {
		Connection connection = HbaseConnection.getInstance();
		try (Table tb = connection.getTable(TableName.valueOf("obj"))) {
			ArrayList<Put> ls = new ArrayList<>();
			for (String k : data.keySet()) {
				ls.add(putObject(k, data.get(k)));
			}
			tb.put(ls);
		}
	}

	public void put(String key, CoronaTweeter obj) throws IOException {
		Connection connection = HbaseConnection.getInstance();
		try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
			tb.put(putObject(key, obj));
		}
	}

	private Put putObject(String key, CoronaTweeter obj) {
		Put p = new Put(Bytes.toBytes(key));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_USER_NAME), Bytes.toBytes(obj.getUserName()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_SCREEN_NAME), Bytes.toBytes(obj.getScreenName()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_LOCATION), Bytes.toBytes(obj.getLocation()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_TWEET_AT), Bytes.toBytes(obj.getTweetAt().format(formatter)));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_ORIGINAL_TWEET), Bytes.toBytes(obj.getOriginalTweet()));
		p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(C_SENTIMENT), Bytes.toBytes(obj.getSentiment()));
		return p;
	}

	public CoronaTweeter get(Connection connection, String key) throws IOException {
		try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
			Get g = new Get(Bytes.toBytes(key));
			Result result = tb.get(g);
			if (result.isEmpty()) {
				return null;
			}
			byte [] value1 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_USER_NAME));
			byte [] value2 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_SCREEN_NAME));
			byte [] value3 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_LOCATION));
			byte [] value4 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_TWEET_AT));
			byte [] value5 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_ORIGINAL_TWEET));
			byte [] value6 = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(C_SENTIMENT));

			return CoronaTweeter.of(
					Bytes.toString(value1),
					Bytes.toString(value2),
					Bytes.toString(value3),
					LocalDateTime.parse(Bytes.toString(value4), formatter),
					Bytes.toString(value5), Bytes.toString(value6)

			);
		}
	}

	public void save(Configuration config, JavaRDD<CoronaTweeter> record)
				throws Exception {
		Job job = Job.getInstance(config);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		job.setOutputFormatClass(TableOutputFormat.class);
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = record.mapToPair(new MyPairFunction());
		hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}

	class MyPairFunction implements PairFunction<CoronaTweeter, ImmutableBytesWritable, Put> {

		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<ImmutableBytesWritable, Put> call(CoronaTweeter record) throws Exception {
			StringJoiner key = new StringJoiner("|");
			key.add(record.getUserName());
			key.add(record.getScreenName());
			key.add(record.getLocation());
			key.add(record.getTweetAt().format(formatter));
			key.add(record.getOriginalTweet());
			key.add(record.getSentiment());
			Put put = putObject(key.toString(), record);
			return new Tuple2<>(new ImmutableBytesWritable(), put);
		}

	}

}
