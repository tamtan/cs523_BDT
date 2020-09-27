package cs523.sparksql;

import cs523.config.Constants;
import cs523.hbase.CoronaTweeterRepository;
import cs523.hbase.HbaseConnection;
import cs523.model.CoronaTweeter;
import cs523.model.HbaseCoRecord;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cs523.hbase.CoronaTweeterRepository.TABLE_NAME;

public class CoronaReview {
	private static SparkSession spark;
	
	public static void ReadRecords(JavaSparkContext jsc) throws IOException{
//		CoronaTweeterRepository repo = CoronaTweeterRepository.getInstance();
//		List<HbaseCoRecord> list = new ArrayList<>();
//		for (String key: keys) {
//			CoronaTweeter record = repo.get(HbaseConnection.getInstance(), key);
//			list.add(HbaseCoRecord.of(record));
//		}
//		spark = SparkSession.builder()
//				.appName(Constants.APP_NAME)
//				.config("option name", "option value")
//				.master(Constants.MASTER)
//				.getOrCreate();
//		spark.createDataFrame(list, HbaseCoRecord.class).createOrReplaceTempView(TABLE_NAME);
/////////////////////////////////////////////////////////////////
//		HBaseConfiguration conf = new HBaseConfiguration();
//		conf.set("hbase.zookeeper.quorum", "hbase-1.example.com")
//
//// the latest HBaseContext will be used afterwards
//		new HBaseContext(jsc, conf);
//
//		val df = spark.sql.read.format("org.apache.hadoop.hbase.spark")
//				.option("hbase.columns.mapping",
//						"name STRING :key, email STRING c:email, " +
//								"birthDate DATE p:birthDate, height FLOAT p:height")
//				.option("hbase.table", "person")
//				.load()
//		df.createOrReplaceTempView("personView")
//
//		val results = sql.sql("SELECT * FROM personView WHERE name = 'alice'")
//		results.show()
		///////////////////////////////////////////////////////////////////////
//		SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME)
//				.setMaster(Constants.MASTER);
//
//		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("spark.hbase.host", "localhost");
		jsc.hadoopConfiguration().set("spark.hbase.port", "60000");

		SQLContext sqlContext = new SQLContext(jsc);

//		Map<String, String> optionsMap = new HashMap<>();
//
//		String htc = HBaseTableCatalog.tableCatalog();
//		String catalog = "{\n" +
//				"\t\"table\":{\"namespace\":\"default\", \"name\":\"test\", \"tableCoder\":\"PrimitiveType\"},\n" +
//				"    \"rowkey\":\"key\",\n" +
//				"    \"columns\":{\n" +
//				"\t    \"rowkey\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\n" +
//				"\t    \"value\":{\"cf\":\"value\", \"col\":\"value\", \"type\":\"string\"}\n" +
//				"    }\n" +
//				"}";
//		optionsMap.put(htc, catalog);
// optionsMap.put(HBaseRelation.MIN_STAMP(), "123");
// optionsMap.put(HBaseRelation.MAX_STAMP(), "456");

		String query =  " SELECT * FROM " + TABLE_NAME
				+ " LIMIT 15 ";
		DataFrame dataset = sqlContext.sql(query);
		dataset.show();
//		DataFrame dataset = sqlContext.read().options(optionsMap).format("org.apache.spark.sql.execution.datasources.hbase").load();
		System.out.println("troi oi chan qua");
	}
	
	public static void GetFirst15Record() {
//		" SELECT * FROM corona_tweeter LIMIT 15 "
		String query =  " SELECT * FROM " + TABLE_NAME 
					  + " LIMIT 15 ";
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show();
		System.out.println(query);
		System.out.println("====================================================");
	}
	
	public static void GetFirst15PositiveCase() {
		String query =  " SELECT * FROM " + TABLE_NAME 
					  + " WHERE sentiment = 'Positive' "
					  + " LIMIT 15 ";
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show();
		System.out.println(query);
		System.out.println("====================================================");
	}
	
	public static void CountNumberOfCorrespondingCases() {
		String query =  " SELECT sentiment, COUNT(sentiment)"
					  + " FROM " + TABLE_NAME
					  + " GROUP BY sentiment";
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show();
		System.out.println(query);
		System.out.println("====================================================");
	}
	
	public static void NumOfCasesPerDayIn15Days() {
		String query =  " SELECT tweetAt, COUNT(tweetAt) FROM " + TABLE_NAME
				+ " GROUP BY tweetAt"
				+ " LIMIT 15 ";
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show();
		System.out.println(query);
		System.out.println("====================================================");
	}
}
