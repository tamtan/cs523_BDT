package cs523;

import java.io.*;
import java.util.*;

import cs523.hbase.CoronaTweeterRepository;
import cs523.model.CoronaTweeter;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.hbase.client.*;

import cs523.hbase.HbaseConnection;
import cs523.model.Parser;
import cs523.sparksql.CoronaReview;

import cs523.config.Constants;
import kafka.serializer.StringDecoder;

import static cs523.config.Constants.APP_NAME;
import static cs523.config.Constants.MASTER;

public class App {

	public static void main(String[] args) {
		try (Connection connection = HbaseConnection.getInstance()) {
			SparkConf sparkConf = new SparkConf().setMaster(MASTER).setAppName(APP_NAME);
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			CoronaTweeterRepository repo = CoronaTweeterRepository.getInstance();
			repo.createTable();
            streamingFromKafka(sc);

//			String mode = "kafka";
//			String mode = "sql";
//			if (args.length > 0) mode = args[0];
//			if (mode.equals("sql")) sparkSql(sc);
//			if (mode.equals("kafka")) streamingFromKafka(sc);
		}catch (Exception e){
			System.out.println(e.getMessage());
		}
	}

    public static void streamingFromKafka(JavaSparkContext sc) {
        CoronaTweeterRepository repo = CoronaTweeterRepository.getInstance();
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", Constants.KAFKA_BROKERS);
        kafkaParams.put("fetch.message.max.bytes", String.valueOf(Constants.MESSAGE_SIZE));
        Set<String> topicName = Collections.singleton(Constants.TOPIC);
        Configuration hadoopConf = sc.hadoopConfiguration();

        try (JavaStreamingContext streamingContext = new JavaStreamingContext(sc, new Duration(1000))) {
            JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils.createDirectStream(
                    streamingContext, String.class,
                    String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicName);
            JavaDStream<CoronaTweeter> recoredRDDs = kafkaSparkPairInputDStream.map(Parser::parseCo);
            recoredRDDs.foreachRDD(rdd -> {
                if (!rdd.isEmpty()) {
                    repo.save(hadoopConf, rdd);
                }
            });

            streamingContext.start();
            streamingContext.awaitTermination();
        }
    }

    public static void sparkSql(JavaSparkContext jsc) throws IOException{
//		CoronaReview.ReadRecords(jsc);
//		CoronaReview.GetFirst15Record();
//		CoronaReview.GetFirst15PositiveCase();
//		CoronaReview.CountNumberOfCorrespondingCases();
	}
}
