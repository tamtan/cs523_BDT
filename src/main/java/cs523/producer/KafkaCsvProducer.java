package cs523.producer;

import au.com.bytecode.opencsv.CSVReader;
import cs523.config.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.util.Properties;
import java.util.UUID;

public class KafkaCsvProducer {
    private static String KafkaBrokerEndpoint = Constants.KAFKA_BROKERS;
    private static String KafkaTopic = Constants.TOPIC;
    private static String CsvFile = null;
    public static final String SEPARATOR = "-------------------------";

    private Producer<String, String> ProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) {
        if (args != null) {
            CsvFile = args[0];
        }

        KafkaCsvProducer kafkaProducer = new KafkaCsvProducer();
        kafkaProducer.PublishMessages();
    }

    private void PublishMessages() {
        final Producer<String, String> CsvProducer = ProducerProperties();
        try {
            CSVReader csvReader = new CSVReader(new FileReader(CsvFile), ',');
            String[] nextLine = csvReader.readNext();//the header of the data.
            while ((nextLine = csvReader.readNext()) != null) {

                if (null != nextLine) {
                    String line = getCoronaTweeterString(nextLine);
                    final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(
                            KafkaTopic, UUID.randomUUID().toString(), line
                    );

                    CsvProducer.send(CsvRecord, (metadata, exception) -> {
                        if (metadata != null) {
                            System.out.println("CsvData: -> " + CsvRecord.key() + " | " + CsvRecord.value());
                        } else {
                            System.out.println("Error Sending Csv Record -> " + CsvRecord.value());
                        }
                    });
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getCoronaTweeterString(String[] nextLine) {
        StringBuilder builder = new StringBuilder();
        for (String s :
                nextLine) {
            builder.append(s + SEPARATOR);
        }
        return builder.toString();
    }
}