package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class MyKafkaUtils {
    public static String broker = "192.168.202.100:9092,192.168.202.101:9092,192.168.202.102:9092";



    private MyKafkaUtils() {
        System.out.println("Kafka Init ---");
    }

    public static FlinkKafkaProducer<String> getProducer(String topic) {
        FlinkKafkaProducer<String> producer = null;
        producer = new FlinkKafkaProducer<String>(broker, topic, new SimpleStringSchema());
        return producer;
    }


    public static FlinkKafkaConsumer<String> getConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        return consumer;
    }
}