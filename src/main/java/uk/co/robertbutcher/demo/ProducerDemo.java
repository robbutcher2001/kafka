package uk.co.robertbutcher.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

  public static void main(String[] args) {
    System.out.println("Hello World");

    // Create Producer properties
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    // Create a Producer record
    ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world, latest one. One more post.");

    // Send data - asynchronous
    producer.send(record);
    producer.flush();
    producer.close();
  }
}
