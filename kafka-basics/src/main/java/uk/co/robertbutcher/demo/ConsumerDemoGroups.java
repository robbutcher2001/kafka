package uk.co.robertbutcher.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private static final String TOPIC = "first_topic";
  private static final String GROUP_ID = "third_app";
  private static Logger logger;

  public static void main(String[] args) {
    logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    // Create Consumer properties
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Create the Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    // Subscribe to topic(s)
    consumer.subscribe(Arrays.asList(TOPIC));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        logger.info(record.key() + ", " + record.value());
        logger.info(record.partition() + ", " + record.offset() + "\n");
      }
    }
  }
}
