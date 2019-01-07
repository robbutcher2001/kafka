package uk.co.robertbutcher.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private static final String TOPIC = "first_topic";
  private static Logger logger;

  public static void main(String[] args) {
    logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    // Create Consumer properties
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Create the Consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    // Assign and seek - mostly used to replay data or fetch a specific message
    TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
    long offsetToReadFrom = 15L;
    consumer.assign(Arrays.asList(partitionToReadFrom));
    consumer.seek(partitionToReadFrom, offsetToReadFrom);

    int numberOfMessagesToRead = 5;
    int numberOfMessagesReadSoFar = 0;
    boolean keepOnReading = true;

    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        numberOfMessagesReadSoFar++;
        logger.info(record.key() + ", " + record.value());
        logger.info(record.partition() + ", " + record.offset() + "\n");

        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false;
          break;
        }
      }
    }

    logger.info("Exiting the application");
  }
}
