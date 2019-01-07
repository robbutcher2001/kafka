package uk.co.robertbutcher.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private static Logger logger;

  public static void main(String[] args) throws Exception {
    logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    // Create Producer properties
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < 10; i++) {
      // Create a Producer record
      final String topic = "first_topic";
      final String key = "id_" + i;
      final String value = "hello world " + i;

      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("Working with: " + i);

      // Send data - forced to synchronous
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e == null) {
            // Record created successfully
            logger.info("Topic meta: \n" + recordMetadata.topic());
            logger.info("Partition meta: " + recordMetadata.partition());
            logger.info("Offset meta: \n" + recordMetadata.offset());
            logger.info("Timestamp meta: \n" + recordMetadata.timestamp());
          }
          else {
            logger.error("Got an error", e);
          }
        }
      }).get();
    }

    producer.flush();
    producer.close();
  }
}
