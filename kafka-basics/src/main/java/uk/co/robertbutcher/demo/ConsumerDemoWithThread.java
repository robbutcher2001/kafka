package uk.co.robertbutcher.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.Properties;

public class ConsumerDemoWithThread {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private static final String GROUP_ID = "fourth_app";
  private static final String TOPIC = "first_topic";
  private static Logger logger;

  public static void main(String[] args) {
    new ConsumerDemoWithThread();
  }

  public ConsumerDemoWithThread() {
    logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    // Latch for dealing with multiple threads
    CountDownLatch latch = new CountDownLatch(1);

    // Create the ConsumerRunnable
    logger.info("Creating the consumer thread");
    Runnable consumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVER, GROUP_ID, TOPIC, latch);

    // Start the Thread
    Thread thread = new Thread(consumerRunnable);
    thread.start();

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Caught shutdown hook");
      ((ConsumerRunnable) consumerRunnable).shutdown();
      try {
        latch.await();
      } catch (InterruptedException ie) {
        ie.printStackTrace();
      }
      logger.info("Application has exited");
    }));

    try {
      latch.await();
    } catch (InterruptedException ie) {
      logger.error("Application got interrupted", ie);
    } finally {
      logger.info("Application is closing");
    }
  }

  public class ConsumerRunnable implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private CountDownLatch latch;
    private Logger logger;

    public ConsumerRunnable(String bootstrapServer,
      String groupId,
      String topic,
      CountDownLatch latch) {

      // Create Consumer properties
      Properties props = new Properties();
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
      props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // Create the Consumer
      this.consumer = new KafkaConsumer<String, String>(props);

      // Subscribe to topic(s)
      this.consumer.subscribe(Arrays.asList(topic));

      this.latch = latch;
      this.logger = LoggerFactory.getLogger(ConsumerRunnable.class);
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord record : records) {
            this.logger.info(record.key() + ", " + record.value());
            this.logger.info(record.partition() + ", " + record.offset() + "\n");
          }
        }
      } catch (WakeupException wue) {
        this.logger.info("Received shutdown signal!");
      } finally {
        this.consumer.close();
        // Informs the main code block that we are done with the Consumer
        this.latch.countDown();
      }
    }

    public void shutdown() {
      // Will interrupt the consumer.poll() and cause it to throw an WakeupException
      this.consumer.wakeup();
    }
  }
}
