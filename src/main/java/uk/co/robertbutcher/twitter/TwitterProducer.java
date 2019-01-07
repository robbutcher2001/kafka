package uk.co.robertbutcher.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
  private final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private final String TOPIC = "twitter_tweets";

  private final String CONSUMER_KEY;
  private final String CONSUMER_SECRET;
  private final String ACCESS_TOKEN;
  private final String ACCESS_SECRET;

  private static Logger logger;

  public TwitterProducer() {
    logger = LoggerFactory.getLogger(TwitterProducer.class);

    final Map<String, String> env = System.getenv();
    CONSUMER_KEY = env.get("CONSUMER_KEY");
    CONSUMER_SECRET = env.get("CONSUMER_SECRET");
    ACCESS_TOKEN = env.get("ACCESS_TOKEN");
    ACCESS_SECRET = env.get("ACCESS_SECRET");
    logger.info("Using this token: " + ACCESS_TOKEN);
  }

  public void run() {
    logger.info("Setting up");

    // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
    final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    // Create a Twitter client
    Client client = createTwitterClient(msgQueue);
    client.connect();

    // Create a Kafka Producer
    final KafkaProducer<String, String> producer = createKafkaProducer();

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Stopping application");
      logger.info("Shutting down the Twitter client");
      client.stop();
      logger.info("Closing the Kafka Producer");
      producer.flush();
      producer.close();
      logger.info("Exiting the application");
    }));

    // Loop to send tweets to Kafka
    while (!client.isDone()) {
      String msg = null;

      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        ie.printStackTrace();
        client.stop();
      }

      if (msg != null) {
        logger.info(msg);

        // Send data - asynchronous
        producer.send(new ProducerRecord<>(TOPIC, null, msg), new Callback() {

          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              logger.error("Got an error", e);
            }
          }
        });
      }
    }

    logger.info("End of application");
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {
    // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    List<String> terms = Lists.newArrayList("bitcoin");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);

    ClientBuilder builder = new ClientBuilder()
      .hosts(hosebirdHosts)
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }

  public KafkaProducer<String, String> createKafkaProducer() {
    // Create Producer properties
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    return new KafkaProducer<String, String>(props);
  }

  public static void main(String[] args) throws Exception {
    new TwitterProducer().run();
  }
}
