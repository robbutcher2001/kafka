package uk.co.robertbutcher.consumer;

// import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ElasticSearchConsumer {
  private final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private final String GROUP_ID = "kafka-demo-elasticsearch";

  private final String ELASTICSEARCH_HOSTNAME;
  private final String ELASTICSEARCH_USERNAME;
  private final String ELASTICSEARCH_PASSWORD;

  private static Logger logger;

  public ElasticSearchConsumer() {
    logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    final Map<String, String> env = System.getenv();
    ELASTICSEARCH_HOSTNAME = env.get("ELASTICSEARCH_HOSTNAME");
    ELASTICSEARCH_USERNAME = env.get("ELASTICSEARCH_USERNAME");
    ELASTICSEARCH_PASSWORD = env.get("ELASTICSEARCH_PASSWORD");
  }

  public RestHighLevelClient createClient() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
      new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));

    RestClientBuilder builder = RestClient.builder(new HttpHost(ELASTICSEARCH_HOSTNAME, 443, "https"))
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      });

    return new RestHighLevelClient(builder);
  }

  public KafkaConsumer<String, String> createConsumer(final String topic) {
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
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  public static void main(String[] args) throws Exception {
    final ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
    final RestHighLevelClient client = elasticSearchConsumer.createClient();

    KafkaConsumer<String, String> consumer = elasticSearchConsumer.createConsumer("twitter_tweets");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        logger.info(record.value().toString());
        // Where we insert data into ElasticSearch
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
          .source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info("Response ID [" + id + "]");

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          ie.printStackTrace();
        }
      }
    }

    // client.close();
  }
}
