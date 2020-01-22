package tests;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.google.common.collect.Iterables;

/**
 * Contains tests that are right now present in 'kafka_broker_integration_test.py'.
 */
public class OriginalTests
    extends BaseTest {

    @Rule
    public final Timeout timeout = new Timeout(30, TimeUnit.SECONDS);

    private static final String ENVOY_KAFKA_ADDRESS = "localhost:19092";

    // replaces Python test 'test_kafka_consumer_with_no_messages_received'
    @Test
    public void shouldPoll() {

        final Properties consumerProperties = new Properties();
        consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY_KAFKA_ADDRESS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        try {
            consumer.assign(Arrays.asList(new TopicPartition("test_kafka_consumer_with_no_messages_received", 0)));

            for (int i = 0; i < 10; ++i) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                assertThat(records, emptyIterable());
            }
        }
        finally {
            consumer.close();
        }

        this.scraper.collectFinalMetrics();
        // 'consumer.poll()' can translate into 0 or more fetch requests.
        // We have set API timeout to 1000ms, while fetch_max_wait is 500ms.
        // This means that consumer will send roughly 2 (1000/500) requests per API call (so 20 total).
        // So increase of 10 (half of that value) should be safe enough to test.
        this.scraper.assertMetricIncrease("fetch", 10);
        // Metadata is used by consumer to figure out current partition leader.
        this.scraper.assertMetricIncrease("metadata", 1);
    }

    // replaces Python test 'test_kafka_producer_and_consumer'
    @Test
    public void shouldSendAndReceiveMessages()
            throws Exception {

        final String topic = "test_kafka_producer_and_consumer";
        final int partition = 0;
        final int messagesToSend = 100;

        final Properties producerProperties = new Properties();
        producerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY_KAFKA_ADDRESS);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        try {
            for (int i = partition; i < messagesToSend; ++i) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null,
                        "some_message_text");
                final Future<RecordMetadata> future = producer.send(record);
                final RecordMetadata recordMetadata = future.get();
                assertThat(recordMetadata.hasOffset(), equalTo(true));
                assertThat(recordMetadata.offset(), greaterThanOrEqualTo(0L));
            }
        }
        finally {
            producer.close();
        }

        final Properties consumerProperties = new Properties();
        consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY_KAFKA_ADDRESS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 100);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        try {
            consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

            final List<ConsumerRecord<String, String>> receivedMessages = new ArrayList<>();
            while (receivedMessages.size() < messagesToSend) {
                final Iterable<ConsumerRecord<String, String>> pollResult = consumer.poll(Duration.ofMillis(1000));
                Iterables.addAll(receivedMessages, pollResult);
            }
        }
        finally {
            consumer.close();
        }

        this.scraper.collectFinalMetrics();

        // Producer had to send separate requests with message payloads
        // (because we were waiting for metadata after each one).
        this.scraper.assertMetricIncrease("produce", messagesToSend);
        // 'fetch_max_bytes' was set to a very low value, so client will need to send a FetchRequest multiple times
        // to broker to get all 100 messages (otherwise all 100 records could have been received in one go).
        this.scraper.assertMetricIncrease("fetch", 20);
        // Both producer & consumer had to fetch cluster metadata.
        this.scraper.assertMetricIncrease("metadata", 2);
    }

    // replaces Python test 'test_consumer_with_consumer_groups'
    @Test
    public void shouldSupportConsumerGroups()
            throws Exception {

        final int consumerCount = 10;
        final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; ++i) {
            final Properties consumerProperties = new Properties();
            consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY_KAFKA_ADDRESS);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
            consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("test-%s", i));
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
            consumer.subscribe(Arrays.asList("test_consumer_with_consumer_groups"));
            consumers.add(consumer);
        }

        final ExecutorService executor = Executors.newFixedThreadPool(consumers.size());
        final List<Future<?>> futures = new ArrayList<>();
        for (final KafkaConsumer<String, String> consumer : consumers) {
            final Runnable worker = () -> {

                final int pollOperationCount = 10;
                for (int i = 0; i < pollOperationCount; ++i) {
                    consumer.poll(Duration.ofMillis(1000));
                }

            };
            final Future<?> future = executor.submit(worker);
            futures.add(future);
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        for (final Future<?> future : futures) {
            // Workers should complete without problems.
            assertThat(future.get(), nullValue());
        }

        for (final KafkaConsumer<String, String> consumer : consumers) {
            consumer.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("api_versions", consumerCount);
        this.scraper.assertMetricIncrease("metadata", consumerCount);
        this.scraper.assertMetricIncrease("join_group", consumerCount);
        this.scraper.assertMetricIncrease("find_coordinator", consumerCount);
        this.scraper.assertMetricIncrease("leave_group", consumerCount);
    }

    // replaces Python test 'test_admin_client'
    @SuppressWarnings("deprecation")
    @Test
    public void shouldPerformAdminOperations()
            throws Exception {

        final String topic = "test_admin_client";

        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY_KAFKA_ADDRESS);
        final AdminClient admin = KafkaAdminClient.create(properties);

        try {
            // Create a topic with 3 partitions.
            final CreateTopicsResult ctr = admin.createTopics(Arrays.asList(new NewTopic(topic, 3, (short) 1)));
            ctr.all().get();

            // Alter topic (change some Kafka-level property).
            final Map<ConfigResource, Config> configs = new HashMap<>();
            configs.put(
                    new ConfigResource(Type.TOPIC, topic),
                    new Config(Arrays.asList(new ConfigEntry("flush.messages", "42"))));
            final AlterConfigsResult acr = admin.alterConfigs(configs);
            acr.all().get();

            // Add 2 more partitions to topic.
            final Map<String, NewPartitions> newPartitions = new HashMap<>();
            newPartitions.put(topic, NewPartitions.increaseTo(5));
            final CreatePartitionsResult cpr = admin.createPartitions(newPartitions);
            cpr.all().get();

            // Delete a topic.
            final DeleteTopicsResult dtr = admin.deleteTopics(Arrays.asList(topic));
            dtr.all().get();
        }
        finally {
            admin.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("create_topics", 1);
        this.scraper.assertMetricIncrease("alter_configs", 1);
        this.scraper.assertMetricIncrease("create_partitions", 1);
        this.scraper.assertMetricIncrease("delete_topics", 1);
    }

}
