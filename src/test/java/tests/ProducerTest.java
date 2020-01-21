package tests;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class ProducerTest
    extends BaseTest {

    @Test
    public void shouldSendAndReceiveMessages()
            throws Exception {

        final Properties producerProperties = new Properties();
        producerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        final String topic = "test_kafka_producer_and_consumer";
        final int partition = 0;

        final int messagesToSend = 100;
        for (int i = partition; i < messagesToSend; ++i) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null,
                    "some_message_text");
            final Future<RecordMetadata> future = producer.send(record);
            final RecordMetadata recordMetadata = future.get();
            assertThat(recordMetadata.hasOffset(), equalTo(true));
            assertThat(recordMetadata.offset(), greaterThanOrEqualTo(0L));
        }

        final Properties consumerProperties = new Properties();
        consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 100);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

        final List<ConsumerRecord<String, String>> receivedMessages = new ArrayList<>();
        while (receivedMessages.size() < messagesToSend) {
            final Iterable<ConsumerRecord<String, String>> pollResult = consumer.poll(Duration.ofMillis(1000));
            Iterables.addAll(receivedMessages, pollResult);
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

        consumer.close();
        producer.close();
    }

}
