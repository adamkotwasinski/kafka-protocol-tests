package tests;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import helpers.Configs;

public class ConsumerTests
    extends BaseTest {

    @Test
    public void shouldCommitOffsets() {
        final String topic = "test_consumer_offset_commit";

        final Properties properties = Configs.makeConsumerConfiguration();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer_offset_commit");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {
            consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));

            final ConsumerRecords<String, String> pollResult = consumer.poll(Duration.ofMillis(1000));
            assertThat(pollResult, emptyIterable());

            consumer.commitSync();

        }
        finally {
            consumer.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("offset_commit", 1);
    }

    @Test
    public void shouldListOffsets() {
        final String topic = "test_consumer_list_offset";

        final Properties properties = Configs.makeConsumerConfiguration();
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {
            final Map<TopicPartition, Long> result1 = consumer
                    .beginningOffsets(Arrays.asList(new TopicPartition(topic, 0)));
            assertThat(result1, notNullValue());

            final Map<TopicPartition, Long> result2 = consumer
                    .endOffsets(Arrays.asList(new TopicPartition(topic, 0)));
            assertThat(result2, notNullValue());

        }
        finally {
            consumer.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("list_offset", 2);
    }

}
