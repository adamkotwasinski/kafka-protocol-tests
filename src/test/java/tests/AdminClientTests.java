package tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.junit.Test;

import helpers.Configs;

public class AdminClientTests
    extends BaseTest {

    @Test
    public void shouldDeleteConsumerGroups()
            throws Exception {

        final Properties properties = Configs.makeAdminConfiguration();
        final AdminClient admin = KafkaAdminClient.create(properties);
        try {
            final DeleteConsumerGroupsResult dcg = admin
                    .deleteConsumerGroups(Arrays.asList("group1", "group2", "group3"));

            try {
                dcg.all().get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (!(cause instanceof GroupIdNotFoundException)) {
                    throw e;
                }
            }
        }
        finally {
            admin.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("delete_groups", 1);
    }

    @Test
    public void shouldDeleteRecords()
            throws Exception {

        final TopicPartition topicPartition = new TopicPartition("test_delete_records", 0);

        final Properties consumerProperties = Configs.makeConsumerConfiguration();
        final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerProperties);
        try {
            consumer.assign(Arrays.asList(topicPartition));
            consumer.poll(Duration.ofMillis(500));
        }
        finally {
            consumer.close();
        }

        final Properties properties = Configs.makeAdminConfiguration();
        final AdminClient admin = KafkaAdminClient.create(properties);
        try {
            final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(topicPartition, RecordsToDelete.beforeOffset(0L));
            final DeleteRecordsResult drr = admin.deleteRecords(recordsToDelete);
            drr.all().get();
        }
        finally {
            admin.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("delete_records", 1);
    }

    @Test
    public void shouldListConsumerGroups()
            throws Exception {

        final Properties properties = Configs.makeAdminConfiguration();
        final AdminClient admin = KafkaAdminClient.create(properties);
        try {
            final ListConsumerGroupsResult lcgr = admin.listConsumerGroups();
            lcgr.all().get();
        }
        finally {
            admin.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("list_groups", 1);
    }

}
