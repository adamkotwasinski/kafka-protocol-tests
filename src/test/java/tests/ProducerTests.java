package tests;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import helpers.Configs;

public class ProducerTests
    extends BaseTest {

    @Test
    public void shouldSupportTransactions() {
        final String topic = "test_producer_transaction_commit";
        final int transactionCount = 5;

        final Properties properties = Configs.makeProducerConfiguration();
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transactional-id");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            producer.initTransactions();

            for (int i = 0; i < transactionCount; ++i) {
                producer.beginTransaction();

                producer.send(new ProducerRecord<>(topic, 0, null, "bytes"));

                producer.commitTransaction();
            }
        }
        finally {
            producer.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("init_producer_id", 1);
        this.scraper.assertMetricIncrease("add_partitions_to_txn", transactionCount);
        this.scraper.assertMetricIncrease("end_txn", transactionCount);
    }

    @Test
    public void shouldSupportAbortedTransactions() {
        final String topic = "test_producer_transaction_abort";
        final int transactionCount = 5;

        final Properties properties = Configs.makeProducerConfiguration();
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-transactional-id");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            producer.initTransactions();

            for (int i = 0; i < transactionCount; ++i) {
                producer.beginTransaction();

                producer.send(new ProducerRecord<>(topic, 0, null, "bytes"));

                producer.abortTransaction();
            }
        }
        finally {
            producer.close();
        }

        this.scraper.collectFinalMetrics();
        this.scraper.assertMetricIncrease("metadata", 1);
        this.scraper.assertMetricIncrease("init_producer_id", 1);
        this.scraper.assertMetricIncrease("add_partitions_to_txn", 0);
        this.scraper.assertMetricIncrease("end_txn", 0);
    }

}
