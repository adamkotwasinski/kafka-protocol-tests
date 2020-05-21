package tests;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import helpers.Configs;

/**
 * Contains tests that are right now present in 'kafka_broker_integration_test.py'.
 */
public class OriginalTests
    extends BaseTest {

    @Test
    public void shouldSendAndReceiveMessages()
            throws Exception {

        final String topic = "test_kafka_producer_and_consumer" + System.currentTimeMillis();
        final int partition = 0;
        final int messagesToSend = 1;

        final Properties producerProperties = Configs.makeProducerConfiguration();
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

    }

}
