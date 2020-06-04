package tests;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import helpers.Configs;

public class EnvoyTest {

    private static final Logger LOG = LoggerFactory.getLogger(EnvoyTest.class);

    private static final int WORKERS_SIZE = 8;
    private static final int ITERATIONS_PER_WORKER = 10;
    private static final int APPLES_MESSAGES = 5;
    private static final int BANANAS_MESSAGES = 3;

    private static final String ENVOY = "localhost:19092";
    private static final String CLUSTER_1 = "localhost:9092";
    private static final String CLUSTER_2 = "localhost:9093";

    private static AtomicLong diff = new AtomicLong();

    @Test
    public void test()
            throws Exception {

        final ExecutorService es = Executors.newFixedThreadPool(WORKERS_SIZE);
        final ExecutorCompletionService<Integer> ecs = new ExecutorCompletionService<>(es);
        for (int i = 0; i < WORKERS_SIZE; ++i) {
            final int id = i;
            final Callable<Integer> task = () -> doWork(id);
            ecs.submit(task);
        }
        int sumFailures = 0;
        for (int i = 0; i < WORKERS_SIZE; ++i) {
            final int workerFailures = ecs.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS).get();
            sumFailures += workerFailures;
        }
        LOG.info("sum failures = {}, diff = {}", sumFailures, diff.get());
        assertThat(sumFailures, equalTo(0));
    }

    private Integer doWork(final int id)
            throws Exception {

        LOG.info("starting worker {}", id);

        final Properties pc = Configs.makeProducerConfiguration();
        pc.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ENVOY);
        pc.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        final Properties cc1 = Configs.makeConsumerConfiguration();
        cc1.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_1);
        final Properties cc2 = Configs.makeConsumerConfiguration();
        cc2.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_2);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(pc);
        final KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(cc1);
        final KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(cc2);

        final TopicPartition c1p = new TopicPartition("apples", 0);
        consumer1.assign(Arrays.asList(c1p));
        final TopicPartition c2p = new TopicPartition("bananas", 0);
        consumer2.assign(Arrays.asList(c2p));

        final List<ProducerRecord<String, String>> ar = new ArrayList<>();
        final List<ProducerRecord<String, String>> br = new ArrayList<>();
        final List<Future<RecordMetadata>> af = new ArrayList<>();
        final List<Future<RecordMetadata>> bf = new ArrayList<>();

        int failures = 0;

        for (int it = 0; it < ITERATIONS_PER_WORKER; ++it) {

            if (it % 10 == 0) {
                LOG.info("iteration {} in worker {}", it, id);
            }

            // send

            for (int i = 0; i < APPLES_MESSAGES; ++i) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(
                        "apples", 0, makeString(10), makeString(30));
                af.add(producer.send(record));
                ar.add(record);
            }

            for (int i = 0; i < BANANAS_MESSAGES; ++i) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(
                        "bananas", 0, makeString(10), makeString(30));
                bf.add(producer.send(record));
                br.add(record);
            }

            // receive & check

            failures += check(consumer1, ar, af);
            failures += check(consumer2, br, bf);
        }

        producer.close();
        consumer1.close();
        consumer2.close();

        return failures;
    }

    private int check(final KafkaConsumer<String, String> consumer,
                      final List<ProducerRecord<String, String>> sentRecords,
                      final List<Future<RecordMetadata>> futures)
            throws Exception {

        int result = 0;
        for (int i = 0; i < sentRecords.size(); ++i) {
            final ProducerRecord<String, String> record = sentRecords.get(i);
            final long offset = futures.get(i).get().offset();
            consumer.seek(consumer.assignment().iterator().next(), offset);
            boolean found = false;
            consumer: while (!found) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    result++;
                    break consumer;
                }
                for (final ConsumerRecord<String, String> rcv : records) {
                    final boolean ok = check(record, rcv);
                    if (ok) {
                        found = true;
                        break consumer;
                    }
                    diff.addAndGet(rcv.offset() - offset);
                }
            }
        }
        sentRecords.clear();
        futures.clear();
        return result;
    }

    private boolean check(final ProducerRecord<String, String> sent, final ConsumerRecord<String, String> rcv) {
        if (Objects.equals(sent.key(), rcv.key()) && Objects.equals(sent.value(), rcv.value())) {
            return true;
        }
        else {
            return false;
        }
    }

    private static String makeString(final int sz) {
        String res = "{";
        for (int i = 0; i < sz; ++i) {
            final char ch = (char) ThreadLocalRandom.current().nextInt('a', 'z' + 1);
            res += ch;
        }
        res += "}";
        return res;
    }

}
