package helpers;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public class Scraper {

    private static final Logger LOG = LoggerFactory.getLogger(Scraper.class);

    private static final String STATS_RESOURCE = "http://localhost:9901/stats";

    private static final String FILTER_NAME = "myfilter"; // filter name in Envoy's yaml file
    private static final String FILTER_METRIC_PREFIX = String.format("kafka.%s.", FILTER_NAME);

    private final TreeMap<String, Long> initialMetrics;
    private final TreeMap<String, Long> finalMetrics;

    public Scraper() {
        this.initialMetrics = new TreeMap<>();
        this.finalMetrics = new TreeMap<>();
    }

    public void collectInitialMetrics() {
        collectMetrics(this.initialMetrics);
    }

    public void collectFinalMetrics() {
        collectMetrics(this.finalMetrics);
    }

    private void collectMetrics(/* out */ final TreeMap<String, Long> sink) {
        final String rawData = getRawData();
        final List<String> rows = Splitter.on("\n").splitToList(rawData);
        final List<String> filterCounterRows = rows.stream()
                .filter(x -> x.startsWith(FILTER_METRIC_PREFIX))
                .filter(x -> !x.contains("_response_duration: "))
                .collect(Collectors.toList());

        for (final String row : filterCounterRows) {
            final String[] tokens = row.split("\\s*:\\s*", 2);
            final String metric = tokens[0].substring(FILTER_METRIC_PREFIX.length());
            final long value = Long.valueOf(tokens[1]);
            sink.put(metric, value);
        }
    }

    private String getRawData() {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            LOG.info("Collecting from [{}]", STATS_RESOURCE);
            final HttpUriRequest request = new HttpGet(STATS_RESOURCE);
            try (CloseableHttpResponse response = client.execute(request)) {
                final String payload = EntityUtils.toString(response.getEntity());
                return payload;
            }
        }
        catch (final Exception e) {
            throw new IllegalStateException("collection failure", e);
        }
    }

    public void assertMetricIncrease(final String messageName, final int expectedIncrease) {
        final String requestMetricName = String.format("request.%s_request", messageName);
        final String responseMetricName = String.format("response.%s_response", messageName);
        {
            final long initialValue = this.initialMetrics.getOrDefault(requestMetricName, 0L);
            final long finalValue = this.finalMetrics.get(requestMetricName);
            assertThat(finalValue, greaterThanOrEqualTo(initialValue + expectedIncrease));
        }
        {
            final long initialValue = this.initialMetrics.getOrDefault(responseMetricName, 0L);
            final long finalValue = this.finalMetrics.get(responseMetricName);
            assertThat(finalValue, greaterThanOrEqualTo(initialValue + expectedIncrease));
        }
    }

}
