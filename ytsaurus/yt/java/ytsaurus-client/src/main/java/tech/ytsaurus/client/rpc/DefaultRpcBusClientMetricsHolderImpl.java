package tech.ytsaurus.client.rpc;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

/**
 * @author dkondra
 */
public class DefaultRpcBusClientMetricsHolderImpl implements DefaultRpcBusClientMetricsHolder {
    private static final MetricRegistry METRICS = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram REQUESTS_ACK_HISTOGRAM = METRICS.histogram(
            MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", "total")
    );
    private static final Histogram REQUESTS_RESPONSE_HISTOGRAM = METRICS.histogram(
            MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", "total")
    );
    private static final Counter ERROR_COUNTER = METRICS.counter(
            MetricRegistry.name(DefaultRpcBusClient.class, "error")
    );

    @Override
    public void updateAck(String name, long millis) {
        Histogram requestsAckHistogramLocal = METRICS.histogram(
                MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", name)
        );
        requestsAckHistogramLocal.update(millis);
        REQUESTS_ACK_HISTOGRAM.update(millis);
    }

    @Override
    public void updateResponse(String name, long millis) {
        Histogram requestsResponseHistogramLocal = METRICS.histogram(
                MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", name)
        );
        requestsResponseHistogramLocal.update(millis);
        REQUESTS_RESPONSE_HISTOGRAM.update(millis);
    }

    @Override
    public void incError() {
        ERROR_COUNTER.inc();
    }
}
