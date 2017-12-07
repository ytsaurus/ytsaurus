package ru.yandex.yt.ytclient.rpc.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;

/**
 * @author dkondra
 */
public class DefaultRpcBusClientMetricsHolderImpl implements DefaultRpcBusClientMetricsHolder {
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram requestsAckHistogram = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", "total"));
    private static final Histogram requestsResponseHistogram = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", "total"));
    private static final Counter errorCounter = metrics.counter(MetricRegistry.name(DefaultRpcBusClient.class, "error"));

    @Override
    public void updateAck(String name, long millis) {
        Histogram requestsAckHistogramLocal = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "ack", name));
        requestsAckHistogramLocal.update(millis);
        requestsAckHistogram.update(millis);
    }

    @Override
    public void updateResponse(String name, long millis) {
        Histogram requestsResponseHistogramLocal = metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "requests", "response", name));
        requestsResponseHistogramLocal.update(millis);
        requestsResponseHistogram.update(millis);
    }

    @Override
    public void incError() {
        errorCounter.inc();
    }
}
