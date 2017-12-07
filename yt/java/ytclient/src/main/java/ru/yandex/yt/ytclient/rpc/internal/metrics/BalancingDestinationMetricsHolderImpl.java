package ru.yandex.yt.ytclient.rpc.internal.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;

/**
 * @author dkondra
 */
public class BalancingDestinationMetricsHolderImpl implements BalancingDestinationMetricsHolder {
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");

    @Override
    public double getLocal99thPercentile(String destinationName) {
        return getForLocal(destinationName).getSnapshot().get99thPercentile();
    }

    @Override
    public void updateLocal(String destinationName, long interval) {
        getForLocal(destinationName).update(interval);
    }

    @Override
    public void updateDc(String dc, long interval) {
        getForDc(dc).update(interval);
    }

    private static Histogram getForLocal(String destinationName) {
        return metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "ping", destinationName));
    }

    private static Histogram getForDc(String dc) {
        return metrics.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "ping", dc));
    }
}
