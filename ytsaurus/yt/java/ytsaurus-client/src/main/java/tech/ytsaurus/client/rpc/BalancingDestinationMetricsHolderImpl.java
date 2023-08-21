package tech.ytsaurus.client.rpc;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

/**
 * @author dkondra
 */
public class BalancingDestinationMetricsHolderImpl implements BalancingDestinationMetricsHolder {
    public static final BalancingDestinationMetricsHolderImpl INSTANCE = new BalancingDestinationMetricsHolderImpl();
    private static final MetricRegistry METRICS = SharedMetricRegistries.getOrCreate("ytclient");

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
        return METRICS.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "ping", destinationName));
    }

    private static Histogram getForDc(String dc) {
        return METRICS.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "ping", dc));
    }
}
