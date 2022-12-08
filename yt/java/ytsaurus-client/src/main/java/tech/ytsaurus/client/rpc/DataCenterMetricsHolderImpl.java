package tech.ytsaurus.client.rpc;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

/**
 * @author dkondra
 */
public class DataCenterMetricsHolderImpl implements DataCenterMetricsHolder {
    public static final DataCenterMetricsHolderImpl INSTANCE = new DataCenterMetricsHolderImpl();
    private static final MetricRegistry METRICS = SharedMetricRegistries.getOrCreate("ytclient");

    @Override
    public double getDc99thPercentile(String dc) {
        Histogram pingHistogramDc = METRICS.histogram(MetricRegistry.name(DefaultRpcBusClient.class, "ping", dc));
        return pingHistogramDc.getSnapshot().get99thPercentile();
    }
}
