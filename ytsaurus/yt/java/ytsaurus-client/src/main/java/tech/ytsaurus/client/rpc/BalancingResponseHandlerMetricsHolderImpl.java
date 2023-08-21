package tech.ytsaurus.client.rpc;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import tech.ytsaurus.client.YTsaurusClient;

public class BalancingResponseHandlerMetricsHolderImpl implements BalancingResponseHandlerMetricsHolder {
    public static final BalancingResponseHandlerMetricsHolderImpl INSTANCE =
            new BalancingResponseHandlerMetricsHolderImpl();

    private static final MetricRegistry METRICS = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Counter INFLIGHT = METRICS.counter(
            MetricRegistry.name(YTsaurusClient.class, "requests", "inflight")
    );
    private static final Counter FAILOVER = METRICS.counter(
            MetricRegistry.name(YTsaurusClient.class, "requests", "failover")
    );
    private static final Counter TOTAL = METRICS.counter(
            MetricRegistry.name(YTsaurusClient.class, "requests", "total")
    );

    @Override
    public void inflightInc() {
        INFLIGHT.inc();
    }

    @Override
    public void inflightDec() {
        INFLIGHT.dec();
    }

    @Override
    public void failoverInc() {
        FAILOVER.inc();
    }

    @Override
    public void totalInc() {
        TOTAL.inc();
    }
}
