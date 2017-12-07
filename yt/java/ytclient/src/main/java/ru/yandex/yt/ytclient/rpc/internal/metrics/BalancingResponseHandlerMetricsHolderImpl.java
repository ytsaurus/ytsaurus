package ru.yandex.yt.ytclient.rpc.internal.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;

/**
 * @author dkondra
 */
public class BalancingResponseHandlerMetricsHolderImpl implements BalancingResponseHandlerMetricsHolder {
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Counter inflight = metrics.counter(MetricRegistry.name(BalancingRpcClient.class, "requests", "inflight"));
    private static final Counter failover = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "failover"));
    private static final Counter total = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "total"));

    @Override
    public void inflightInc() {
        inflight.inc();
    }

    @Override
    public void inflightDec() {
        inflight.dec();
    }

    @Override
    public void failoverInc() {
        failover.inc();
    }

    @Override
    public void totalInc() {
        total.inc();
    }
}
