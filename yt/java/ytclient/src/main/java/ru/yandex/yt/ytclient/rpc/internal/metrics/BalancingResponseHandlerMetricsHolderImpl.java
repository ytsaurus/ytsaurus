package ru.yandex.yt.ytclient.rpc.internal.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.proxy.YtClient;

/**
 * @author dkondra
 */
public class BalancingResponseHandlerMetricsHolderImpl implements BalancingResponseHandlerMetricsHolder {
    public static final BalancingResponseHandlerMetricsHolderImpl instance = new BalancingResponseHandlerMetricsHolderImpl();

    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Counter inflight = metrics.counter(MetricRegistry.name(YtClient.class, "requests", "inflight"));
    private static final Counter failover = metrics.counter(MetricRegistry.name(YtClient.class,"requests", "failover"));
    private static final Counter total = metrics.counter(MetricRegistry.name(YtClient.class,"requests", "total"));

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
