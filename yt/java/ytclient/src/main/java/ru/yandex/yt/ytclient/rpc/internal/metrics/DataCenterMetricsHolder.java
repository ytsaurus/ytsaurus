package ru.yandex.yt.ytclient.rpc.internal.metrics;

/**
 * @author dkondra
 */
public interface DataCenterMetricsHolder {
    double getDc99thPercentile(String dc);
}
