package ru.yandex.yt.ytclient.rpc.internal.metrics;

/**
 * @author dkondra
 */
public interface BalancingDestinationMetricsHolder {
    double getLocal99thPercentile(String destinationName);

    void updateLocal(String destinationName, long interval);

    void updateDc(String dc, long interval);
}
