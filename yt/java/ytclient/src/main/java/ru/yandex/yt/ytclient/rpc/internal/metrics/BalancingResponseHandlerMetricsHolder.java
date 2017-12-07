package ru.yandex.yt.ytclient.rpc.internal.metrics;

/**
 * @author dkondra
 */
public interface BalancingResponseHandlerMetricsHolder {
    void inflightInc();

    void inflightDec();

    void failoverInc();

    void totalInc();
}
