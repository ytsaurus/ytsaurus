package ru.yandex.yt.ytclient.rpc.metrics;

/**
 * @author dkondra
 */
public interface DefaultRpcBusClientMetricsHolder {
    void updateAck(String name, long millis);

    void updateResponse(String name, long millis);

    void incError();
}
