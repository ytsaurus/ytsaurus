package ru.yandex.yt.ytclient.bus.metrics;

/**
 * @author dkondra
 */
public interface DefaultBusChannelMetricsHolder {
    void updatePacketsHistogram(long elapsed);
}
