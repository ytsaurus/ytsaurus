package ru.yandex.yt.ytclient.bus.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.bus.DefaultBusChannel;

/**
 * @author dkondra
 */
public class DefaultBusChannelMetricsHolderImpl implements DefaultBusChannelMetricsHolder {
    public static final DefaultBusChannelMetricsHolderImpl INSTANCE = new DefaultBusChannelMetricsHolderImpl();

    private static final MetricRegistry METRICS = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram PACKETS_HISTOGRAM = METRICS.histogram(
            MetricRegistry.name(DefaultBusChannel.class, "packets", "histogram")
    );

    @Override
    public void updatePacketsHistogram(long elapsed) {
        PACKETS_HISTOGRAM.update(elapsed);
    }
}
