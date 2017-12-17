package ru.yandex.yt.ytclient.bus.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import ru.yandex.yt.ytclient.bus.DefaultBusChannel;

/**
 * @author dkondra
 */
public class DefaultBusChannelMetricsHolderImpl implements DefaultBusChannelMetricsHolder {
    public static final DefaultBusChannelMetricsHolderImpl instance = new DefaultBusChannelMetricsHolderImpl();

    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Histogram packetsHistogram = metrics.histogram(MetricRegistry.name(DefaultBusChannel.class, "packets", "histogram"));

    @Override
    public void updatePacketsHistogram(long elapsed) {
        packetsHistogram.update(elapsed);
    }
}
