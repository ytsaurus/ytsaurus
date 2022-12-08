package tech.ytsaurus.client.bus;

/**
 * @author dkondra
 */
public interface DefaultBusChannelMetricsHolder {
    void updatePacketsHistogram(long elapsed);
}
