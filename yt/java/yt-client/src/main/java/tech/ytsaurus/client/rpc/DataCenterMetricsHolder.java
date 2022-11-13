package tech.ytsaurus.client.rpc;

/**
 * @author dkondra
 */
public interface DataCenterMetricsHolder {
    double getDc99thPercentile(String dc);
}
