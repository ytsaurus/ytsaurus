package tech.ytsaurus.client.rpc;

/**
 * @author dkondra
 */
public interface BalancingDestinationMetricsHolder {
    double getLocal99thPercentile(String destinationName);

    void updateLocal(String destinationName, long interval);

    void updateDc(String dc, long interval);
}
