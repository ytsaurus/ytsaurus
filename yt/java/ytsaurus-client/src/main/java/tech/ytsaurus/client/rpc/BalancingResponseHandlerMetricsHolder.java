package tech.ytsaurus.client.rpc;

/**
 * @author dkondra
 */
public interface BalancingResponseHandlerMetricsHolder {
    void inflightInc();

    void inflightDec();

    void failoverInc();

    void totalInc();
}
