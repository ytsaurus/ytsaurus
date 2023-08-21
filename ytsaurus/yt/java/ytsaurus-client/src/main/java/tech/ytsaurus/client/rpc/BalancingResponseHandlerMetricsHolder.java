package tech.ytsaurus.client.rpc;

public interface BalancingResponseHandlerMetricsHolder {
    void inflightInc();

    void inflightDec();

    void failoverInc();

    void totalInc();
}
