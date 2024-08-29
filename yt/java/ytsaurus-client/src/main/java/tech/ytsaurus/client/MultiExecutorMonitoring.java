package tech.ytsaurus.client;

import java.time.Duration;

public interface MultiExecutorMonitoring {
    /**
     * Report successful request execution.
     *
     * @param clusterName cluster which was first to respond successfully
     * @param time        that was spent to execute request
     */
    void reportRequestSuccess(String clusterName, Duration time);

    /**
     * Report failed request execution.
     *
     * @param time spent to poll all clusters and fail
     */
    void reportRequestFailure(Duration time);

    /**
     * Report that specific cluster has failed to process the request.
     *
     * @param clusterName cluster which has failed
     * @param time        time within which exactly this one hedging request has failed
     */
    void reportRequestHedgingFailure(String clusterName, Duration time);
}

class NoopMultiExecutorMonitoring implements MultiExecutorMonitoring {

    @Override
    public void reportRequestSuccess(String clusterName, Duration time) {
    }

    @Override
    public void reportRequestFailure(Duration time) {
    }

    @Override
    public void reportRequestHedgingFailure(String clusterName, Duration time) {
    }

}