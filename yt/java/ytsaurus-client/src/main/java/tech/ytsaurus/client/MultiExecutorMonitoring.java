package tech.ytsaurus.client;

import java.time.Duration;

/**
 * MultiExecutorMonitoring is executor callback to monitor hedging request execution.
 * Please, note that callbacks must be non-blocking.
 */
public interface MultiExecutorMonitoring {
    /**
     * Report successful request execution.
     *
     * @param clusterName cluster which was first to respond successfully
     * @param time        that was spent to execute the whole request
     */
    void reportRequestSuccess(String clusterName, Duration time);

    /**
     * Report failed request execution.
     *
     * @param time        that was spent to find failed the whole request
     * @param throwable   last exception which led to request failure
     */
    void reportRequestFailure(Duration time, Throwable throwable);

    /**
     * Report that specific cluster has been polled.
     *
     * @param clusterName cluster to which request is sent
     */
    void reportSubrequestStart(String clusterName);

    /**
     * Report that specific cluster has successfully processed the request.
     *
     * @param clusterName cluster which has responded
     * @param time        time within which subrequest ran
     */
    void reportSubrequestSuccess(String clusterName, Duration time);

    /**
     * Report that specific cluster has failed to process the request.
     *
     * @param clusterName cluster which has failed
     * @param time        time within which exactly this subrequest has failed
     * @param throwable   exception which led to request failure
     */
    void reportSubrequestFailure(String clusterName, Duration time, Throwable throwable);

    /**
     * Report that inner request has been cancelled because another request has completed.
     *
     * @param clusterName cluster request to which has been cancelled
     * @param time        time of subrequest within which exactly this subrequest has been cancelled
     */
    void reportSubrequestCancelled(String clusterName, Duration time);
}

class NoopMultiExecutorMonitoring implements MultiExecutorMonitoring {

    @Override
    public void reportRequestSuccess(String clusterName, Duration time) {
    }

    @Override
    public void reportRequestFailure(Duration time, Throwable throwable) {
    }

    @Override
    public void reportSubrequestStart(String clusterName) {
    }

    @Override
    public void reportSubrequestSuccess(String clusterName, Duration time) {
    }

    @Override
    public void reportSubrequestFailure(String clusterName, Duration time, Throwable throwable) {
    }

    @Override
    public void reportSubrequestCancelled(String clusterName, Duration time) {
    }
}
