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
    void onRequestSuccess(String clusterName, Duration time);

    /**
     * Report failed request execution.
     *
     * @param time      that was spent to find failed the whole request
     * @param throwable last exception which led to request failure
     */
    void onRequestFailure(Duration time, Throwable throwable);

    /**
     * Report that specific cluster has been polled.
     *
     * @param clusterName cluster to which request is sent
     */
    void onSubrequestStart(String clusterName);

    /**
     * Report that specific cluster has successfully processed the request.
     *
     * @param clusterName cluster which has responded
     * @param time        time within which subrequest ran
     */
    void onSubrequestSuccess(String clusterName, Duration time);

    /**
     * Report that specific cluster has failed to process the request.
     *
     * @param clusterName cluster which has failed
     * @param time        time within which exactly this subrequest has failed
     * @param throwable   exception which led to request failure
     */
    void onSubrequestFailure(String clusterName, Duration time, Throwable throwable);

    /**
     * Report that inner request has been canceled because another request has completed.
     *
     * @param clusterName cluster request to which has been canceled
     * @param time        time of subrequest within which exactly this subrequest has been canceled
     */
    void onSubrequestCancelation(String clusterName, Duration time);
}

class NoopMultiExecutorMonitoring implements MultiExecutorMonitoring {

    @Override
    public void onRequestSuccess(String clusterName, Duration time) {
    }

    @Override
    public void onRequestFailure(Duration time, Throwable throwable) {
    }

    @Override
    public void onSubrequestStart(String clusterName) {
    }

    @Override
    public void onSubrequestSuccess(String clusterName, Duration time) {
    }

    @Override
    public void onSubrequestFailure(String clusterName, Duration time, Throwable throwable) {
    }

    @Override
    public void onSubrequestCancelation(String clusterName, Duration time) {
    }
}
