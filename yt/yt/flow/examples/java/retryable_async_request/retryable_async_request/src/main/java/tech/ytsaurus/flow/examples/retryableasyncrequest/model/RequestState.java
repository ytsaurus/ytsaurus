package tech.ytsaurus.flow.examples.retryableasyncrequest.model;

import javax.persistence.Column;
import javax.persistence.Entity;

/**
 * Internal state for a single request tracked by the processor.
 * Serialized as YSON for internal state storage.
 */
// [BEGIN request_state]
@Entity
public class RequestState {
    @Column(name = "request_id")
    private long requestId;

    private long key;

    private String request;

    @Column(name = "failed_attempts")
    private int failedAttempts;

    public RequestState() {
    }

    public RequestState(long requestId, long key, String request, int failedAttempts) {
        this.requestId = requestId;
        this.key = key;
        this.request = request;
        this.failedAttempts = failedAttempts;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public int getFailedAttempts() {
        return failedAttempts;
    }

    public void setFailedAttempts(int failedAttempts) {
        this.failedAttempts = failedAttempts;
    }

    @Override
    public String toString() {
        return "RequestState{" +
                "requestId=" + requestId +
                ", key=" + key +
                ", request='" + request + '\'' +
                ", failedAttempts=" + failedAttempts +
                '}';
    }
}
// [END request_state]
