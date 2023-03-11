package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAbortJob;

/**
 * Immutable abort job request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#abortJob(AbortJob)
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_job">
 * abort_job documentation
 * </a>
 */
public class AbortJob
        extends RequestBase<AbortJob.Builder, AbortJob>
        implements HighLevelRequest<TReqAbortJob.Builder> {
    private final GUID jobId;
    @Nullable
    private final Duration interruptTimeout;

    /**
     * Construct abort job request from jobId with other options set to defaults.
     */
    public AbortJob(GUID jobId) {
        this(builder().setJobId(jobId));
    }

    AbortJob(Builder builder) {
        super(builder);
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.interruptTimeout = builder.interruptTimeout;
    }

    /**
     * Construct empty builder for abort job request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortJob.Builder, ?> requestBuilder) {
        TReqAbortJob.Builder messageBuilder = requestBuilder.body();
        messageBuilder.setJobId(RpcUtil.toProto(jobId));
        if (interruptTimeout != null) {
            messageBuilder.setInterruptTimeout(interruptTimeout.toNanos() / 1000L);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
        sb.append("interruptTimeout: ").append(interruptTimeout).append(";");
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        Builder builder = builder().setJobId(jobId);
        if (interruptTimeout != null) {
            builder.setInterruptTimeout(interruptTimeout);
        }
        builder.setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        return builder;
    }

    /**
     * Builder for {@link AbortJob}
     */
    public static class Builder extends RequestBase.Builder<Builder, AbortJob> {
        @Nullable
        private GUID jobId;
        @Nullable
        private Duration interruptTimeout;

        Builder() {
        }

        /**
         * Set id of a job to abort.
         */
        public Builder setJobId(GUID jobId) {
            this.jobId = jobId;
            return self();
        }

        /**
         * Set interrupt timeout.
         * <p>
         * Interrupt timeout is a time that is given to the job to gracefully complete execution.
         * If job successfully completes within this timeout YT accepts its result and marks job completed,
         * otherwise job is aborted.
         */
        public Builder setInterruptTimeout(Duration interruptTimeout) {
            this.interruptTimeout = interruptTimeout;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("jobId: ").append(jobId).append(";");
            sb.append("interruptTimeout: ").append(interruptTimeout).append(";");
        }

        /**
         * Construct {@link AbortJob} instance.
         */
        @Override
        public AbortJob build() {
            return new AbortJob(this);
        }

        @Override
        protected Builder self() {
            return this;
        }

    }
}
