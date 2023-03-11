package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqSuspendOperation;
import tech.ytsaurus.ysontree.YTreeBuilder;

/**
 * Request for suspending operation
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#suspend_operation">
 * suspend_operation documentation
 * </a>
 * @see ResumeOperation
 */
public class SuspendOperation extends OperationReq<SuspendOperation.Builder, SuspendOperation>
        implements HighLevelRequest<TReqSuspendOperation.Builder> {
    private final boolean abortRunningJobs;

    public SuspendOperation(BuilderBase<?> builder) {
        super(builder);
        this.abortRunningJobs = builder.abortRunningJobs;
    }

    /**
     * Construct request from operation id.
     */
    public SuspendOperation(GUID operationId) {
        this(builder().setOperationId(operationId));
    }

    SuspendOperation(String operationAlias) {
        this(builder().setOperationAlias(operationAlias));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct request from operation alias
     */
    public static SuspendOperation fromAlias(String operationAlias) {
        return new SuspendOperation(operationAlias);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSuspendOperation.Builder, ?> builder) {
        TReqSuspendOperation.Builder messageBuilder = builder.body();
        if (operationId != null) {
            messageBuilder.setOperationId(RpcUtil.toProto(operationId));
        } else {
            assert operationAlias != null;
            messageBuilder.setOperationAlias(operationAlias);
        }
        if (abortRunningJobs) {
            messageBuilder.setAbortRunningJobs(abortRunningJobs);
        }
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        super.toTree(builder);
        if (abortRunningJobs) {
            builder.key("abort_running_jobs").value(abortRunningJobs);
        }
        return builder;
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setAbortRunningJobs(abortRunningJobs)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends OperationReq.Builder<TBuilder, SuspendOperation> {
        private boolean abortRunningJobs;

        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.abortRunningJobs = builder.abortRunningJobs;
        }

        /**
         * Abort running jobs
         */
        public TBuilder setAbortRunningJobs(boolean abortRunningJobs) {
            this.abortRunningJobs = abortRunningJobs;
            return self();
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            super.toTree(builder);
            if (abortRunningJobs) {
                builder.key("abort_running_jobs").value(abortRunningJobs);
            }
            return builder;
        }

        @Override
        public SuspendOperation build() {
            return new SuspendOperation(this);
        }
    }
}
