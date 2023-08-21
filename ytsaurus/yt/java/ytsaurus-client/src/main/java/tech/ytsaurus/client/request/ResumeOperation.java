package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqResumeOperation;
import tech.ytsaurus.ysontree.YTreeBuilder;

/**
 * Request for resuming suspended operation
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#suspend_operation">
 * resume_operation documentation
 * </a>
 * @see SuspendOperation
 */
public class ResumeOperation extends OperationReq<ResumeOperation.Builder, ResumeOperation>
        implements HighLevelRequest<TReqResumeOperation.Builder> {
    public ResumeOperation(BuilderBase<?> builder) {
        super(builder);
    }

    public ResumeOperation(GUID operationId) {
        this(builder().setOperationId(operationId));
    }

    ResumeOperation(String operationAlias) {
        this(builder().setOperationAlias(operationAlias));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static ResumeOperation fromAlias(String alias) {
        return new ResumeOperation(alias);
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return super.toTree(builder);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqResumeOperation.Builder, ?> builder) {
        TReqResumeOperation.Builder messageBuilder = builder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
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
            extends OperationReq.Builder<TBuilder, ResumeOperation> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            return super.toTree(builder);
        }

        @Override
        public ResumeOperation build() {
            return new ResumeOperation(this);
        }
    }
}
