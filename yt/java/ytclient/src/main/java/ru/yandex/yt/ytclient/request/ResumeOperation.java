package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqResumeOperation;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

/**
 * Request for resuming suspended operation
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#suspend_operation">
 *     resume_operation documentation
 *     </a>
 * @see SuspendOperation
 */
@NonNullApi
@NonNullFields
public class ResumeOperation extends OperationReq<ResumeOperation.Builder, ResumeOperation>
        implements HighLevelRequest<TReqResumeOperation.Builder> {
    public ResumeOperation(BuilderBase<?, ?> builder) {
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

    public static class Builder extends BuilderBase<Builder, ResumeOperation> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public ResumeOperation build() {
            return new ResumeOperation(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends OperationReq<?, TRequest>>
            extends OperationReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqResumeOperation.Builder> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?, ?> builder) {
            super(builder);
        }

        @Override
        public void writeTo(RpcClientRequestBuilder<TReqResumeOperation.Builder, ?> builder) {
            TReqResumeOperation.Builder messageBuilder = builder.body();
            writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            return super.toTree(builder);
        }
    }
}
