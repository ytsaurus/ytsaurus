package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqCompleteOperation;

/**
 * Immutable complete operation request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#completeOperation(CompleteOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#complete_operation">
 * complete_op documentation
 * </a>
 */
public class CompleteOperation extends OperationReq<CompleteOperation.Builder, CompleteOperation>
        implements HighLevelRequest<TReqCompleteOperation.Builder> {
    /**
     * Construct complete operation request from operation id with other options set to default.
     */
    public CompleteOperation(GUID id) {
        this(builder().setOperationId(id));
    }

    /**
     * Construct complete operation request from operation alias with other options set to default.
     */
    public CompleteOperation(String alias) {
        this(builder().setOperationAlias(alias));
    }

    CompleteOperation(Builder builder) {
        super(builder);
    }

    /**
     * Construct empty builder for complete operation request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        return builder;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCompleteOperation.Builder, ?> builder) {
        TReqCompleteOperation.Builder messageBuilder = builder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Builder for {@link CompleteOperation}
     */
    public static class Builder extends OperationReq.Builder<Builder, CompleteOperation> {
        Builder() {
        }

        /**
         * Construct {@link CompleteOperation} instance.
         */
        public CompleteOperation build() {
            return new CompleteOperation(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
