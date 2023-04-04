package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAbortOperation;

/**
 * Immutable abort operation request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#abortOperation(AbortOperation)
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_operation">
 * abort_op documentation
 * </a>
 */
public class AbortOperation extends OperationReq<AbortOperation.Builder, AbortOperation>
        implements HighLevelRequest<TReqAbortOperation.Builder> {
    @Nullable
    private final String message;

    /**
     * Construct abort job request from operation id with other options set to default.
     */
    public AbortOperation(GUID id) {
        this(builder().setOperationId(id));
    }

    /**
     * Construct abort job request from operation alias with other options set to default.
     */
    public AbortOperation(String alias) {
        this(builder().setOperationAlias(alias));
    }

    AbortOperation(Builder builder) {
        super(builder);
        this.message = builder.message;
    }

    /**
     * Construct empty builder for abort operation request.
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
        if (message != null) {
            builder.setMessage(message);
        }
        return builder;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortOperation.Builder, ?> builder) {
        TReqAbortOperation.Builder messageBuilder = builder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        if (message != null) {
            messageBuilder.setAbortMessage(message);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (message != null) {
            sb.append("Message: ").append(message).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    /**
     * Builder for {@link AbortOperation}
     */
    public static class Builder extends OperationReq.Builder<Builder, AbortOperation> {
        @Nullable
        private String message;

        Builder() {
        }

        /**
         * Set message to be shown in operation aborted error (show in Web UI, logs etc)
         */
        public Builder setMessage(String message) {
            this.message = message;
            return self();
        }

        /**
         * Construct {@link AbortOperation} instance.
         */
        public AbortOperation build() {
            return new AbortOperation(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
