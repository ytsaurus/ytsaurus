package ru.yandex.yt.ytclient.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAbortOperation;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class AbortOperation extends OperationReq<AbortOperation.Builder, AbortOperation>
        implements HighLevelRequest<TReqAbortOperation.Builder> {
    @Nullable
    private final String message;

    AbortOperation(Builder builder) {
        super(builder);
        this.message = builder.message;
    }

    public AbortOperation(GUID id) {
        this(builder().setOperationId(id));
    }

    public AbortOperation(String alias) {
        this(builder().setOperationAlias(alias));
    }

    public static Builder builder() {
        return new Builder();
    }

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

    @NonNullApi
    @NonNullFields
    public static class Builder extends OperationReq.Builder<Builder, AbortOperation> {
        @Nullable
        private String message;

        public Builder() {
        }

        public Builder(Builder builder) {
            super(builder);
            message = builder.message;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return self();
        }

        public AbortOperation build() {
            return new AbortOperation(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
