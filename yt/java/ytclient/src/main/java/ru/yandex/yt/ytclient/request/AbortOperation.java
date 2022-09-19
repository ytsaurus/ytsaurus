package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAbortOperation;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public class AbortOperation extends RequestBase implements HighLevelRequest<TReqAbortOperation.Builder> {
    private final GUID id;
    @Nullable
    private final String message;

    AbortOperation(Builder builder) {
        super(builder);
        this.id = Objects.requireNonNull(builder.id);
        this.message = builder.message;
    }

    public AbortOperation(GUID id) {
        this(builder().setId(id));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortOperation.Builder, ?> requestBuilder) {
        requestBuilder.body().setOperationId(RpcUtil.toProto(id));
        if (message != null) {
            requestBuilder.body().setAbortMessage(message);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Id: ").append(id).append("; ");
        if (message != null) {
            sb.append("Message: ").append(message).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    public Builder toBuilder() {
        Builder builder = builder().setId(id)
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
    public static class Builder extends RequestBase.Builder<Builder> {
        @Nullable
        private GUID id;
        @Nullable
        private String message;

        public Builder() {
        }

        public Builder(Builder builder) {
            super(builder);
            id = builder.id;
            message = builder.message;
        }

        public Builder setId(GUID id) {
            this.id = id;
            return self();
        }

        public Builder setMessage(String message) {
            this.message = message;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
