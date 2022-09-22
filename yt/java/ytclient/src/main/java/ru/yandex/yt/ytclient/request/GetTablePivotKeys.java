package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class GetTablePivotKeys extends RequestBase<GetTablePivotKeys.Builder>
        implements HighLevelRequest<TReqGetTablePivotKeys.Builder> {
    private final String path;

    GetTablePivotKeys(Builder builder) {
        super(builder);
        path = Objects.requireNonNull(builder.path);
    }

    public GetTablePivotKeys(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetTablePivotKeys.Builder, ?> builder) {
        builder.body().setPath(path);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Path: ").append(path).append("; ");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder> {
        @Nullable
        private String path;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            path = builder.path;
        }

        public Builder setPath(String path) {
            this.path = path;
            return self();
        }

        public GetTablePivotKeys build() {
            return new GetTablePivotKeys(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
