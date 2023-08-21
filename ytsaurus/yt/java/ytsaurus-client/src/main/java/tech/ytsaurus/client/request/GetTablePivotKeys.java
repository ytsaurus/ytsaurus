package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqGetTablePivotKeys;

public class GetTablePivotKeys extends RequestBase<GetTablePivotKeys.Builder, GetTablePivotKeys>
        implements HighLevelRequest<TReqGetTablePivotKeys.Builder> {
    private final String path;
    private final boolean representKeyAsList;

    GetTablePivotKeys(Builder builder) {
        super(builder);
        path = Objects.requireNonNull(builder.path);
        representKeyAsList = builder.representKeyAsList;
    }

    public GetTablePivotKeys(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetTablePivotKeys.Builder, ?> builder) {
        builder.body().setPath(path).setRepresentKeyAsList(representKeyAsList);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Path: ").append(path).append("; RepresentKeyAsList: ").append(representKeyAsList).append("; ");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setRepresentKeyAsList(representKeyAsList)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetTablePivotKeys> {
        @Nullable
        private String path;
        private boolean representKeyAsList = TReqGetTablePivotKeys.getDefaultInstance().getRepresentKeyAsList();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            path = builder.path;
            representKeyAsList = builder.representKeyAsList;
        }

        public Builder setPath(String path) {
            this.path = path;
            return self();
        }

        public Builder setRepresentKeyAsList(boolean representKeyAsList) {
            this.representKeyAsList = representKeyAsList;
            return self();
        }

        @Override
        public GetTablePivotKeys build() {
            return new GetTablePivotKeys(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
