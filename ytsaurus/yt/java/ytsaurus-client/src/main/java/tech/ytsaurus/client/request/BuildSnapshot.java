package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqBuildSnapshot;

public class BuildSnapshot extends RequestBase<BuildSnapshot.Builder, BuildSnapshot>
        implements HighLevelRequest<TReqBuildSnapshot.Builder> {
    private final GUID cellId;
    private final boolean setReadOnly;

    BuildSnapshot(Builder builder) {
        super(builder);
        this.cellId = Objects.requireNonNull(builder.cellId);
        this.setReadOnly = builder.setReadOnly;
    }

    public BuildSnapshot(GUID cellId) {
        this(builder().setCellId(cellId));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqBuildSnapshot.Builder, ?> builder) {
        builder.body()
                .setCellId(RpcUtil.toProto(cellId))
                .setSetReadOnly(setReadOnly);
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("CellId: ").append(cellId).append("; SetReadOnly: ").append(setReadOnly).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setCellId(cellId)
                .setSetReadOnly(setReadOnly)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, BuildSnapshot> {
        @Nullable
        private GUID cellId;
        private boolean setReadOnly = TReqBuildSnapshot.getDefaultInstance().getSetReadOnly();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.cellId = builder.cellId;
            this.setReadOnly = builder.setReadOnly;
        }

        public Builder setCellId(GUID cellId) {
            this.cellId = cellId;
            return this;
        }

        public Builder setSetReadOnly(boolean setReadOnly) {
            this.setReadOnly = setReadOnly;
            return this;
        }

        public BuildSnapshot build() {
            return new BuildSnapshot(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
