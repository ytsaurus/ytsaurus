package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqGCCollect;

public class GcCollect
        extends RequestBase<GcCollect.Builder, GcCollect>
        implements HighLevelRequest<TReqGCCollect.Builder> {
    private final GUID cellId;

    GcCollect(Builder builder) {
        super(builder);
        cellId = Objects.requireNonNull(builder.cellId);
    }

    public GcCollect(GUID cellId) {
        this(builder().setCellId(cellId));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGCCollect.Builder, ?> builder) {
        builder.body().setCellId(RpcUtil.toProto(cellId));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("CellId: ").append(cellId).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public Builder toBuilder() {
        return builder()
                .setCellId(cellId)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GcCollect> {
        @Nullable
        private GUID cellId;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            cellId = builder.cellId;
        }

        public Builder setCellId(GUID cellId) {
            this.cellId = cellId;
            return self();
        }

        public GcCollect build() {
            return new GcCollect(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
