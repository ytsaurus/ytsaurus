package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqTrimTable;

public class TrimTable
        extends RequestBase<TrimTable.Builder, TrimTable>
        implements HighLevelRequest<TReqTrimTable.Builder> {
    private final String path;
    private final int tabletIndex;
    private final long trimmedRowCount;

    TrimTable(Builder builder) {
        super(builder);
        path = Objects.requireNonNull(builder.path);
        tabletIndex = Objects.requireNonNull(builder.tabletIndex);
        trimmedRowCount = Objects.requireNonNull(builder.trimmedRowCount);
    }

    public TrimTable(String path, int tabletIndex, long trimmedRowCount) {
        this(builder().setPath(path).setTabletIndex(tabletIndex).setTrimmedRowCount(trimmedRowCount));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqTrimTable.Builder, ?> builder) {
        builder.body().setPath(path);
        builder.body().setTabletIndex(tabletIndex);
        builder.body().setTrimmedRowCount(trimmedRowCount);
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Path: ").append(path)
                .append("; TabletIndex: ").append(tabletIndex)
                .append("; TrimmedRowCount: ").append(trimmedRowCount).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setTabletIndex(tabletIndex)
                .setTrimmedRowCount(trimmedRowCount)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, TrimTable> {
        @Nullable
        private String path;
        @Nullable
        private Integer tabletIndex;
        @Nullable
        private Long trimmedRowCount;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.path = builder.path;
            this.tabletIndex = builder.tabletIndex;
            this.trimmedRowCount = builder.trimmedRowCount;
        }

        public Builder setPath(String path) {
            this.path = path;
            return self();
        }

        public Builder setTabletIndex(int tabletIndex) {
            this.tabletIndex = tabletIndex;
            return self();
        }

        public Builder setTrimmedRowCount(long trimmedRowCount) {
            this.trimmedRowCount = trimmedRowCount;
            return self();
        }

        public TrimTable build() {
            return new TrimTable(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
