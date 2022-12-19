package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqGetTabletInfos;

public class GetTabletInfos
        extends RequestBase<GetTabletInfos.Builder, GetTabletInfos>
        implements HighLevelRequest<TReqGetTabletInfos.Builder> {
    private final String path;
    private final List<Integer> tabletIndexes;

    GetTabletInfos(Builder builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.tabletIndexes = builder.tabletIndexes;
    }

    public GetTabletInfos(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetTabletInfos.Builder, ?> builder) {
        builder.body().setPath(path);
        builder.body().addAllTabletIndexes(tabletIndexes);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; TabletIndexes: ").append(tabletIndexes).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setTabletIndexes(tabletIndexes)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetTabletInfos> {
        @Nullable
        private String path;
        private List<Integer> tabletIndexes = new ArrayList<>();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.path = builder.path;
            this.tabletIndexes = new ArrayList<>(builder.tabletIndexes);
        }

        public Builder setPath(String path) {
            this.path = path;
            return self();
        }

        public Builder addTabletIndex(int idx) {
            tabletIndexes.add(idx);
            return self();
        }

        public Builder setTabletIndexes(List<Integer> tabletIndexes) {
            this.tabletIndexes.clear();
            this.tabletIndexes.addAll(tabletIndexes);
            return self();
        }

        public GetTabletInfos build() {
            return new GetTabletInfos(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
