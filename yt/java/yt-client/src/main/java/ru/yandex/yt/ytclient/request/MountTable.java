package ru.yandex.yt.ytclient.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
public class MountTable
        extends TableReq<MountTable.Builder, MountTable>
        implements HighLevelRequest<TReqMountTable.Builder> {
    @Nullable
    private final GUID cellId;
    private final boolean freeze;

    public MountTable(BuilderBase<?> builder) {
        super(builder);
        this.cellId = builder.cellId;
        this.freeze = builder.freeze;
    }

    public MountTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqMountTable.Builder, ?> builder) {
        super.writeTo(builder.body());
        builder.body().setFreeze(freeze);
        if (cellId != null) {
            builder.body().setCellId(RpcUtil.toProto(cellId));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Freeze: ").append(freeze).append("; ");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setCellId(cellId)
                .setFreeze(freeze)
                .setMutatingOptions(mutatingOptions)
                .setPath(path)
                .setTabletRangeOptions(tabletRangeOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends TableReq.Builder<TBuilder, MountTable> {
        @Nullable
        private GUID cellId;
        private boolean freeze = false;

        protected BuilderBase() {
        }

        BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.cellId = builder.cellId;
            this.freeze = builder.freeze;
        }

        public TBuilder setCellId(@Nullable GUID cellId) {
            this.cellId = cellId;
            return self();
        }

        public TBuilder setFreeze(boolean freeze) {
            this.freeze = freeze;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("Freeze: ").append(freeze).append("; ");
        }

        @Override
        public MountTable build() {
            return new MountTable(this);
        }
    }
}
