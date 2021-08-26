package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
public class MountTable extends TableReq<MountTable> implements HighLevelRequest<TReqMountTable.Builder> {
    private @Nullable GUID cellId;
    private boolean freeze = false;

    public MountTable(YPath path) {
        super(path.justPath());
    }

    /**
     * @deprecated Use {@link #MountTable(YPath path)} instead.
     */
    @Deprecated
    public MountTable(String path) {
        super(path);
    }

    public MountTable setCellId(GUID cellId) {
        this.cellId = cellId;
        return this;
    }

    public MountTable setFreeze(boolean freeze) {
        this.freeze = freeze;
        return this;
    }

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
    protected MountTable self() {
        return this;
    }
}
