package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TReqBuildSnapshot;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class BuildSnapshot extends RequestBase<BuildSnapshot> implements HighLevelRequest<TReqBuildSnapshot.Builder> {
    final GUID cellId;
    boolean setReadOnly = TReqBuildSnapshot.getDefaultInstance().getSetReadOnly();

    public BuildSnapshot(GUID cellId) {
        this.cellId = cellId;
    }

    public BuildSnapshot setSetReadOnly(boolean setReadOnly) {
        this.setReadOnly = setReadOnly;
        return this;
    }

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

    @Nonnull
    @Override
    protected BuildSnapshot self() {
        return this;
    }
}
