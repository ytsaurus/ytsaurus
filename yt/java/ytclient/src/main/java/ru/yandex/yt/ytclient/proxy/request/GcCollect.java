package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGCCollect;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullFields
@NonNullApi
public class GcCollect extends RequestBase<GcCollect> implements HighLevelRequest<TReqGCCollect.Builder> {
    private final GUID cellId;

    public GcCollect(GUID cellId) {
        this.cellId = cellId;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGCCollect.Builder, ?> builder) {
        builder.body().setCellId(RpcUtil.toProto(cellId));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("CellId: ").append(cellId).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    protected GcCollect self() {
        return this;
    }
}
