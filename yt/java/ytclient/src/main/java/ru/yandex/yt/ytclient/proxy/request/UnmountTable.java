package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class UnmountTable extends TableReq<UnmountTable> implements HighLevelRequest<TReqUnmountTable.Builder> {
    boolean force = false;

    public UnmountTable(String path) {
        super(path);
    }

    public UnmountTable setForce(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnmountTable.Builder, ?> builder) {
        super.writeTo(builder.body());
        builder.body().setForce(force);
    }

    @Override
    protected UnmountTable self() {
        return this;
    }

}
