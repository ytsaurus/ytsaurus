package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class UnfreezeTable extends TableReq<UnfreezeTable> implements HighLevelRequest<TReqUnfreezeTable.Builder> {
    public UnfreezeTable(String path) {
        super(path);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnfreezeTable.Builder, ?> builder) {
        super.writeTo(builder.body());
    }

    @Override
    protected UnfreezeTable self() {
        return this;
    }
}
