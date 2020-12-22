package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class FreezeTable extends TableReq<FreezeTable> implements HighLevelRequest<TReqFreezeTable.Builder> {
    public FreezeTable(String path) {
        super(path);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqFreezeTable.Builder, ?> builder) {
        super.writeTo(builder.body());
    }

    @Override
    protected FreezeTable self() {
        return this;
    }

}
