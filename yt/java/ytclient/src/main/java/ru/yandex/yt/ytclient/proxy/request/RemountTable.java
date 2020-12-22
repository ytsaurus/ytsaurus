package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class RemountTable extends TableReq<RemountTable> implements HighLevelRequest<TReqRemountTable.Builder> {
    public RemountTable(String path) {
        super(path);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqRemountTable.Builder, ?> builder) {
        super.writeTo(builder.body());
    }

    @Override
    protected RemountTable self() {
        return this;
    }
}
