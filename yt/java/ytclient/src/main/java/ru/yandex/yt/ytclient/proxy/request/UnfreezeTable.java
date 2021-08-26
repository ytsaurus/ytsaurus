package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class UnfreezeTable extends TableReq<UnfreezeTable> implements HighLevelRequest<TReqUnfreezeTable.Builder> {
    public UnfreezeTable(YPath path) {
        super(path.justPath());
    }

    /**
     * @deprecated Use {@link #UnfreezeTable(YPath path)} instead.
     */
    @Deprecated
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
