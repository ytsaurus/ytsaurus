package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class RemountTable extends TableReq<RemountTable> implements HighLevelRequest<TReqRemountTable.Builder> {
    public RemountTable(YPath path) {
        super(path.justPath());
    }

    /**
     * @deprecated Use {@link #RemountTable(YPath path)} instead.
     */
    @Deprecated
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
