package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGetTablePivotKeys;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class GetTablePivotKeys
        extends RequestBase<GetTablePivotKeys>
        implements HighLevelRequest<TReqGetTablePivotKeys.Builder> {
    private final String path;

    public GetTablePivotKeys(String path) {
        this.path = path;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetTablePivotKeys.Builder, ?> builder) {
        builder.body().setPath(path);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Path: ").append(path).append("; ");
    }

    @Nonnull
    @Override
    protected GetTablePivotKeys self() {
        return this;
    }
}
