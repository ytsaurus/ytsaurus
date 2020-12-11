package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class TrimTable extends RequestBase<TrimTable> implements HighLevelRequest<TReqTrimTable.Builder> {
    final String path;
    final int tabletIndex;
    final long trimmedRowCount;

    public TrimTable(String path, int tabletIndex, long trimmedRowCount) {
        this.path = path;
        this.tabletIndex = tabletIndex;
        this.trimmedRowCount = trimmedRowCount;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqTrimTable.Builder, ?> builder) {
        builder.body().setPath(path);
        builder.body().setTabletIndex(tabletIndex);
        builder.body().setTrimmedRowCount(trimmedRowCount);
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Path: ").append(path)
                .append("; TabletIndex: ").append(tabletIndex)
                .append("; TrimmedRowCount: ").append(trimmedRowCount).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    protected TrimTable self() {
        return this;
    }
}
