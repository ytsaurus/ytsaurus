package ru.yandex.yt.ytclient.proxy.request;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqGetTabletInfos;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class GetTabletInfos
        extends RequestBase<GetTabletInfos>
        implements HighLevelRequest<TReqGetTabletInfos.Builder> {
    private final String path;
    private final List<Integer> tabletIndexes = new ArrayList<>();

    public GetTabletInfos(String path) {
        this.path = path;
    }

    public GetTabletInfos addTabletIndex(int idx) {
        tabletIndexes.add(idx);
        return this;
    }

    public GetTabletInfos setTabletIndexes(List<Integer> tabletIndexes) {
        this.tabletIndexes.clear();
        this.tabletIndexes.addAll(tabletIndexes);
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetTabletInfos.Builder, ?> builder) {
        builder.body().setPath(path);
        builder.body().addAllTabletIndexes(tabletIndexes);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; TabletIndexes: ").append(tabletIndexes).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Nonnull
    @Override
    protected GetTabletInfos self() {
        return this;
    }
}
