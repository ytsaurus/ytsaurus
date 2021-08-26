package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class ReshardTable
        extends TableReq<ReshardTable>
        implements HighLevelRequest<TReqReshardTable.Builder> {
    private @Nullable Integer tabletCount;
    private @Nullable TableSchema schema;

    public ReshardTable(YPath path) {
        super(path.justPath());
    }

    /**
     * @deprecated Use {@link #ReshardTable(YPath path)} instead.
     */
    @Deprecated
    public ReshardTable(String path) {
        super(path);
    }

    public ReshardTable setSchema(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    public ReshardTable setTabletCount(int tabletCount) {
        this.tabletCount = tabletCount;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqReshardTable.Builder, ?> requestBuilder) {
        TReqReshardTable.Builder builder = requestBuilder.body();
        super.writeTo(builder);
        if (schema != null) {
            builder.setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(schema));
        }
        if (tabletCount != null) {
            builder.setTabletCount(tabletCount);
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        if (schema != null) {
            sb.append("Schema: ").append(schema.toString()).append("; ");
        }
        if (tabletCount != null) {
            sb.append("TabletCount: ").append(tabletCount).append("; ");
        }
    }

    @Nonnull
    @Override
    protected ReshardTable self() {
        return this;
    }
}
