package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import javax.annotation.Nonnull;

import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class ReshardTable extends TableReq<ReshardTable> {
    private Optional<Integer> tabletCount = Optional.empty();
    private TableSchema schema;

    public ReshardTable(String path) {
        super(path);
    }

    public ReshardTable setSchema(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    public ReshardTable setTabletCount(int tabletCount) {
        this.tabletCount = Optional.of(tabletCount);
        return this;
    }

    public TReqReshardTable.Builder writeTo(TReqReshardTable.Builder builder) {
        super.writeTo(builder);
        if (schema != null) {
            builder.setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(schema));
        }
        tabletCount.ifPresent(x -> builder.setTabletCount(x));
        return builder;
    }

    @Nonnull
    @Override
    protected ReshardTable self() {
        return this;
    }
}
