package ru.yandex.yt.ytclient.proxy.request;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class ReshardTable
        extends TableReq<ReshardTable>
        implements HighLevelRequest<TReqReshardTable.Builder> {
    private @Nullable Integer tabletCount;
    private @Nullable TableSchema schema;
    private final List<UnversionedRow> pivotKeys = new ArrayList<>();

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

    public ReshardTable addPivotKey(List<?> values) {
        pivotKeys.add(convertValuesToRow(values));
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
        if (!pivotKeys.isEmpty()) {
            serializeRowsetTo(requestBuilder.attachments());
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

    private UnversionedRow convertValuesToRow(List<?> values) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema for pivot keys must be set");
        }
        if (values.size() > schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException("Pivot keys must contain only key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(values.size());
        ApiServiceUtil.convertKeyColumns(row, schema, values, true);
        return new UnversionedRow(row);
    }

    private void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(pivotKeys, new UnversionedRowSerializer(schema));
        writer.finish();
    }

}
