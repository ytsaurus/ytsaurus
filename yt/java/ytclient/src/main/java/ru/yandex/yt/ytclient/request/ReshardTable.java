package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class ReshardTable
        extends TableReq<ReshardTable.Builder, ReshardTable>
        implements HighLevelRequest<TReqReshardTable.Builder> {
    @Nullable
    private final Integer tabletCount;
    @Nullable
    private final TableSchema schema;
    private final List<UnversionedRow> pivotKeys;

    public ReshardTable(BuilderBase<?, ?> builder) {
        super(builder);
        this.tabletCount = builder.tabletCount;
        this.schema = builder.schema;
        this.pivotKeys = new ArrayList<>(builder.pivotKeys);
    }

    public ReshardTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    /**
     * @deprecated Use {@link #ReshardTable(YPath path)} instead.
     */
    @Deprecated
    public ReshardTable(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
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
            sb.append("Schema: ").append(schema).append("; ");
        }
        if (tabletCount != null) {
            sb.append("TabletCount: ").append(tabletCount).append("; ");
        }
    }

    private void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(pivotKeys, new UnversionedRowSerializer(schema));
        writer.finish();
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTabletCount(tabletCount)
                .setSchema(schema)
                .setPivotKeys(pivotKeys)
                .setMutatingOptions(mutatingOptions)
                .setPath(path)
                .setTabletRangeOptions(tabletRangeOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder, ReshardTable> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public ReshardTable build() {
            return new ReshardTable(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends TableReq<?, TRequest>>
            extends TableReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqReshardTable.Builder> {
        @Nullable
        private Integer tabletCount;
        @Nullable
        private TableSchema schema;
        private final List<UnversionedRow> pivotKeys = new ArrayList<>();

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?, ?> builder) {
            super(builder);
            this.tabletCount = builder.tabletCount;
            this.schema = builder.schema;
            this.pivotKeys.clear();
            this.pivotKeys.addAll(builder.pivotKeys);
        }

        public TBuilder setTabletCount(@Nullable Integer tabletCount) {
            this.tabletCount = tabletCount;
            return self();
        }

        public TBuilder setSchema(@Nullable TableSchema schema) {
            this.schema = schema;
            return self();
        }

        public TBuilder addPivotKey(List<?> values) {
            pivotKeys.add(convertValuesToRow(values));
            return self();
        }

        public TBuilder setPivotKeys(List<UnversionedRow> pivotKeys) {
            this.pivotKeys.clear();
            this.pivotKeys.addAll(pivotKeys);
            return self();
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
}
