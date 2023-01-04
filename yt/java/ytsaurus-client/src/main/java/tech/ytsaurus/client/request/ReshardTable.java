package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TReqReshardTable;

public class ReshardTable
        extends TableReq<ReshardTable.Builder, ReshardTable>
        implements HighLevelRequest<TReqReshardTable.Builder> {
    @Nullable
    private final Integer tabletCount;
    @Nullable
    private final TableSchema schema;
    private final List<UnversionedRow> pivotKeys;
    private final List<List<?>> unconvertedPivotKeys;

    public ReshardTable(BuilderBase<?> builder) {
        super(builder);
        this.tabletCount = builder.tabletCount;
        this.schema = builder.schema;
        this.pivotKeys = new ArrayList<>(builder.pivotKeys);
        this.unconvertedPivotKeys = new ArrayList<>(builder.unconvertedPivotKeys);
    }

    public ReshardTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public void convertValues(SerializationResolver serializationResolver) {
        this.pivotKeys.addAll(this.unconvertedPivotKeys.stream().map(
                values -> convertValuesToRow(values, serializationResolver)).collect(Collectors.toList()));
        this.unconvertedPivotKeys.clear();
    }

    /**
     * Internal method: prepare request to send over network.
     */
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

    private UnversionedRow convertValuesToRow(List<?> values, SerializationResolver serializationResolver) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema for pivot keys must be set");
        }
        if (values.size() > schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException("Pivot keys must contain only key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(values.size());
        ApiServiceUtil.convertKeyColumns(row, schema, values, true, serializationResolver);
        return new UnversionedRow(row);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTabletCount(tabletCount)
                .setSchema(schema)
                .setPivotKeys(pivotKeys)
                .setUnconvertedPivotKeys(unconvertedPivotKeys)
                .setMutatingOptions(mutatingOptions)
                .setPath(path)
                .setTabletRangeOptions(tabletRangeOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends TableReq.Builder<TBuilder, ReshardTable> {
        @Nullable
        private Integer tabletCount;
        @Nullable
        private TableSchema schema;
        private final List<UnversionedRow> pivotKeys = new ArrayList<>();
        private final List<List<?>> unconvertedPivotKeys = new ArrayList<>();

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
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
            unconvertedPivotKeys.add(values);
            return self();
        }

        public TBuilder setUnconvertedPivotKeys(List<List<?>> pivotKeys) {
            this.unconvertedPivotKeys.clear();
            this.unconvertedPivotKeys.addAll(pivotKeys);
            return self();
        }

        public TBuilder setPivotKeys(List<UnversionedRow> pivotKeys) {
            this.pivotKeys.clear();
            this.pivotKeys.addAll(pivotKeys);
            return self();
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

        @Override
        public ReshardTable build() {
            return new ReshardTable(this);
        }
    }
}
