package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.core.tables.TableSchema;

public class GetInSyncReplicas extends RequestBase<GetInSyncReplicas.Builder, GetInSyncReplicas> {
    private final String path;
    private final TableSchema schema;

    private final List<UnversionedRow> rows;
    private final List<List<?>> unconvertedRows;

    GetInSyncReplicas(Builder builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.schema = Objects.requireNonNull(builder.schema);
        this.rows = new ArrayList<>(builder.rows);
        this.unconvertedRows = new ArrayList<>(builder.unconvertedRows);
    }

    public GetInSyncReplicas(String path, TableSchema schema) {
        this(builder()
                .setPath(path)
                .setSchema(schema));
    }

    public GetInSyncReplicas(String path, TableSchema schema, Iterable<? extends List<?>> keys) {
        this(builder()
                .setPath(path)
                .setSchema(schema)
                .addKeys(keys)
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public void convertValues(SerializationResolver serializationResolver) {
        this.rows.addAll(this.unconvertedRows.stream().map(
                values -> convertValuesToRow(values, serializationResolver)).collect(Collectors.toList()));
        this.unconvertedRows.clear();
    }

    public String getPath() {
        return path;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void serializeRowsetTo(List<byte[]> attachments) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Keys must be set");
        }
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows, new UnversionedRowSerializer(schema));
        writer.finish();
    }

    private UnversionedRow convertValuesToRow(
            List<?> values,
            SerializationResolver serializationResolver
    ) {
        if (values.size() < schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException(
                    "Number of values must be more than or equal to the number of key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(values.size());
        ApiServiceUtil.convertKeyColumns(row, schema, values, serializationResolver);
        ApiServiceUtil.convertValueColumns(
                row, schema, values, false, false, serializationResolver);
        return new UnversionedRow(row);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setSchema(schema)
                .setRows(rows)
                .setUnconvertedRows(unconvertedRows)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetInSyncReplicas> {
        @Nullable
        private String path;
        @Nullable
        private TableSchema schema;

        private final List<UnversionedRow> rows = new ArrayList<>();
        private final List<List<?>> unconvertedRows = new ArrayList<>();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
        }

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder addKey(List<?> values) {
            Objects.requireNonNull(schema);
            if (values.size() != schema.getKeyColumnsCount()) {
                throw new IllegalArgumentException("Number of delete columns must match number of key columns");
            }
            unconvertedRows.add(values);
            return this;
        }

        public Builder addKeys(Iterable<? extends List<?>> keys) {
            for (List<?> key : keys) {
                addKey(key);
            }
            return this;
        }

        Builder setUnconvertedRows(List<List<?>> rows) {
            this.unconvertedRows.clear();
            this.unconvertedRows.addAll(rows);
            return this;
        }

        Builder setRows(List<UnversionedRow> rows) {
            this.rows.clear();
            this.rows.addAll(rows);
            return this;
        }

        public GetInSyncReplicas build() {
            return new GetInSyncReplicas(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
