package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

@NonNullApi
@NonNullFields
public class GetInSyncReplicas extends RequestBase<GetInSyncReplicas.Builder> {
    private final String path;
    private final TableSchema schema;

    private final List<UnversionedRow> rows;

    GetInSyncReplicas(Builder builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.schema = Objects.requireNonNull(builder.schema);
        this.rows = builder.rows;
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

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setSchema(schema)
                .setRows(rows);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder> {
        @Nullable
        private String path;
        @Nullable
        private TableSchema schema;

        private final List<UnversionedRow> rows = new ArrayList<>();

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
            rows.add(convertValuesToRow(values, false, false));
            return this;
        }

        public Builder addKeys(Iterable<? extends List<?>> keys) {
            for (List<?> key : keys) {
                addKey(key);
            }
            return this;
        }

        Builder setRows(List<UnversionedRow> rows) {
            this.rows.clear();
            this.rows.addAll(rows);
            return this;
        }

        private UnversionedRow convertValuesToRow(List<?> values, boolean skipMissingValues, boolean aggregate) {
            if (values.size() < schema.getKeyColumnsCount()) {
                throw new IllegalArgumentException(
                        "Number of values must be more than or equal to the number of key columns");
            }
            List<UnversionedValue> row = new ArrayList<>(values.size());
            ApiServiceUtil.convertKeyColumns(row, schema, values);
            ApiServiceUtil.convertValueColumns(row, schema, values, skipMissingValues, aggregate);
            return new UnversionedRow(row);
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
