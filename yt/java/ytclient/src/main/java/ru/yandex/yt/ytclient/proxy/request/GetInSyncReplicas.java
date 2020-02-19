package ru.yandex.yt.ytclient.proxy.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class GetInSyncReplicas extends RequestBase<GetInSyncReplicas> {
    private final String path;
    private final TableSchema schema;

    private final List<UnversionedRow> rows = new ArrayList<>();

    public GetInSyncReplicas(String path, TableSchema schema) {
        this(path, schema, null);
    }

    public GetInSyncReplicas(String path, TableSchema schema, Iterable<? extends List<?>> keys) {
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
        if (keys != null) {
            addKeys(keys);
        }
    }

    public String getPath() {
        return path;
    }

    public TableSchema getSchema() {
        return schema;
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

    public GetInSyncReplicas addKey(List<?> values) {
        if (values.size() != schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException("Number of delete columns must match number of key columns");
        }
        rows.add(convertValuesToRow(values, false, false));
        return this;
    }

    public GetInSyncReplicas addKeys(Iterable<? extends List<?>> keys) {
        for (List<?> key : keys) {
            addKey(key);
        }
        return this;
    }

    public void serializeRowsetTo(List<byte[]> attachments) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Keys must be set");
        }
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows, new UnversionedRowSerializer(schema));
        writer.finish();
    }
}
