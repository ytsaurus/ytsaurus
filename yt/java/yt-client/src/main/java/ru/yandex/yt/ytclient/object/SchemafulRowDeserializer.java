package ru.yandex.yt.ytclient.object;

import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.WireColumnSchema;

public class SchemafulRowDeserializer
        extends UnversionedRowDeserializer
        implements WireSchemafulRowDeserializer<UnversionedRow> {
    private final List<WireColumnSchema> columnSchema;

    public SchemafulRowDeserializer(List<WireColumnSchema> columnSchema) {
        this.columnSchema = Objects.requireNonNull(columnSchema);
    }

    @Override
    public List<WireColumnSchema> getColumnSchema() {
        return columnSchema;
    }
}
