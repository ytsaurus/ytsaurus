package ru.yandex.yt.ytclient.object;

import java.util.List;
import java.util.Objects;

import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.WireColumnSchema;
import ru.yandex.yt.ytclient.wire.WireProtocolReader;

public class SchemafulRowsetDeserializer
        extends UnversionedRowsetDeserializer
        implements WireSchemafulRowsetDeserializer<UnversionedRow> {
    private final List<WireColumnSchema> columnSchema;

    public SchemafulRowsetDeserializer(TableSchema tableSchema) {
        super(tableSchema);
        this.columnSchema = Objects.requireNonNull(WireProtocolReader.makeSchemaData(tableSchema));
    }

    @Override
    public List<WireColumnSchema> getColumnSchema() {
        return columnSchema;
    }

}
