package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.Objects;


import tech.ytsaurus.core.tables.TableSchema;

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
