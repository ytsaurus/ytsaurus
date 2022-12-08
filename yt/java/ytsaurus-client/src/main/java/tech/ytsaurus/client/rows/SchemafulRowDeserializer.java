package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.Objects;

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
