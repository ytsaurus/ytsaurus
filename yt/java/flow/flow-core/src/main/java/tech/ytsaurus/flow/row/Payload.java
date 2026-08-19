package tech.ytsaurus.flow.row;

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.row.UnversionedValueHelper;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Wrapper for {@link tech.ytsaurus.client.rows.UnversionedRow} with associated
 * {@link tech.ytsaurus.core.tables.TableSchema} and access methods.
 */
public class Payload implements YTreeConvertible {
    public static final UnversionedRow EMPTY_ROW = new UnversionedRow(List.of());
    public static final TableSchema EMPTY_SCHEMA = TableSchema.builder().build();
    public static final Payload EMPTY = new Payload(EMPTY_ROW, EMPTY_SCHEMA);

    private final UnversionedRow row;
    private final TableSchema schema;

    public Payload(UnversionedRow row) {
        this.row = row;
        this.schema = EMPTY_SCHEMA;
    }

    public Payload(UnversionedRow row, TableSchema schema) {
        this.row = row;
        this.schema = schema;
    }

    /**
     * Returns the value of the named column converted to {@code fieldClass},
     * or {@code null} if the column holds a YT null value.
     */
    public <T> @Nullable T get(String columnName, Class<T> fieldClass) {
        int columnId = schema.findColumn(columnName);
        if (columnId < 0) {
            throw new IllegalArgumentException("Column not found in schema. (ColumnName: %s, Schema: %s)"
                    .formatted(columnName, schema));
        }
        return get(columnId, fieldClass);
    }

    /**
     * Returns the value of the column with the given id converted to {@code fieldClass},
     * or {@code null} if the column holds a YT null value.
     */
    @SuppressWarnings("unchecked")
    public <T> @Nullable T get(int columnId, Class<T> fieldClass) {
        if (columnId < 0 || columnId >= schema.getColumns().size()) {
            throw new IllegalArgumentException("Column not found in schema. (ColumnId: %s)".formatted(columnId));
        }
        return (T) UnversionedValueHelper.convertValueFrom(
                row.getValues().get(columnId),
                fieldClass
        );
    }

    public UnversionedRow getRow() {
        return row;
    }

    public TableSchema getSchema() {
        return schema;
    }

    /**
     * Convert Payload to PayloadBuilder for further modifications.
     *
     * @return PayloadBuilder.
     */
    public PayloadBuilder toBuilder() {
        return new PayloadBuilder(this);
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Payload payload = (Payload) o;
        return Objects.equals(row, payload.row) && Objects.equals(schema, payload.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, schema);
    }

    @Override
    public YTreeNode toYTree() {
        return row.toYTreeMap(schema);
    }
}
