package tech.ytsaurus.flow.row;

import java.util.ArrayList;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.row.UnversionedValueHelper;

/**
 * Builder for {@link UnversionedRow} with associated {@link TableSchema}.
 */
public class PayloadBuilder {
    private final TableSchema schema;
    private final ArrayList<UnversionedValue> values;

    public PayloadBuilder(TableSchema schema) {
        this.schema = schema;
        this.values = new ArrayList<>(schema.getColumnsCount());
        reset();
    }

    PayloadBuilder(Payload payload) {
        this.schema = payload.getSchema();
        this.values = new ArrayList<>(schema.getColumnsCount());
        values.addAll(payload.getRow().getValues());
    }

    /**
     * Sets the value of the named column in the row being built.
     *
     * <p>The value is converted to the column's declared type. A {@code null} value (or a value
     * that converts to {@code null}) is stored as a {@code NULL}-typed cell rather than as a typed
     * cell with a null payload.
     *
     * @param columnName name of the column from the schema
     * @param value      value to store, may be {@code null}
     * @param <T>        value type
     * @return this builder
     * @throws IllegalArgumentException if the column is not found in the schema
     */
    public <T> PayloadBuilder set(String columnName, @Nullable T value) {
        int columnIdx = schema.findColumn(columnName);
        if (columnIdx == -1) {
            throw new IllegalArgumentException("Column %s not found in schema".formatted(columnName));
        }
        ColumnSchema columnSchema = schema.getColumnSchema(columnIdx);
        // Convert value to acceptable by UnversionedValue.
        Object convertedValue = UnversionedValueHelper.convertValueTo(value, columnSchema.getType());
        // A null value is stored as a NULL-typed value, since UnversionedValue forbids a null
        // payload for any other column type.
        UnversionedValue unversionedValue = convertedValue == null
                ? new UnversionedValue(columnIdx, ColumnValueType.NULL, false, null)
                : new UnversionedValue(columnIdx, columnSchema.getType(), false, convertedValue);
        values.set(columnIdx, unversionedValue);
        return this;
    }

    /**
     * Build {@link UnversionedRow} and clear all underling buffers.
     *
     * @return UnversionedRow.
     */
    public Payload finish() {
        var row = new UnversionedRow(new ArrayList<>(values));
        reset();
        return new Payload(row, schema);
    }

    /**
     * Reset all underling values to Null UnversionedValue.
     */
    public void reset() {
        values.clear();
        values.ensureCapacity(schema.getColumnsCount());
        for (int i = 0; i < schema.getColumnsCount(); i++) {
            values.add(new UnversionedValue(i, ColumnValueType.NULL, false, null));
        }
    }

}
