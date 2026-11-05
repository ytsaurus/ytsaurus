package tech.ytsaurus.flow.internal.row;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.tables.ColumnValueType;

/**
 * Immutable typed view of a row containing every schema column in order.
 * Each cell contains a column id, a type and a value. Column ids equal their zero-based positions.
 *
 * <p>Accessors throw {@link IllegalArgumentException} for a column id outside the schema or a
 * value of another type. A schema-unbound {@link ProtoWireRow} (see
 * {@link ProtoWireRow#isSchemaBound()}) has no schema: every accessor except
 * {@link #cellCount()} throws {@link UnsupportedOperationException}.
 */
public interface RowView {
    int cellCount();

    ColumnValueType type(int columnId);

    long getInt64(int columnId);

    /**
     * Returns the raw two's-complement bits of a UINT64 value.
     */
    long getUint64Bits(int columnId);

    double getDouble(int columnId);

    boolean getBoolean(int columnId);

    /**
     * Returns the byte length of a STRING, ANY or COMPOSITE value.
     */
    int valueLength(int columnId);

    /**
     * Returns an immutable slice of a STRING, ANY or COMPOSITE value without copying it.
     * The slice is owned by the row. Callers that need mutable storage copy it, for example
     * with {@code bytes(columnId).toByteArray()}.
     */
    ByteString bytes(int columnId);
}
