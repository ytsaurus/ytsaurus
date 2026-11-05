package tech.ytsaurus.flow.internal.row;

import java.util.Objects;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.yson.YsonParser;

import static tech.ytsaurus.flow.internal.row.CompactWireFormat.packRange;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.rangeLength;

/**
 * Immutable wire bytes and typed access to a Flow companion row.
 *
 * <p>{@link ProtoWireRowDecoder} validates input and supplies decoded cells. Rows produced by
 * {@link ProtoWireRowWriter#finishTrusted} decode their cells on the first column access.
 * Schema-unbound keys retain their wire order and expose an empty, non-null schema.
 *
 * <p>Input values may exceed the YT output size limit. {@link #validateForOutput} and the
 * encoded-cell accessors enforce that limit when forwarding or copying data. A row with an
 * oversized value can still be read, filtered or rewritten by user code.
 */
public final class ProtoWireRow implements RowView {
    private final ByteString wireBytes;
    private final TableSchema schema;
    private final boolean schemaBound;
    private final int cellCount;
    /**
     * Zero, or the length of the first STRING, ANY or COMPOSITE value above the output limit (see
     * the output policy in the class javadoc). Such a row is valid input, so parsing does not
     * reject it: the scan records the length, and {@link #validateForOutput} uses it to reject
     * the row in constant time when it is emitted. Always 0 for trusted rows because the writer
     * rejects such values.
     */
    private final int oversizedOutputValueLength;
    /**
     * Cells decoded by {@link #fromWire}; null for trusted and schema-unbound rows.
     */
    private final @Nullable DecodedCells eagerCells;
    /**
     * Cells of a trusted row, decoded on its first column access; never set for other rows.
     */
    private volatile @Nullable DecodedCells lazyCells;

    ProtoWireRow(
            ByteString wireBytes,
            TableSchema schema,
            boolean schemaBound,
            int cellCount,
            int oversizedOutputValueLength,
            @Nullable DecodedCells eagerCells
    ) {
        this.wireBytes = wireBytes;
        this.schema = schema;
        this.schemaBound = schemaBound;
        this.cellCount = cellCount;
        this.oversizedOutputValueLength = oversizedOutputValueLength;
        this.eagerCells = eagerCells;
    }

    /**
     * Validates and decodes the cells of an immutable compact row in one pass.
     *
     * @throws MalformedRowException if the bytes cannot be decoded
     * @throws InvalidRowException if the decoded row does not match the format or {@code schema}
     */
    public static ProtoWireRow fromWire(ByteString wireBytes, TableSchema schema) {
        return ProtoWireRowDecoder.decode(wireBytes, schema);
    }

    /**
     * Structurally validates a compact key whose schema is not present in the companion protocol.
     *
     * <p>Schema-unbound rows can be compared and forwarded byte-for-byte, but cannot be inspected by
     * column because the wire format contains column ids and physical types, not column names.
     * {@link #schema} returns an empty schema, and every {@link RowView} accessor except
     * {@link #cellCount} throws {@link UnsupportedOperationException}.
     *
     * @throws MalformedRowException if the bytes cannot be decoded
     * @throws InvalidRowException if the decoded row does not match the format
     */
    public static ProtoWireRow fromSchemaUnboundWire(ByteString wireBytes) {
        return ProtoWireRowDecoder.decodeSchemaUnbound(wireBytes);
    }

    /**
     * Wraps the bytes of a trusted row without scanning them; see
     * {@link ProtoWireRowWriter#finishTrusted}, which checks that the row has one cell per
     * schema column.
     */
    static ProtoWireRow fromTrustedWriter(ByteString wireBytes, TableSchema schema) {
        return new ProtoWireRow(wireBytes, schema, true, schema.getColumnsCount(), 0, null);
    }

    public ByteString wireBytes() {
        return wireBytes;
    }

    public TableSchema schema() {
        return schema;
    }

    public boolean isSchemaBound() {
        return schemaBound;
    }

    /**
     * Rejects a row that is valid as companion input but exceeds the output value-size policy.
     */
    public void validateForOutput() {
        if (oversizedOutputValueLength != 0) {
            throw new InvalidRowException(
                    "Unsupported string-like data length " + oversizedOutputValueLength
            );
        }
    }

    @Override
    public int cellCount() {
        return cellCount;
    }

    public boolean isNull(int columnId) {
        return type(columnId) == ColumnValueType.NULL;
    }

    @Override
    public ColumnValueType type(int columnId) {
        DecodedCells current = cells();
        validateSchemaColumnId(columnId);
        return ColumnValueType.fromValue(current.type(columnId));
    }

    @Override
    public long getInt64(int columnId) {
        DecodedCells current = cells();
        validateType(columnId, ColumnValueType.INT64, current);
        return current.value(columnId);
    }

    @Override
    public long getUint64Bits(int columnId) {
        DecodedCells current = cells();
        validateType(columnId, ColumnValueType.UINT64, current);
        return current.value(columnId);
    }

    @Override
    public double getDouble(int columnId) {
        DecodedCells current = cells();
        validateType(columnId, ColumnValueType.DOUBLE, current);
        return Double.longBitsToDouble(current.value(columnId));
    }

    @Override
    public boolean getBoolean(int columnId) {
        DecodedCells current = cells();
        validateType(columnId, ColumnValueType.BOOLEAN, current);
        return current.value(columnId) != 0;
    }

    @Override
    public int valueLength(int columnId) {
        DecodedCells current = cells();
        validateStringLike(columnId, current);
        return current.valueLength(columnId);
    }

    @Override
    public ByteString bytes(int columnId) {
        DecodedCells current = cells();
        validateStringLike(columnId, current);
        int offset = current.valueOffset(columnId);
        return wireBytes.substring(offset, offset + current.valueLength(columnId));
    }

    /**
     * Returns the encoded cell length after checking the output value-size policy.
     */
    public int encodedCellLengthForOutput(int columnId) {
        return rangeLength(encodedCellRangeForOutput(columnId));
    }

    /**
     * Returns the wire range of an encoded cell (see {@link CompactWireFormat#packRange}) after
     * checking the output value-size policy.
     */
    long encodedCellRangeForOutput(int columnId) {
        DecodedCells current = cells();
        validateSchemaColumnId(columnId);
        validateOutputValueLength(current, columnId);
        return packRange(current.encodedCellOffset(columnId), current.encodedCellLength(columnId));
    }

    /**
     * Returns the wire range of the consecutive encoded cells {@code [fromColumnId, toColumnId)}
     * after checking the output value-size policy for each of them.
     */
    long encodedCellsRangeForOutput(int fromColumnId, int toColumnId) {
        DecodedCells current = cells();
        Objects.checkFromToIndex(fromColumnId, toColumnId, cellCount);
        // Trusted rows and most input rows have no oversized value, so the per-cell check is rare.
        if (oversizedOutputValueLength != 0) {
            for (int columnId = fromColumnId; columnId < toColumnId; columnId++) {
                validateOutputValueLength(current, columnId);
            }
        }
        int start = current.encodedCellOffset(fromColumnId);
        return packRange(start, current.encodedCellOffset(toColumnId) - start);
    }

    private static void validateOutputValueLength(DecodedCells current, int columnId) {
        ColumnValueType type = ColumnValueType.fromValue(current.type(columnId));
        if (type.isStringLikeType()) {
            ProtoWireRowWriter.validateStringLikeValueLength(type, current.valueLength(columnId));
        }
    }

    /**
     * Writes this row as a YSON map without materializing legacy row/value objects.
     * Intended for debugging and tests; it copies every string-like value.
     */
    public void writeTo(YsonConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        DecodedCells current = cells();
        consumer.onBeginMap();
        for (int columnId = 0; columnId < cellCount; columnId++) {
            consumer.onKeyedItem(schema.getColumnName(columnId));
            writeValueTo(current, columnId, consumer);
        }
        consumer.onEndMap();
    }

    private void writeValueTo(DecodedCells current, int columnId, YsonConsumer consumer) {
        ColumnValueType type = ColumnValueType.fromValue(current.type(columnId));
        switch (type) {
            case NULL:
                consumer.onEntity();
                break;
            case INT64:
                consumer.onInteger(current.value(columnId));
                break;
            case UINT64:
                consumer.onUnsignedInteger(current.value(columnId));
                break;
            case DOUBLE:
                consumer.onDouble(Double.longBitsToDouble(current.value(columnId)));
                break;
            case BOOLEAN:
                consumer.onBoolean(current.value(columnId) != 0);
                break;
            case STRING: {
                byte[] value = bytes(columnId).toByteArray();
                consumer.onString(value, 0, value.length);
                break;
            }
            case ANY:
            case COMPOSITE:
                new YsonParser(bytes(columnId).toByteArray()).parseNode(consumer);
                break;
            default:
                throw new IllegalArgumentException("Cannot convert " + type + " to YTree");
        }
    }

    private DecodedCells cells() {
        if (!schemaBound) {
            throw new UnsupportedOperationException("Schema-unbound compact row has no column schema");
        }
        if (eagerCells != null) {
            return eagerCells;
        }
        DecodedCells current = lazyCells;
        if (current == null) {
            synchronized (this) {
                current = lazyCells;
                if (current == null) {
                    current = ProtoWireRowDecoder.decodeCells(wireBytes, schema);
                    // Publish only complete, validated cells to all readers.
                    lazyCells = current;
                }
            }
        }
        return current;
    }

    /**
     * Returns the decoded cells without decoding them, or null. Package-private for tests.
     */
    @Nullable Object builtCells() {
        return eagerCells != null ? eagerCells : lazyCells;
    }

    /**
     * Returns the number of slots in the decoded cells, or -1. Package-private for tests.
     */
    int decodedCellsCapacity() {
        DecodedCells current = eagerCells != null ? eagerCells : lazyCells;
        return current == null ? -1 : current.capacity();
    }

    private void validateSchemaColumnId(int columnId) {
        // The row's own count, not the schema's: the decoded cells have exactly cellCount entries.
        if (columnId < 0 || columnId >= cellCount) {
            throw new IllegalArgumentException("Column not found in schema. (ColumnId: " + columnId + ")");
        }
    }

    private void validateType(int columnId, ColumnValueType expected, DecodedCells current) {
        validateSchemaColumnId(columnId);
        int actual = current.type(columnId);
        if (actual != expected.getValue()) {
            throw new IllegalArgumentException(
                    "Unexpected column type. (ColumnId: %d, Expected: %s, Actual: %s)"
                            .formatted(columnId, expected, ColumnValueType.fromValue(actual))
            );
        }
    }

    private void validateStringLike(int columnId, DecodedCells current) {
        validateSchemaColumnId(columnId);
        int actual = current.type(columnId);
        if (actual != ColumnValueType.STRING.getValue() && actual != ColumnValueType.ANY.getValue()
                && actual != ColumnValueType.COMPOSITE.getValue()) {
            throw new IllegalArgumentException(ColumnValueType.fromValue(actual) + " is not a string-like value");
        }
    }

}
