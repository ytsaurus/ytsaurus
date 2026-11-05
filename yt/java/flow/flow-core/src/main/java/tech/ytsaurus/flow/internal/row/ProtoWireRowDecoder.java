package tech.ytsaurus.flow.internal.row;

import java.util.Objects;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static tech.ytsaurus.flow.internal.row.CompactWireFormat.LAST_UINT64_BYTE_OVERFLOW_MASK;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.MAX_UINT64_VARINT_BYTES;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.malformedVarint;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.nextOffset;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.rangeEnd;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.rangeLength;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.readFixed64;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.readUInt32;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.requireRemaining;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.skipBytes;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.skipUInt64;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.valueType;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.varintValue;

/**
 * Decodes and validates rows in the Flow companion compact wire format.
 *
 * <p>Wire layout (fields have no protobuf tags):
 * <pre>{@code
 * row  = version:varuint32 (= 0) | cellCount:varuint32 | cell[cellCount]
 * cell = columnId:varuint32 | type:varuint32 | value
 *
 * type (ColumnValueType)        value
 * INT64                        ZigZag-encoded varuint64
 * UINT64                       varuint64
 * DOUBLE                       eight bytes, little-endian IEEE 754
 * BOOLEAN                      one byte (writers emit 0 or 1; readers treat any byte but 1 as false)
 * STRING / ANY / COMPOSITE      byteLength:varuint32 | bytes[byteLength]
 * NULL / MIN / MAX / THE_BOTTOM no value bytes
 * }</pre>
 * A column belongs to the schema; a cell contains its column id, type and value.
 * Schema-bound rows contain every schema column in order, with ids from zero to cellCount - 1.
 * Unset values use NULL cells. Schema-unbound keys preserve wire order when their schema
 * is not available to the SDK.
 *
 * <p>Input values may be larger than YT's output limit: external sources can supply them,
 * and user code can filter or rewrite them before emission. Decoding rejects malformed input
 * immediately and records the output restriction for {@link ProtoWireRow#validateForOutput}.
 *
 * <p>Row content errors: {@link MalformedRowException} when the bytes cannot be decoded
 * (truncated value, invalid varint, unknown value type) and {@link InvalidRowException} when a
 * decoded row is invalid (version, cell count, column order, schema type, value length or
 * trailing bytes). Both are unchecked because a trusted row is validated inside its accessors.
 * Two range checks, of the cell count and of a column id, use the YT client's
 * {@code WireProtocol} validators and throw a plain {@link IllegalStateException}.
 */
public final class ProtoWireRowDecoder {
    private static final TableSchema EMPTY_SCHEMA = TableSchema.builder().build();

    private ProtoWireRowDecoder() {
    }

    /**
     * Validates a schema-bound row and decodes its cells in one pass.
     *
     * @throws MalformedRowException if the bytes cannot be decoded
     * @throws InvalidRowException if the row does not match the format or schema
     */
    public static ProtoWireRow decode(ByteString wireBytes, TableSchema schema) {
        Objects.requireNonNull(wireBytes, "wireBytes");
        Objects.requireNonNull(schema, "schema");
        long packedHeader = readHeader(wireBytes, schema);
        int cellCount = varintValue(packedHeader);
        var cells = DecodedCells.create(cellCount);
        int oversizedOutputValueLength = scan(wireBytes, schema, cells, packedHeader);
        return new ProtoWireRow(wireBytes, schema, true, cellCount, oversizedOutputValueLength, cells);
    }

    /**
     * Validates a key whose schema is unavailable, preserving its bytes and column order.
     *
     * @throws MalformedRowException if the bytes cannot be decoded
     * @throws InvalidRowException if the row does not match the format
     */
    public static ProtoWireRow decodeSchemaUnbound(ByteString wireBytes) {
        Objects.requireNonNull(wireBytes, "wireBytes");
        long packedHeader = readHeader(wireBytes, null);
        int cellCount = varintValue(packedHeader);
        int oversizedOutputValueLength = scanSchemaUnbound(wireBytes, packedHeader);
        return new ProtoWireRow(wireBytes, EMPTY_SCHEMA, false, cellCount, oversizedOutputValueLength, null);
    }

    /**
     * Decodes a trusted writer result before the row publishes its cells to readers.
     * The writer has already enforced output value-size limits.
     */
    static DecodedCells decodeCells(ByteString wireBytes, TableSchema schema) {
        long packedHeader = readHeader(wireBytes, schema);
        var cells = DecodedCells.create(varintValue(packedHeader));
        scan(wireBytes, schema, cells, packedHeader);
        return cells;
    }

    /**
     * Validates the header before allocating decoded cells and returns the cell count packed with
     * the offset of the first cell, like {@link CompactWireFormat#readUInt32}.
     */
    static long readHeader(ByteString wireBytes, @Nullable TableSchema schema) {
        int offset = 0;
        long decoded = readUInt32(wireBytes, offset);
        int version = varintValue(decoded);
        offset = nextOffset(decoded);
        if (version != 0) {
            throw new InvalidRowException("Unversioned row does not support versions.");
        }

        decoded = readUInt32(wireBytes, offset);
        int cellCount = varintValue(decoded);
        offset = nextOffset(decoded);
        // 0xFFFFFFFF is the YT wire marker of a null row.
        if (cellCount == -1) {
            throw new InvalidRowException("Null rows cannot be represented as compact rows.");
        }
        WireProtocol.validateRowValueCount(cellCount);
        if (schema != null && cellCount != schema.getColumnsCount()) {
            throw new InvalidRowException(
                    "Row cell count does not match schema. (CellCount: %d, SchemaColumnCount: %d)"
                            .formatted(cellCount, schema.getColumnsCount())
            );
        }
        return decoded;
    }

    /**
     * Validates the cells of a schema-bound row and fills {@code cells}. Column ids must equal
     * their positions and cell types must match the schema.
     *
     * @param packedHeader the result of {@link #readHeader}: the cell count packed with the
     * offset of the first cell
     * @return the length of the first string-like value above the output limit, or 0; see
     * {@link ProtoWireRow#validateForOutput}
     */
    private static int scan(ByteString wireBytes, TableSchema schema, DecodedCells cells, long packedHeader) {
        int cellCount = varintValue(packedHeader);
        int offset = nextOffset(packedHeader);
        int oversizedOutputValueLength = 0;
        for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
            // Keep this shape: a variant that looked the schema up by cellIndex and read the type
            // at nextOffset(decoded) without updating offset measured 9-15% slower on integer
            // rows, because C2 compiled the loop differently.
            int encodedCellOffset = offset;
            long decoded = readUInt32(wireBytes, offset);
            int columnId = varintValue(decoded);
            offset = nextOffset(decoded);
            requireColumnId(columnId, cellIndex);

            decoded = readUInt32(wireBytes, offset);
            ColumnValueType type = valueType(varintValue(decoded));
            offset = nextOffset(decoded);
            validateSchemaType(schema, columnId, type);

            // Value slot: raw scalar bits, or the wire range of a string-like value.
            long slot = 0;
            switch (type) {
                case INT64, UINT64 -> {
                    // Keep one-byte integers inline: the multibyte decoder is too large for
                    // HotSpot to inline into this loop.
                    requireRemaining(wireBytes, offset, 1);
                    int first = wireBytes.byteAt(offset++);
                    long rawBits = first & 0x7f;
                    if (first < 0) {
                        offset = readMultibyteInteger(wireBytes, offset, first, type, cells, cellIndex, encodedCellOffset);
                        // The helper has already stored this cell's decoded data.
                        continue;
                    }
                    slot = type == ColumnValueType.INT64 ? CodedInputStream.decodeZigZag64(rawBits) : rawBits;
                }
                case DOUBLE -> {
                    requireRemaining(wireBytes, offset, Long.BYTES);
                    slot = readFixed64(wireBytes, offset);
                    offset += Long.BYTES;
                }
                case BOOLEAN -> {
                    requireRemaining(wireBytes, offset, 1);
                    // Like the C++ reader, only 1 is true. Copied cells keep the original byte.
                    slot = wireBytes.byteAt(offset) == 1 ? 1 : 0;
                    offset++;
                }
                case STRING, ANY, COMPOSITE -> {
                    slot = readValueRange(wireBytes, offset, type);
                    int valueLength = rangeLength(slot);
                    if (oversizedOutputValueLength == 0
                            && !ProtoWireRowWriter.isStringLikeValueLengthValid(type, valueLength)) {
                        oversizedOutputValueLength = valueLength;
                    }
                    offset = rangeEnd(slot);
                }
                default -> {
                    // NULL, MIN, MAX and THE_BOTTOM have no value bytes.
                }
            }
            cells.set(cellIndex, type.getValue(), slot, encodedCellOffset);
        }
        cells.end(offset);
        requireRowEnd(wireBytes, offset);
        return oversizedOutputValueLength;
    }

    /**
     * Reads the remaining bytes of an INT64 or UINT64 value and stores it in {@code cells}.
     * Writing the value here lets the method return the next offset without allocating a result
     * object for the two results.
     *
     * @param offset the position immediately after the first byte of the value
     * @param first the signed first byte, whose continuation bit is set
     * @return the offset immediately after the value
     */
    private static int readMultibyteInteger(
            ByteString wireBytes,
            int offset,
            int first,
            ColumnValueType type,
            DecodedCells cells,
            int cellIndex,
            int encodedCellOffset
    ) {
        long rawBits = first & 0x7f;
        if (wireBytes.size() - offset >= MAX_UINT64_VARINT_BYTES - 1) {
            // Constant shifts avoid the dependency chain of a varint loop. The
            // remaining width is checked once, so bytes are read without checks.
            do {
                int b1 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b1 & 0x7f) << 7;
                if (b1 >= 0) {
                    break;
                }
                int b2 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b2 & 0x7f) << 14;
                if (b2 >= 0) {
                    break;
                }
                int b3 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b3 & 0x7f) << 21;
                if (b3 >= 0) {
                    break;
                }
                int b4 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b4 & 0x7f) << 28;
                if (b4 >= 0) {
                    break;
                }
                int b5 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b5 & 0x7f) << 35;
                if (b5 >= 0) {
                    break;
                }
                int b6 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b6 & 0x7f) << 42;
                if (b6 >= 0) {
                    break;
                }
                int b7 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b7 & 0x7f) << 49;
                if (b7 >= 0) {
                    break;
                }
                int b8 = wireBytes.byteAt(offset++);
                rawBits |= (long) (b8 & 0x7f) << 56;
                if (b8 >= 0) {
                    break;
                }
                int b9 = wireBytes.byteAt(offset++);
                if ((b9 & LAST_UINT64_BYTE_OVERFLOW_MASK) != 0) {
                    throw malformedVarint();
                }
                rawBits |= (long) b9 << 63;
            } while (false);
        } else {
            // Fewer than nine bytes follow, so the input ends before a tenth
            // byte: the varint either terminates or is truncated.
            for (int shift = 7; ; shift += 7) {
                requireRemaining(wireBytes, offset, 1);
                int current = wireBytes.byteAt(offset++);
                rawBits |= (long) (current & 0x7f) << shift;
                if (current >= 0) {
                    break;
                }
            }
        }
        long slot = type == ColumnValueType.INT64 ? CodedInputStream.decodeZigZag64(rawBits) : rawBits;
        cells.set(cellIndex, type.getValue(), slot, encodedCellOffset);
        return offset;
    }

    /**
     * Validates the cells of a schema-unbound key: column ids are only range-checked
     * and scalar values are skipped.
     *
     * @param packedHeader the result of {@link #readHeader}: the cell count packed with the
     * offset of the first cell
     * @return the length of the first string-like value above the output limit, or 0; see
     * {@link ProtoWireRow#validateForOutput}
     */
    private static int scanSchemaUnbound(ByteString wireBytes, long packedHeader) {
        int cellCount = varintValue(packedHeader);
        int offset = nextOffset(packedHeader);
        int oversizedOutputValueLength = 0;
        for (int cellIndex = 0; cellIndex < cellCount; cellIndex++) {
            long decoded = readUInt32(wireBytes, offset);
            WireProtocol.validateColumnId(varintValue(decoded));
            decoded = readUInt32(wireBytes, nextOffset(decoded));
            ColumnValueType type = valueType(varintValue(decoded));
            offset = nextOffset(decoded);

            switch (type) {
                case INT64, UINT64 -> offset = skipUInt64(wireBytes, offset);
                case DOUBLE -> offset = skipBytes(wireBytes, offset, Long.BYTES);
                case BOOLEAN -> offset = skipBytes(wireBytes, offset, 1);
                case STRING, ANY, COMPOSITE -> {
                    long range = readValueRange(wireBytes, offset, type);
                    int valueLength = rangeLength(range);
                    if (oversizedOutputValueLength == 0
                            && !ProtoWireRowWriter.isStringLikeValueLengthValid(type, valueLength)) {
                        oversizedOutputValueLength = valueLength;
                    }
                    offset = rangeEnd(range);
                }
                default -> {
                    // NULL, MIN, MAX and THE_BOTTOM have no value bytes.
                }
            }
        }
        requireRowEnd(wireBytes, offset);
        return oversizedOutputValueLength;
    }

    /**
     * Reads the length prefix of a string-like value and returns the value's wire range (see
     * {@link CompactWireFormat#packRange}) after checking that the input contains it.
     */
    private static long readValueRange(ByteString wireBytes, int offset, ColumnValueType type) {
        long decoded = readUInt32(wireBytes, offset);
        int valueLength = varintValue(decoded);
        if (valueLength < 0) {
            throw new InvalidRowException(
                    "Unsupported " + type + " data length " + Integer.toUnsignedLong(valueLength));
        }
        requireRemaining(wireBytes, nextOffset(decoded), valueLength);
        // The length packed with the offset after it is already the value's range.
        return decoded;
    }

    /**
     * Requires the wire column id of a schema-bound cell to equal its position.
     */
    private static void requireColumnId(int columnId, int position) {
        WireProtocol.validateColumnId(columnId);
        if (columnId != position) {
            throw new InvalidRowException(
                    "Column id does not match its position. (Expected: %d, Actual: %d)"
                            .formatted(position, columnId)
            );
        }
    }

    private static void requireRowEnd(ByteString wireBytes, int offset) {
        if (offset != wireBytes.size()) {
            throw new InvalidRowException(
                    "Trailing bytes after compact row. (Remaining: " + (wireBytes.size() - offset) + ")"
            );
        }
    }

    private static void validateSchemaType(TableSchema schema, int columnId, ColumnValueType actualType) {
        if (actualType == ColumnValueType.NULL
                || actualType == ColumnValueType.MIN
                || actualType == ColumnValueType.MAX
                || actualType == ColumnValueType.THE_BOTTOM) {
            return;
        }
        ColumnValueType expectedType = schema.getColumnSchema(columnId).getWireType();
        if (actualType != expectedType) {
            throw new InvalidRowException(
                    "Column type does not match schema. (ColumnId: %d, Expected: %s, Actual: %s)"
                            .formatted(columnId, expectedType, actualType)
            );
        }
    }

}
