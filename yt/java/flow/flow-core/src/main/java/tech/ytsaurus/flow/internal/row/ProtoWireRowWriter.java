package tech.ytsaurus.flow.internal.row;

import java.util.Arrays;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * Direct sequential writer for one Flow companion compact row with column ids 0..cellCount-1.
 */
public final class ProtoWireRowWriter {
    private final int cellCount;
    private byte[] output;
    private int outputSize;
    private int writtenCellCount;
    private boolean finished;

    public ProtoWireRowWriter(int cellCount) {
        this(cellCount, Math.max(16, cellCount * 4 + 2));
    }

    public ProtoWireRowWriter(int cellCount, int estimatedSize) {
        WireProtocol.validateRowValueCount(cellCount);
        this.cellCount = cellCount;
        this.output = new byte[Math.max(2, estimatedSize)];
        writeUnsignedVarint(0);
        writeUnsignedVarint(cellCount);
    }

    /**
     * Returns the exact number of bytes used by the compact row prefix.
     */
    public static int encodedRowHeaderSize(int cellCount) {
        WireProtocol.validateRowValueCount(cellCount);
        return 1 + unsignedVarintSize(cellCount);
    }

    /**
     * Returns the exact encoded size of one cell. {@code rawBits} contains the scalar value (or
     * raw double bits); {@code byteValue} is required only for string-like types.
     */
    public static int encodedCellSize(
            int columnId,
            ColumnValueType type,
            long rawBits,
            @Nullable ByteString byteValue
    ) {
        WireProtocol.validateColumnId(columnId);
        int size = unsignedVarintSize(columnId) + unsignedVarintSize(type.getValue());
        switch (type) {
            case INT64:
                return size + unsignedVarintSize(CodedOutputStream.encodeZigZag64(rawBits));
            case UINT64:
                return size + unsignedVarintSize(rawBits);
            case DOUBLE:
                return size + Long.BYTES;
            case BOOLEAN:
                return size + 1;
            case STRING:
            case ANY:
            case COMPOSITE:
                if (byteValue == null) {
                    throw new IllegalArgumentException(type + " requires a byte value");
                }
                // The length limit keeps the sum far below Integer.MAX_VALUE.
                validateStringLikeValueLength(type, byteValue.size());
                return size + unsignedVarintSize(byteValue.size()) + byteValue.size();
            default:
                return size;
        }
    }

    public void writeNull(int columnId) {
        writeEntity(columnId, ColumnValueType.NULL);
    }

    public void writeEntity(int columnId, ColumnValueType type) {
        if (type.isValueType() || type.isStringLikeType()) {
            throw new IllegalArgumentException(type + " requires a value");
        }
        writeHeader(columnId, type);
        cellWritten();
    }

    public void writeInt64(int columnId, long value) {
        writeHeader(columnId, ColumnValueType.INT64);
        writeUnsignedVarint(CodedOutputStream.encodeZigZag64(value));
        cellWritten();
    }

    public void writeUint64Bits(int columnId, long value) {
        writeHeader(columnId, ColumnValueType.UINT64);
        writeUnsignedVarint(value);
        cellWritten();
    }

    public void writeDouble(int columnId, double value) {
        writeHeader(columnId, ColumnValueType.DOUBLE);
        writeFixed64(Double.doubleToRawLongBits(value));
        cellWritten();
    }

    public void writeBoolean(int columnId, boolean value) {
        writeHeader(columnId, ColumnValueType.BOOLEAN);
        writeByte(value ? 1 : 0);
        cellWritten();
    }

    public void writeBytes(int columnId, ColumnValueType type, ByteString value) {
        if (!type.isStringLikeType()) {
            throw new IllegalArgumentException(type + " is not a string-like value");
        }
        validateStringLikeValueLength(type, value.size());
        writeHeader(columnId, type);
        writeUnsignedVarint(value.size());
        append(value);
        cellWritten();
    }

    /**
     * Copies a cell from an immutable row view.
     *
     * <p>A {@link ProtoWireRow} cell copied into the same column id keeps its original bytes,
     * including non-canonical encodings such as a BOOLEAN byte 2, and is copied without creating
     * a value object or byte slice. Other cells are re-encoded from their decoded values.
     */
    public void writeFrom(int columnId, RowView source, int sourceColumnId) {
        // An encoded cell starts with its own column id, so it is copied verbatim only into the
        // same position.
        if (source instanceof ProtoWireRow protoWireRow && columnId == sourceColumnId) {
            validateNextColumn(columnId);
            appendEncodedCells(protoWireRow, protoWireRow.encodedCellRangeForOutput(columnId), 1);
            return;
        }
        ColumnValueType type = source.type(sourceColumnId);
        switch (type) {
            case NULL:
            case MIN:
            case MAX:
            case THE_BOTTOM:
                writeEntity(columnId, type);
                break;
            case INT64:
                writeInt64(columnId, source.getInt64(sourceColumnId));
                break;
            case UINT64:
                writeUint64Bits(columnId, source.getUint64Bits(sourceColumnId));
                break;
            case DOUBLE:
                writeDouble(columnId, source.getDouble(sourceColumnId));
                break;
            case BOOLEAN:
                writeBoolean(columnId, source.getBoolean(sourceColumnId));
                break;
            case STRING:
            case ANY:
            case COMPOSITE:
                writeBytes(columnId, type, source.bytes(sourceColumnId));
                break;
        }
    }

    /**
     * Copies the consecutive cells {@code [fromColumnId, toColumnId)} of {@code source} into the
     * same column ids. Equivalent to calling {@link #writeFrom(int, RowView, int)} for each id,
     * with one bounds check and one byte copy.
     */
    public void writeFrom(ProtoWireRow source, int fromColumnId, int toColumnId) {
        long range = source.encodedCellsRangeForOutput(fromColumnId, toColumnId);
        int count = toColumnId - fromColumnId;
        if (count == 0) {
            return;
        }
        validateNextColumn(fromColumnId);
        if (count > cellCount - writtenCellCount) {
            throw new IllegalStateException("Attempted to write more than " + cellCount + " cells");
        }
        appendEncodedCells(source, range, count);
    }

    public ByteString finish() {
        ensureOpen();
        if (writtenCellCount != cellCount) {
            throw new IllegalStateException(
                    "Unexpected number of cells. (Expected: %d, Written: %d)"
                            .formatted(cellCount, writtenCellCount)
            );
        }
        if (outputSize < output.length / 2 && output.length - outputSize > 4096) {
            output = Arrays.copyOf(output, outputSize);
        }
        finished = true;
        return UnsafeByteOperations.unsafeWrap(output, 0, outputSize);
    }

    /**
     * Wraps the written bytes as a row without scanning them. The caller guarantees that every
     * cell's type matches {@code schema}. The first column access decodes the cells and checks
     * the format and schema, so a type mismatch surfaces there; rows that are only forwarded
     * are never scanned.
     */
    public ProtoWireRow finishTrusted(TableSchema schema) {
        if (cellCount != schema.getColumnsCount()) {
            throw new IllegalArgumentException(
                    "Trusted dense row must match its schema. (CellCount: %d, SchemaColumnCount: %d)"
                            .formatted(cellCount, schema.getColumnsCount())
            );
        }
        return ProtoWireRow.fromTrustedWriter(finish(), schema);
    }

    int retainedCapacity() {
        return output.length;
    }

    static void validateStringLikeValueLength(ColumnValueType type, int length) {
        if (!isStringLikeValueLengthValid(type, length)) {
            throw new InvalidRowException("Unsupported " + type + " data length " + length);
        }
    }

    static boolean isStringLikeValueLengthValid(ColumnValueType type, int length) {
        return switch (type) {
            case STRING -> length >= 0 && length <= WireProtocol.MAX_STRING_VALUE_LENGTH;
            case ANY -> length >= 0 && length <= WireProtocol.MAX_ANY_VALUE_LENGTH;
            case COMPOSITE -> length >= 0 && length <= WireProtocol.MAX_COMPOSITE_VALUE_LENGTH;
            default -> true;
        };
    }

    private void writeHeader(int columnId, ColumnValueType type) {
        validateNextColumn(columnId);
        writeUnsignedVarint(columnId);
        writeUnsignedVarint(type.getValue());
    }

    private void appendEncodedCells(ProtoWireRow source, long range, int count) {
        int length = CompactWireFormat.rangeLength(range);
        ensureCapacity(length);
        source.wireBytes().copyTo(output, CompactWireFormat.rangeOffset(range), outputSize, length);
        outputSize += length;
        writtenCellCount += count;
    }

    private void validateNextColumn(int columnId) {
        ensureOpen();
        if (writtenCellCount >= cellCount) {
            throw new IllegalStateException("Attempted to write more than " + cellCount + " cells");
        }
        WireProtocol.validateColumnId(columnId);
        if (columnId != writtenCellCount) {
            throw new IllegalStateException(
                    "Column id does not match its position. (Expected: %d, Actual: %d)"
                            .formatted(writtenCellCount, columnId)
            );
        }
    }

    private void writeUnsignedVarint(long value) {
        ensureCapacity(unsignedVarintSize(value));
        while ((value & ~0x7fL) != 0) {
            output[outputSize++] = (byte) (((int) value & 0x7f) | 0x80);
            value >>>= 7;
        }
        output[outputSize++] = (byte) value;
    }

    private static int unsignedVarintSize(long value) {
        // Keep the common single-byte headers and values cheap.
        return (value & ~0x7fL) == 0 ? 1 : CodedOutputStream.computeUInt64SizeNoTag(value);
    }

    private void writeFixed64(long value) {
        ensureCapacity(Long.BYTES);
        for (int i = 0; i < Long.BYTES; i++) {
            output[outputSize++] = (byte) (value >>> (Byte.SIZE * i));
        }
    }

    private void writeByte(int value) {
        ensureCapacity(1);
        output[outputSize++] = (byte) value;
    }

    private void append(ByteString value) {
        ensureCapacity(value.size());
        value.copyTo(output, 0, outputSize, value.size());
        outputSize += value.size();
    }

    private void ensureCapacity(int additionalSize) {
        if (additionalSize < 0 || additionalSize > Integer.MAX_VALUE - outputSize) {
            throw new IllegalStateException("Compact row is too large");
        }
        int requiredSize = outputSize + additionalSize;
        if (requiredSize <= output.length) {
            return;
        }
        int grownSize = output.length + (output.length >> 1) + 1;
        output = Arrays.copyOf(output, Math.max(requiredSize, grownSize));
    }

    private void cellWritten() {
        writtenCellCount++;
    }

    private void ensureOpen() {
        if (finished) {
            throw new IllegalStateException("Writer is already finished");
        }
    }
}
