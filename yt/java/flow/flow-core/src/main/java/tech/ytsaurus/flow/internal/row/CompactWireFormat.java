package tech.ytsaurus.flow.internal.row;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.tables.ColumnValueType;

/**
 * Checked decoding primitives of the compact wire format described in {@link ProtoWireRow}.
 *
 * <p>Methods return primitives only, so row scans stay allocation-free. Two results are packed
 * into one {@code long}: {@link #readUInt32} packs a value with the offset after it, and
 * {@link #packRange} packs the offset and length of a byte range.
 *
 * <p>Truncated input, invalid varints and unknown value types throw
 * {@link MalformedRowException}.
 */
final class CompactWireFormat {
    private static final int MAX_UINT32_VARINT_BYTES = 5;
    static final int MAX_UINT64_VARINT_BYTES = 10;
    /**
     * The fifth byte of a uint32 varint may carry only bits 28..31.
     */
    private static final int LAST_UINT32_BYTE_OVERFLOW_MASK = 0xf0;
    /**
     * The tenth byte of a uint64 varint may carry only bit 63.
     */
    static final int LAST_UINT64_BYTE_OVERFLOW_MASK = 0xfe;

    private static final ColumnValueType[] VALUE_TYPES = new ColumnValueType[1 << Byte.SIZE];

    static {
        for (var type : ColumnValueType.values()) {
            VALUE_TYPES[type.getValue()] = type;
        }
    }

    private CompactWireFormat() {
    }

    /**
     * Reads a uint32 varint and packs it with the offset after it; see {@link #varintValue} and
     * {@link #nextOffset}. The packing matches {@link #packRange}: for a length prefix, the result
     * is the range of the bytes that follow.
     */
    static long readUInt32(ByteString bytes, int offset) {
        requireRemaining(bytes, offset, 1);
        int first = bytes.byteAt(offset++);
        if (first >= 0) {
            return ((long) offset << Integer.SIZE) | first;
        }
        int value = first & 0x7f;
        for (int byteIndex = 1; byteIndex < MAX_UINT32_VARINT_BYTES; byteIndex++) {
            requireRemaining(bytes, offset, 1);
            int current = bytes.byteAt(offset++) & 0xff;
            if (byteIndex == MAX_UINT32_VARINT_BYTES - 1 && (current & LAST_UINT32_BYTE_OVERFLOW_MASK) != 0) {
                throw malformedVarint();
            }
            value |= (current & 0x7f) << (byteIndex * 7);
            if ((current & 0x80) == 0) {
                return ((long) offset << Integer.SIZE) | (value & 0xffffffffL);
            }
        }
        throw malformedVarint();
    }

    static int varintValue(long packed) {
        return (int) packed;
    }

    static int nextOffset(long packed) {
        return (int) (packed >>> Integer.SIZE);
    }

    /**
     * Reads a uint64 varint when only its value is needed.
     */
    static long readUInt64(ByteString bytes, int offset) {
        long value = 0;
        for (int byteIndex = 0; byteIndex < MAX_UINT64_VARINT_BYTES; byteIndex++) {
            requireRemaining(bytes, offset, 1);
            int current = bytes.byteAt(offset++) & 0xff;
            if (byteIndex == MAX_UINT64_VARINT_BYTES - 1 && (current & LAST_UINT64_BYTE_OVERFLOW_MASK) != 0) {
                throw malformedVarint();
            }
            value |= (long) (current & 0x7f) << (byteIndex * 7);
            if ((current & 0x80) == 0) {
                return value;
            }
        }
        throw malformedVarint();
    }

    /**
     * Validates a uint64 varint and returns the offset after it.
     */
    static int skipUInt64(ByteString bytes, int offset) {
        if (bytes.size() - offset >= MAX_UINT64_VARINT_BYTES) {
            // The remaining width is checked once, so bytes are read without checks.
            for (int byteIndex = 0; byteIndex < MAX_UINT64_VARINT_BYTES - 1; byteIndex++) {
                if (bytes.byteAt(offset++) >= 0) {
                    return offset;
                }
            }
            if ((bytes.byteAt(offset++) & LAST_UINT64_BYTE_OVERFLOW_MASK) != 0) {
                throw malformedVarint();
            }
            return offset;
        }
        for (int byteIndex = 0; byteIndex < MAX_UINT64_VARINT_BYTES; byteIndex++) {
            requireRemaining(bytes, offset, 1);
            int current = bytes.byteAt(offset++) & 0xff;
            if (byteIndex == MAX_UINT64_VARINT_BYTES - 1 && (current & LAST_UINT64_BYTE_OVERFLOW_MASK) != 0) {
                throw malformedVarint();
            }
            if ((current & 0x80) == 0) {
                return offset;
            }
        }
        throw malformedVarint();
    }

    /**
     * Reads eight little-endian bytes that the caller has already checked with
     * {@link #requireRemaining}.
     */
    static long readFixed64(ByteString bytes, int offset) {
        long value = 0;
        for (int byteIndex = 0; byteIndex < Long.BYTES; byteIndex++) {
            value |= (long) (bytes.byteAt(offset + byteIndex) & 0xff) << (byteIndex * Byte.SIZE);
        }
        return value;
    }

    /**
     * Checks that {@code length} bytes follow {@code offset} and returns the offset after them.
     */
    static int skipBytes(ByteString bytes, int offset, int length) {
        requireRemaining(bytes, offset, length);
        return offset + length;
    }

    /**
     * Packs a byte range into one value; see {@link #rangeOffset}, {@link #rangeLength} and
     * {@link #rangeEnd}.
     */
    static long packRange(int offset, int length) {
        return ((long) offset << Integer.SIZE) | (length & 0xffffffffL);
    }

    static int rangeOffset(long range) {
        return (int) (range >>> Integer.SIZE);
    }

    static int rangeLength(long range) {
        return (int) range;
    }

    static int rangeEnd(long range) {
        return rangeOffset(range) + rangeLength(range);
    }

    /**
     * Returns the value type with the given wire code.
     */
    static ColumnValueType valueType(int code) {
        ColumnValueType type = code >= 0 && code < VALUE_TYPES.length ? VALUE_TYPES[code] : null;
        if (type == null) {
            throw new MalformedRowException("Unsupported value type 0x" + Integer.toHexString(code));
        }
        return type;
    }

    static void requireRemaining(ByteString bytes, int offset, int length) {
        if (length < 0 || offset < 0 || offset > bytes.size() - length) {
            throw new MalformedRowException("Malformed compact row: truncated value");
        }
    }

    static MalformedRowException malformedVarint() {
        return new MalformedRowException("Malformed compact row: invalid varint");
    }
}
