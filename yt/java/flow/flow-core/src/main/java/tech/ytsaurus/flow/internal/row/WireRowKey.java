package tech.ytsaurus.flow.internal.row;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static tech.ytsaurus.flow.internal.row.CompactWireFormat.nextOffset;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.readUInt32;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.readUInt64;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.varintValue;

/**
 * Immutable wire key with a cached hash and buffer-range equality.
 *
 * <p>Construction does not validate the whole row: {@link HashMode#LEADING_UINT64} parses only
 * the key prefix. The owner of a key map must validate a key's row, for example with
 * {@link ProtoWireRow#fromWire}, before storing it after a lookup miss.
 */
public final class WireRowKey {
    /**
     * A farm_hash call whose arguments are plain column names; group 1 is the argument list.
     */
    private static final Pattern FULL_HASH_EXPRESSION = Pattern.compile(
            "\\s*farm_hash\\(\\s*([A-Za-z_][A-Za-z_0-9]*(?:\\s*,\\s*[A-Za-z_][A-Za-z_0-9]*)*)\\s*\\)\\s*"
    );
    private static final Pattern ARGUMENT_SEPARATOR = Pattern.compile("\\s*,\\s*");
    /**
     * Keys up to this size use {@link ByteString#hashCode} and {@link ByteString#equals}; larger
     * keys use {@link Arrays#hashCode(byte[])} and vectorized {@link ByteBuffer#equals}, which are
     * faster on long inputs (see ConsumerKeyBenchmark).
     */
    private static final int SMALL_KEY_BYTES = 64;

    public enum HashMode {
        CONTENT,
        /**
         * For a known layout whose first column contains a full UINT64 hash of all other key
         * columns. If the hash covers only some of them, every key that shares those columns
         * gets the same hash code and map lookups degrade to a linear scan.
         * Null or sentinel values fall back to content hashing.
         */
        LEADING_UINT64
    }

    private final ByteString bytes;
    private final HashMode hashMode;
    private final int hash;
    /**
     * The first read-only buffer of a large key, cached for equality; null for small keys.
     */
    private final @Nullable ByteBuffer first;
    /**
     * All buffers of a large key that spans several buffers (a rope); null otherwise.
     */
    private final @Nullable List<ByteBuffer> segments;

    private WireRowKey(ByteString bytes, HashMode hashMode) {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.hashMode = Objects.requireNonNull(hashMode, "hashMode");
        int leadingHashOffset = hashMode == HashMode.LEADING_UINT64 ? leadingHashOffset(bytes) : -1;
        @Nullable List<ByteBuffer> buffers = bytes.size() <= SMALL_KEY_BYTES ? null : bytes.asReadOnlyByteBufferList();
        first = buffers == null ? null : buffers.get(0);
        segments = buffers == null || buffers.size() == 1 ? null : buffers;
        if (leadingHashOffset >= 0) {
            hash = Long.hashCode(readUInt64(bytes, leadingHashOffset));
        } else if (buffers == null) {
            hash = bytes.hashCode();
        } else {
            // Arrays.hashCode is a vectorized HotSpot intrinsic on JDK 21. It takes a whole array,
            // so the key is copied: that costs allocation proportional to the key size and makes
            // the hash independent of the key's buffer boundaries.
            hash = Arrays.hashCode(bytes.toByteArray());
        }
    }

    /**
     * Returns the offset of the leading UINT64 value of a key, or -1 when the first cell holds a
     * null or sentinel value.
     *
     * @throws MalformedRowException if the key prefix cannot be decoded
     * @throws InvalidRowException if the key has an invalid header, no values or a first column
     * other than column zero
     */
    private static int leadingHashOffset(ByteString bytes) {
        long packedHeader = ProtoWireRowDecoder.readHeader(bytes, null);
        if (varintValue(packedHeader) == 0) {
            throw new InvalidRowException("Key has no leading hash column");
        }
        long decoded = readUInt32(bytes, nextOffset(packedHeader));
        if (varintValue(decoded) != 0) {
            throw new InvalidRowException("Leading hash column must have id zero");
        }
        decoded = readUInt32(bytes, nextOffset(decoded));
        return varintValue(decoded) == ColumnValueType.UINT64.getValue() ? nextOffset(decoded) : -1;
    }

    /**
     * Keys made with different hash modes are distinct even when their wire bytes match.
     * A request interner and its state maps must use the same mode.
     */
    public static WireRowKey of(ByteString bytes, HashMode hashMode) {
        return new WireRowKey(bytes, hashMode);
    }

    public ByteString wireBytes() {
        return bytes;
    }

    public HashMode hashMode() {
        return hashMode;
    }

    /**
     * Selects the hash mode for keys of {@code schema}, which must describe exactly the key columns.
     *
     * <p>Recognizes only a first column declared as a full farm_hash of every other column, in any
     * order. Keys whose hash covers only some of the columns, as in {@code [farm_hash(a), a, b]},
     * and keys with reduced or unknown expressions use content hashing. A column name such as
     * "hash" is not sufficient evidence.
     */
    public static HashMode hashModeFor(@Nullable TableSchema schema) {
        if (schema == null || schema.getColumnsCount() == 0) {
            return HashMode.CONTENT;
        }
        var column = schema.getColumnSchema(0);
        var expression = column.getExpression();
        if (column.getWireType() != ColumnValueType.UINT64 || expression == null) {
            return HashMode.CONTENT;
        }
        var matcher = FULL_HASH_EXPRESSION.matcher(expression);
        if (!matcher.matches()) {
            return HashMode.CONTENT;
        }
        List<String> hashedColumns = Arrays.asList(ARGUMENT_SEPARATOR.split(matcher.group(1)));
        for (int columnId = 1; columnId < schema.getColumnsCount(); columnId++) {
            if (!hashedColumns.contains(schema.getColumnName(columnId))) {
                return HashMode.CONTENT;
            }
        }
        return HashMode.LEADING_UINT64;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WireRowKey key) || hashMode != key.hashMode
                || hash != key.hash || bytes.size() != key.bytes.size()) {
            return false;
        }
        if (bytes == key.bytes) {
            return true;
        }
        if (first == null) {
            return bytes.equals(key.bytes);
        }
        if (segments == null && key.segments == null) {
            return first.equals(key.first);
        }
        return equalSegments(key);
    }

    private boolean equalSegments(WireRowKey other) {
        int leftIndex = 0;
        int rightIndex = 0;
        int leftOffset = 0;
        int rightOffset = 0;
        int compared = 0;
        while (compared < bytes.size()) {
            var left = segments == null ? Objects.requireNonNull(first) : segments.get(leftIndex);
            var right = other.segments == null ? Objects.requireNonNull(other.first) : other.segments.get(rightIndex);
            int length = Math.min(left.remaining() - leftOffset, right.remaining() - rightOffset);
            if (!left.slice(left.position() + leftOffset, length)
                    .equals(right.slice(right.position() + rightOffset, length))) {
                return false;
            }
            compared += length;
            leftOffset += length;
            rightOffset += length;
            if (leftOffset == left.remaining()) {
                leftIndex++;
                leftOffset = 0;
            }
            if (rightOffset == right.remaining()) {
                rightIndex++;
                rightOffset = 0;
            }
        }
        return true;
    }
}
