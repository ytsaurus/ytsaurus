package tech.ytsaurus.flow.state;

import java.util.Objects;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * State entry: a reset marker, or a value in one or both of its representations — the wire
 * bytes and the decoded object — each materialized on first access. Internal and external
 * states share this shape.
 */
public final class State implements YTreeConvertible {
    public static final State RESET = new State(true, null);

    private final boolean reset;
    // The entry is request-local and single-threaded; set()/clear() replace the whole entry, so
    // neither representation outlives the other.
    // Wire bytes; absent until first read for a value that came with its codec.
    private @Nullable ByteString bytes;
    // Decoded value: the one handed to set(), or memoized by getValue() on first read.
    private @Nullable Object value;
    // Encodes the value into the bytes: set for an entry created from a value, and for one whose
    // value was handed out as mutable.
    private @Nullable ByteStringCodec<Object> codec;

    public State(ByteString bytes) {
        this(false, Objects.requireNonNull(bytes, "Non-reset state must have bytes"));
    }

    private State(boolean reset, @Nullable ByteString bytes) {
        this.reset = reset;
        this.bytes = bytes;
    }

    /**
     * Creates an entry from both representations — a value and the bytes it was just encoded
     * into — so a read that follows does not decode it again.
     */
    public State(ByteString bytes, Object value) {
        this(bytes);
        this.value = Objects.requireNonNull(value, "Value must not be null");
    }

    /**
     * Creates an entry from a value alone: it is encoded with {@code codec} when the bytes are
     * first read, typically once at the end of the request, however many times it is set.
     */
    @SuppressWarnings("unchecked")
    public <T> State(T value, ByteStringCodec<T> codec) {
        this(false, null);
        this.value = Objects.requireNonNull(value, "Value must not be null");
        this.codec = (ByteStringCodec<Object>) Objects.requireNonNull(codec, "Codec must not be null");
    }

    public boolean isReset() {
        return reset;
    }

    /**
     * Returns the wire bytes, encoding the value on the first call if the entry came without them.
     */
    public @Nullable ByteString getBytes() {
        if (bytes == null && codec != null) {
            bytes = codec.encode(value);
        }
        return bytes;
    }

    /**
     * Returns the value, decoding the bytes with {@code codec} on the first call.
     *
     * @param codec codec of the bytes.
     * @param <T>   value type.
     * @return the value.
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(ByteStringCodec<T> codec) {
        if (value == null) {
            value = codec.decode(Objects.requireNonNull(bytes, "Non-reset state must have bytes"));
        }
        return (T) value;
    }

    /**
     * Returns the value like {@link #getValue} and keeps {@code codec}, so that
     * {@link #syncBytes} re-encodes the value: changes made to the returned object in place
     * reach the wire.
     *
     * @param codec codec of the bytes.
     * @param <T>   value type.
     * @return the value.
     */
    @SuppressWarnings("unchecked")
    <T> T getMutableValue(ByteStringCodec<T> codec) {
        T result = getValue(codec);
        this.codec = (ByteStringCodec<Object>) codec;
        return result;
    }

    /**
     * Re-encodes a mutable value and reports whether the bytes changed. An entry that has no
     * bytes yet is encoded on the first {@link #getBytes} instead.
     */
    boolean syncBytes() {
        if (codec == null || bytes == null) {
            return false;
        }
        ByteString encoded = codec.encode(value);
        if (encoded.equals(bytes)) {
            return false;
        }
        bytes = encoded;
        return true;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        return YTree.builder().beginMap()
                .key("reset").value(reset)
                .endMap().build();
    }
}
