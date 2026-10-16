package tech.ytsaurus.flow.row.codec;

import tech.ytsaurus.core.rows.YTreeSerializer;

/**
 * Codec that (de)serializes a state value as binary YSON through a supplied {@link YsonCodec}.
 *
 * <p>The set of supported value types is exactly the set the supplied {@link YsonCodec} can resolve
 * a serializer for. The underlying {@link YTreeSerializer} is resolved once, at construction time,
 * and reused for every {@link #encode}/{@link #decode} call.
 *
 * @param <T> state value type
 */
public final class YsonByteArrayCodec<T> implements ByteArrayCodec<T> {

    private final Class<T> stateClass;
    private final YsonCodec ysonCodec;
    private final YTreeSerializer<?> serializer;

    /**
     * Creates a codec for the given state value class backed by the supplied YSON codec, resolving
     * the serializer once.
     *
     * @param stateClass runtime class of the state value type {@code T}
     * @param ysonCodec  YSON codec that resolves the serializer and performs (de)serialization
     */
    public YsonByteArrayCodec(Class<T> stateClass, YsonCodec ysonCodec) {
        this.stateClass = stateClass;
        this.ysonCodec = ysonCodec;
        this.serializer = ysonCodec.getOrCreateSerializer(stateClass);
    }

    /**
     * Serializes a state value into its binary YSON representation.
     *
     * @param value the value to encode
     * @return the binary YSON bytes
     */
    @Override
    public byte[] encode(T value) {
        return ysonCodec.encodeWithSerializer(serializer, value);
    }

    /**
     * Reconstructs a state value from its binary YSON representation.
     *
     * <p>The result is {@code null} only if {@code bytes} encode a YSON entity ({@code #}); the
     * state layer never persists such a value for a set state, so in practice a non-null value is
     * always returned.
     *
     * @param bytes the binary YSON bytes
     * @return the decoded value
     */
    @Override
    @SuppressWarnings("unchecked")
    public T decode(byte[] bytes) {
        return (T) ysonCodec.decodeWithSerializer(serializer, bytes, stateClass);
    }
}
