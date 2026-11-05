package tech.ytsaurus.flow.row.codec;

import tech.ytsaurus.flow.typeinfo.TypeInfo;

/**
 * Pluggable factory of type-bound codecs that (de)serialize typed message payloads between
 * their domain form and their wire {@link com.google.protobuf.ByteString} form.
 *
 * <p>Implementations and the codecs they produce must be thread-safe.
 *
 * @see CodecRegistry#getTypedPayloadCodec()
 */
public interface TypedPayloadCodec {

    /**
     * Returns a {@link ByteStringCodec} specialised for the given type.
     *
     * @param typeInfo runtime description of the domain type {@code T}
     * @param <T>      domain value type
     * @return a thread-safe codec bound to {@code typeInfo}
     */
    <T> ByteStringCodec<T> codecFor(TypeInfo<T> typeInfo);
}
