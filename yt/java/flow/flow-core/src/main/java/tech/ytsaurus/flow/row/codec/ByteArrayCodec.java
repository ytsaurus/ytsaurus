package tech.ytsaurus.flow.row.codec;

/**
 * Bidirectional codec between a domain value of type {@code T} and a raw {@code byte[]} wire
 * representation.
 *
 * @param <T> domain value type
 */
public interface ByteArrayCodec<T> extends Codec<T, byte[]> {
}
