package tech.ytsaurus.flow.row.codec;

/**
 * Generic bidirectional codec between a domain value of type {@code T} and a wire
 * representation of type {@code W}.
 *
 * @param <T> domain value type
 * @param <W> wire representation type
 */
public interface Codec<T, W> {

    /**
     * Decodes a wire representation into a domain value.
     *
     * @param wire the wire representation to decode
     * @return the decoded domain value
     */
    T decode(W wire);

    /**
     * Encodes a domain value into its wire representation.
     *
     * @param value the domain value to encode
     * @return the encoded wire representation
     */
    W encode(T value);
}
