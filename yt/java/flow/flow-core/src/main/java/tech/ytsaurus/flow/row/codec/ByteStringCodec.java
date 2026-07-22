package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;

/**
 * Bidirectional codec between a domain value of type {@code T} and a protobuf
 * {@link ByteString} wire representation.
 *
 * @param <T> domain value type
 */
public interface ByteStringCodec<T> extends Codec<T, ByteString> {
}
