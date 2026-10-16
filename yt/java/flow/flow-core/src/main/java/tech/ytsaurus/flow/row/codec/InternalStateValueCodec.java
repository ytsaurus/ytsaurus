package tech.ytsaurus.flow.row.codec;

/**
 * Pluggable codec that (de)serializes an opaque internal-state value blob between its
 * {@code byte[]} domain representation and its {@link com.google.protobuf.ByteString} wire form.
 *
 * <p>An internal-state value is treated as an opaque byte blob: the flow runtime does not
 * impose any structural interpretation on it. Implementations must be thread-safe.
 *
 * @see CodecRegistry#getInternalStateValueCodec()
 */
public interface InternalStateValueCodec extends ByteStringCodec<byte[]> {
}
