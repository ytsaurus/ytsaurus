package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;

/**
 * Default {@link InternalStateValueCodec} that performs a straightforward, lossless conversion
 * between {@link ByteString} and {@code byte[]} via {@link ByteString#copyFrom(byte[])} /
 * {@link ByteString#toByteArray()}.
 *
 * <p>The codec is stateless; use {@link #INSTANCE} to avoid unnecessary allocations.
 */
public final class IdentityInternalStateValueCodec implements InternalStateValueCodec {

    /**
     * Shared stateless instance.
     */
    public static final IdentityInternalStateValueCodec INSTANCE = new IdentityInternalStateValueCodec();

    private IdentityInternalStateValueCodec() {
    }

    /**
     * Decodes the wire byte string into a fresh byte array.
     *
     * @param bytes the wire byte string
     * @return the decoded byte array
     */
    @Override
    public byte[] decode(ByteString bytes) {
        return bytes.toByteArray();
    }

    /**
     * Encodes the byte array as a wire byte string.
     *
     * @param value the domain byte array
     * @return the encoded byte string
     */
    @Override
    public ByteString encode(byte[] value) {
        return ByteString.copyFrom(value);
    }
}
