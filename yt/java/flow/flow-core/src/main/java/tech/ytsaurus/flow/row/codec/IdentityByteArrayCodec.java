package tech.ytsaurus.flow.row.codec;

/**
 * Identity {@link ByteArrayCodec} for raw byte-array state values: encode and decode return the
 * input array by reference, without copying.
 *
 * <p>The codec is stateless; use {@link #INSTANCE} to avoid unnecessary allocations.
 * Callers must not mutate arrays passed in or returned while the state is still reachable.
 */
public final class IdentityByteArrayCodec implements ByteArrayCodec<byte[]> {

    /**
     * Shared stateless instance.
     */
    public static final IdentityByteArrayCodec INSTANCE = new IdentityByteArrayCodec();

    private IdentityByteArrayCodec() {
    }

    /**
     * Returns the input byte array unchanged.
     *
     * @param bytes the wire byte array
     * @return the same array reference
     */
    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }

    /**
     * Returns the input byte array unchanged.
     *
     * @param value the domain byte array
     * @return the same array reference
     */
    @Override
    public byte[] encode(byte[] value) {
        return value;
    }
}
