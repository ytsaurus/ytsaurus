package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link IdentityInternalStateValueCodec} is byte-for-byte compatible with
 * the legacy direct {@link ByteString}/{@code byte[]} conversion used in the state mappers.
 */
class IdentityInternalStateValueCodecTest {

    @Test
    void decodeReturnsRawBytes() {
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        byte[] decoded = IdentityInternalStateValueCodec.INSTANCE.decode(ByteString.copyFrom(data));
        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeReturnsByteStringCopy() {
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        ByteString encoded = IdentityInternalStateValueCodec.INSTANCE.encode(data);
        assertEquals(ByteString.copyFrom(data), encoded);
    }

    @Test
    void roundTripPreservesValue() {
        byte[] data = new byte[] {0x00, 0x7f, (byte) 0x80, (byte) 0xff};
        byte[] result = IdentityInternalStateValueCodec.INSTANCE
                .decode(IdentityInternalStateValueCodec.INSTANCE.encode(data));
        assertArrayEquals(data, result);
    }

    @Test
    void handlesEmptyValue() {
        byte[] empty = new byte[0];
        ByteString encoded = IdentityInternalStateValueCodec.INSTANCE.encode(empty);
        assertEquals(ByteString.EMPTY, encoded);
        assertArrayEquals(empty, IdentityInternalStateValueCodec.INSTANCE.decode(ByteString.EMPTY));
    }
}
