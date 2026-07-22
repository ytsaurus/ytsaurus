package tech.ytsaurus.flow.row.codec;

import javax.persistence.Entity;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.TypeAwareProtoWireProtocolWriter;
import tech.ytsaurus.flow.typeinfo.TypeInfo;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link TypeAwareProtoWireTypedPayloadCodec} is byte-for-byte compatible with
 * the legacy direct use of {@link TypeAwareProtoWireProtocolReader} /
 * {@link TypeAwareProtoWireProtocolWriter} that lived inside
 * the typed flow stream implementation before the introduction of the codec SPI.
 */
class TypeAwareProtoWireTypedPayloadCodecTest {

    @Entity
    static class Sample {
        String name;
        Long count;
    }

    @Test
    void encodeMatchesLegacyWriter() {
        var sample = new Sample();
        sample.name = "flow";
        sample.count = 42L;

        var typeInfo = new TypeInfo<>(Sample.class);
        ByteString encoded = TypeAwareProtoWireTypedPayloadCodec.INSTANCE.codecFor(typeInfo).encode(sample);
        ByteString expected = new TypeAwareProtoWireProtocolWriter<>(typeInfo).writeEntity(sample);

        assertArrayEquals(expected.toByteArray(), encoded.toByteArray());
    }

    @Test
    void decodeMatchesLegacyReader() {
        var sample = new Sample();
        sample.name = "flow";
        sample.count = 42L;

        var typeInfo = new TypeInfo<>(Sample.class);
        ByteString bytes = new TypeAwareProtoWireProtocolWriter<>(typeInfo).writeEntity(sample);

        Sample decoded = TypeAwareProtoWireTypedPayloadCodec.INSTANCE.codecFor(typeInfo).decode(bytes);
        Sample viaLegacy = new TypeAwareProtoWireProtocolReader<>(typeInfo).readChunk(bytes);

        assertEquals(viaLegacy.name, decoded.name);
        assertEquals(viaLegacy.count, decoded.count);
    }

    @Test
    void roundTripPreservesBinaryRepresentation() {
        var sample = new Sample();
        sample.name = "abc";
        sample.count = 7L;

        var codec = TypeAwareProtoWireTypedPayloadCodec.INSTANCE.codecFor(new TypeInfo<>(Sample.class));

        ByteString encoded = codec.encode(sample);
        Sample decoded = codec.decode(encoded);
        ByteString reEncoded = codec.encode(decoded);

        assertArrayEquals(encoded.toByteArray(), reEncoded.toByteArray());
        assertEquals(sample.name, decoded.name);
        assertEquals(sample.count, decoded.count);
    }
}
