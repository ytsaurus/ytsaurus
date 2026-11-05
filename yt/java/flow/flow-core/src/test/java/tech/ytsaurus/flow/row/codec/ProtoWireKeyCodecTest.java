package tech.ytsaurus.flow.row.codec;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolWriter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link ProtoByteStringKeyCodec} is byte-for-byte compatible with the legacy
 * direct use of {@link ProtoWireProtocolReader} / {@link ProtoWireProtocolWriter}.
 */
class ProtoWireKeyCodecTest {

    @Test
    void encodeProducesExpectedBytes() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.STRING, false, "flow".getBytes(StandardCharsets.UTF_8)),
                new UnversionedValue(1, ColumnValueType.UINT64, false, 1L)
        ));
        byte[] expected = new byte[] {
                0x00, // version
                0x02, // number of values
                0x00, // first value id
                0x10, // STRING
                0x04, // 4 bytes length
                0x66, 0x6c, 0x6f, 0x77, // "flow"
                0x01, // second value id
                0x04, // UINT64
                0x01  // value
        };

        ByteString encoded = ProtoByteStringKeyCodec.INSTANCE.encode(row);

        assertArrayEquals(expected, encoded.toByteArray());
    }

    @Test
    void encodeMatchesLegacyWriter() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, 42L),
                new UnversionedValue(1, ColumnValueType.STRING, false, "flow".getBytes(StandardCharsets.UTF_8))
        ));

        ByteString encoded = ProtoByteStringKeyCodec.INSTANCE.encode(row);
        ByteString expected = new ProtoWireProtocolWriter(row).getBinary();

        assertArrayEquals(expected.toByteArray(), encoded.toByteArray());
    }

    @Test
    void decodeMatchesLegacyReader() {
        byte[] data = new byte[] {
                0x00, // version
                0x02, // number of values
                0x00, // first value id
                0x10, // STRING
                0x04, // 4 bytes length
                0x66, // f
                0x6c, // l
                0x6f, // o
                0x77, // w
                0x01, // second value id
                0x04, // UINT64
                0x01  // value
        };
        UnversionedRow viaCodec = ProtoByteStringKeyCodec.INSTANCE.decode(ByteString.copyFrom(data));
        UnversionedRow viaLegacy = new ProtoWireProtocolReader(data).readUnversionedRow();

        assertEquals(viaLegacy, viaCodec);
    }

    @Test
    void roundTripPreservesBinaryRepresentation() {
        byte[] data = new byte[] {
                0x00, 0x02,
                0x00, 0x10, 0x04, 0x66, 0x6c, 0x6f, 0x77,
                0x01, 0x04, 0x01
        };
        var codec = ProtoByteStringKeyCodec.INSTANCE;

        UnversionedRow decoded = codec.decode(ByteString.copyFrom(data));
        ByteString reEncoded = codec.encode(decoded);

        assertArrayEquals(data, reEncoded.toByteArray());
    }
}
