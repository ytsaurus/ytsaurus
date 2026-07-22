package tech.ytsaurus.flow.row.codec;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolReader;
import tech.ytsaurus.flow.internal.row.ProtoWireProtocolWriter;
import tech.ytsaurus.flow.row.Payload;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies that {@link ProtoWirePayloadCodec} is byte-for-byte compatible with the legacy
 * direct use of {@link ProtoWireProtocolReader} / {@link ProtoWireProtocolWriter} that lived
 * inside the raw stream implementation and the external-state mapper before the introduction
 * of the codec SPI.
 */
class ProtoWirePayloadCodecTest {

    private static TableSchema schema() {
        return TableSchema.builder()
                .add(new ColumnSchema("id", ColumnValueType.UINT64))
                .add(new ColumnSchema("name", ColumnValueType.STRING))
                .build();
    }

    @Test
    void encodeProducesExpectedBytes() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, 42L),
                new UnversionedValue(1, ColumnValueType.STRING, false, "flow".getBytes(StandardCharsets.UTF_8))
        ));
        var payload = new Payload(row, schema());
        byte[] expected = new byte[] {
                0x00, // version
                0x02, // number of values
                0x00, // first value id
                0x04, // UINT64
                0x2A, // value = 42
                0x01, // second value id
                0x10, // STRING
                0x04, // 4 bytes length
                0x66, 0x6c, 0x6f, 0x77 // "flow"
        };

        ByteString encoded = ProtoWirePayloadCodec.INSTANCE.codecFor(schema()).encode(payload);

        assertArrayEquals(expected, encoded.toByteArray());
    }

    @Test
    void encodeMatchesLegacyWriter() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, 42L),
                new UnversionedValue(1, ColumnValueType.STRING, false, "flow".getBytes(StandardCharsets.UTF_8))
        ));
        var payload = new Payload(row, schema());

        ByteString encoded = ProtoWirePayloadCodec.INSTANCE.codecFor(schema()).encode(payload);
        ByteString expected = new ProtoWireProtocolWriter(row).getBinary();

        assertArrayEquals(expected.toByteArray(), encoded.toByteArray());
    }

    @Test
    void decodeMatchesLegacyReader() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, 42L),
                new UnversionedValue(1, ColumnValueType.STRING, false, "flow".getBytes(StandardCharsets.UTF_8))
        ));
        ByteString bytes = new ProtoWireProtocolWriter(row).getBinary();
        var schema = schema();

        Payload decoded = ProtoWirePayloadCodec.INSTANCE.codecFor(schema).decode(bytes);
        UnversionedRow viaLegacy = new ProtoWireProtocolReader(bytes).readUnversionedRow();

        assertEquals(viaLegacy, decoded.getRow());
        assertSame(schema, decoded.getSchema());
    }

    @Test
    void roundTripPreservesBinaryRepresentation() {
        var row = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.UINT64, false, 7L),
                new UnversionedValue(1, ColumnValueType.STRING, false, "abc".getBytes(StandardCharsets.UTF_8))
        ));
        var payload = new Payload(row, schema());
        var codec = ProtoWirePayloadCodec.INSTANCE.codecFor(payload.getSchema());

        ByteString encoded = codec.encode(payload);
        Payload decoded = codec.decode(encoded);
        ByteString reEncoded = codec.encode(decoded);

        assertArrayEquals(encoded.toByteArray(), reEncoded.toByteArray());
        assertEquals(row, decoded.getRow());
    }
}
