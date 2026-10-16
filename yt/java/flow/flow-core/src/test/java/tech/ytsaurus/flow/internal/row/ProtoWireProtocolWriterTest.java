package tech.ytsaurus.flow.internal.row;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoWireProtocolWriterTest {

    @Test
    void estimateRowSize() {
        List<UnversionedValue> values = new ArrayList<>();
        // 8 bytes value header (2 * 4 bytes)
        // 8 bytes for value itself
        values.add(new UnversionedValue(0, ColumnValueType.INT64, false, 1L));
        // 8 bytes value header (2 * 4 bytes)
        // 4 bytes for value length
        // 5 bytes for value itself ("hello".getBytes(StandardCharsets.UTF_8).length)
        values.add(new UnversionedValue(1, ColumnValueType.STRING, false, "hello".getBytes(StandardCharsets.UTF_8)));
        // 8 bytes value header (2 * 4 bytes)
        // 8 bytes for value itself
        values.add(new UnversionedValue(2, ColumnValueType.DOUBLE, false, 2d));
        // 8 bytes value header (2 * 4 bytes)
        // 1 byte for value itself
        values.add(new UnversionedValue(3, ColumnValueType.BOOLEAN, false, false));
        // 8 bytes row header (2 * 4 bytes)
        UnversionedRow row = new UnversionedRow(values);
        assertEquals(
                66, // sum of all above
                ProtoWireProtocolWriter.estimateRowSize(row)
        );
    }

    @Test
    void getBinary() {
        List<UnversionedValue> values = new ArrayList<>();
        values.add(new UnversionedValue(0, ColumnValueType.INT64, false, 1L));
        values.add(new UnversionedValue(1, ColumnValueType.STRING, false, "hello".getBytes(StandardCharsets.UTF_8)));
        UnversionedRow row = new UnversionedRow(values);
        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        ByteString byteString = writer.getBinary();
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(byteString.toByteArray());
        UnversionedRow row2 = reader.readUnversionedRow();
        assertEquals(row, row2);
    }

    @Test
    void compositeRoundTrip() {
        byte[] compositePayload = "[1;2;3]".getBytes(StandardCharsets.UTF_8);
        List<UnversionedValue> values = new ArrayList<>();
        values.add(new UnversionedValue(0, ColumnValueType.INT64, false, 42L));
        values.add(new UnversionedValue(1, ColumnValueType.COMPOSITE, false, compositePayload));
        UnversionedRow row = new UnversionedRow(values);

        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        byte[] bytes = writer.getBinary().toByteArray();

        // Header (version+count) + INT64 value (header + 8 bytes payload) +
        // COMPOSITE header (id + type) + length varint + payload bytes.
        // Lower bound check: we must have written more than just the COMPOSITE header
        // (this guards against the previous bug where payload was silently dropped).
        int int64Section = 1 /* id */ + 1 /* type */ + 1 /* zig-zag varint for 42 */;
        int compositeHeader = 1 /* id */ + 1 /* type */;
        int rowHeader = 1 /* version */ + 1 /* row size */;
        int minExpected = rowHeader + int64Section + compositeHeader + 1 /* len varint */ + compositePayload.length;
        assertEquals(minExpected, bytes.length, "COMPOSITE payload was not written to the wire");

        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(bytes);
        UnversionedRow read = reader.readUnversionedRow();

        assertEquals(2, read.getValues().size());
        UnversionedValue compositeValue = read.getValues().get(1);
        assertEquals(ColumnValueType.COMPOSITE, compositeValue.getType());
        assertArrayEquals(compositePayload, compositeValue.bytesValue());
        assertEquals(row, read);
    }

    @ParameterizedTest(name = "INT64 round-trip: {0}")
    @ValueSource(longs = {0L, 1L, -1L, 42L, -42L, Long.MAX_VALUE, Long.MIN_VALUE})
    void int64RoundTrip(long value) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.INT64, false, value);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.INT64, read.getType());
        assertEquals(value, read.longValue());
    }

    @ParameterizedTest(name = "UINT64 round-trip: {0}")
    @ValueSource(longs = {0L, 1L, 42L, Long.MAX_VALUE, Long.MIN_VALUE /* unsigned 2^63 */, -1L /* unsigned 2^64-1 */})
    void uint64RoundTrip(long value) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.UINT64, false, value);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.UINT64, read.getType());
        // longValue() may not be available for UINT64 raw representation;
        // toRawBits() round-trips reliably for the bit pattern.
        assertEquals(value, read.toRawBits());
    }

    @ParameterizedTest(name = "DOUBLE round-trip: {0}")
    @MethodSource("doubleValues")
    void doubleRoundTrip(double value) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.DOUBLE, false, value);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.DOUBLE, read.getType());
        // Compare raw bits to avoid NaN!=NaN pitfalls.
        assertEquals(Double.doubleToRawLongBits(value), read.toRawBits());
    }

    static Stream<Arguments> doubleValues() {
        return Stream.of(
                Arguments.of(0.0),
                Arguments.of(-0.0),
                Arguments.of(1.0),
                Arguments.of(-1.0),
                Arguments.of(Math.PI),
                Arguments.of(Double.MIN_VALUE),
                Arguments.of(Double.MAX_VALUE),
                Arguments.of(Double.MIN_NORMAL),
                Arguments.of(Double.POSITIVE_INFINITY),
                Arguments.of(Double.NEGATIVE_INFINITY),
                Arguments.of(Double.NaN)
        );
    }

    @ParameterizedTest(name = "BOOLEAN round-trip: {0}")
    @ValueSource(booleans = {true, false})
    void booleanRoundTrip(boolean value) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.BOOLEAN, false, value);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.BOOLEAN, read.getType());
        assertEquals(value, read.booleanValue());
    }

    @ParameterizedTest(name = "STRING round-trip: \"{0}\"")
    @MethodSource("stringPayloads")
    void stringRoundTrip(String label, byte[] payload) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.STRING, false, payload);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.STRING, read.getType());
        assertArrayEquals(payload, read.bytesValue());
    }

    @ParameterizedTest(name = "ANY round-trip: \"{0}\"")
    @MethodSource("stringPayloads")
    void anyRoundTrip(String label, byte[] payload) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.ANY, false, payload);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.ANY, read.getType());
        assertArrayEquals(payload, read.bytesValue());
    }

    @ParameterizedTest(name = "COMPOSITE round-trip: \"{0}\"")
    @MethodSource("stringPayloads")
    void compositeRoundTripParametrized(String label, byte[] payload) {
        UnversionedValue v = new UnversionedValue(0, ColumnValueType.COMPOSITE, false, payload);
        UnversionedValue read = roundTripSingleValue(v);
        assertEquals(ColumnValueType.COMPOSITE, read.getType());
        assertArrayEquals(payload, read.bytesValue());
    }

    static Stream<Arguments> stringPayloads() {
        return Stream.of(
                Arguments.of("empty", new byte[0]),
                Arguments.of("ascii", "hello".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("utf8-cyrillic", "Привет, мир".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("utf8-emoji", "\uD83D\uDE80\uD83D\uDC4B".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("yson-list", "[1;2;3]".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("zero-bytes", new byte[]{0, 0, 0, 0}),
                Arguments.of("binary-0xff", new byte[]{(byte) 0xff, 0x00, (byte) 0xff, 0x7f, (byte) 0x80})
        );
    }

    /**
     * NULL/MIN/MAX/THE_BOTTOM are sentinel types: the writer emits only the value
     * header (id + type) without payload, and the reader signals {@code onEntity()}.
     * The round-tripped value should preserve the type and have no body.
     */
    @ParameterizedTest(name = "Sentinel round-trip: {0}")
    @MethodSource("sentinelTypes")
    void sentinelRoundTrip(ColumnValueType type) {
        UnversionedValue v = new UnversionedValue(0, type, false, null);
        UnversionedRow row = new UnversionedRow(List.of(v));

        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        byte[] bytes = writer.getBinary().toByteArray();

        // Exactly: row-header (version + count) + value-header (id + type) — no body.
        int expectedSize = 1 /* version */ + 1 /* row size */ + 1 /* id */ + 1 /* type */;
        assertEquals(expectedSize, bytes.length,
                "sentinel " + type + " must be encoded as header only");

        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(bytes);
        UnversionedRow read = reader.readUnversionedRow();
        assertEquals(1, read.getValues().size());
        UnversionedValue readValue = read.getValues().get(0);
        assertEquals(type, readValue.getType());
        // Sentinel types don't carry a payload.
        assertNull(readValue.getValue());
    }

    static Stream<Arguments> sentinelTypes() {
        return Stream.of(
                Arguments.of(ColumnValueType.NULL),
                Arguments.of(ColumnValueType.MIN),
                Arguments.of(ColumnValueType.MAX),
                Arguments.of(ColumnValueType.THE_BOTTOM)
        );
    }

    @Test
    void mixedRowRoundTrip() {
        byte[] strBytes = "str".getBytes(StandardCharsets.UTF_8);
        byte[] anyBytes = "{a=1}".getBytes(StandardCharsets.UTF_8);
        byte[] compositeBytes = "[1;2]".getBytes(StandardCharsets.UTF_8);

        List<UnversionedValue> values = new ArrayList<>();
        values.add(new UnversionedValue(0, ColumnValueType.NULL, false, null));
        values.add(new UnversionedValue(1, ColumnValueType.INT64, false, -123L));
        values.add(new UnversionedValue(2, ColumnValueType.UINT64, false, Long.MIN_VALUE /* 2^63 unsigned */));
        values.add(new UnversionedValue(3, ColumnValueType.DOUBLE, false, Math.PI));
        values.add(new UnversionedValue(4, ColumnValueType.BOOLEAN, false, true));
        values.add(new UnversionedValue(5, ColumnValueType.STRING, false, strBytes));
        values.add(new UnversionedValue(6, ColumnValueType.ANY, false, anyBytes));
        values.add(new UnversionedValue(7, ColumnValueType.COMPOSITE, false, compositeBytes));
        UnversionedRow row = new UnversionedRow(values);

        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        byte[] bytes = writer.getBinary().toByteArray();

        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(bytes);
        UnversionedRow read = reader.readUnversionedRow();

        assertEquals(values.size(), read.getValues().size());

        UnversionedValue rNull = read.getValues().get(0);
        assertEquals(ColumnValueType.NULL, rNull.getType());

        UnversionedValue rInt = read.getValues().get(1);
        assertEquals(ColumnValueType.INT64, rInt.getType());
        assertEquals(-123L, rInt.longValue());

        UnversionedValue rUint = read.getValues().get(2);
        assertEquals(ColumnValueType.UINT64, rUint.getType());
        assertEquals(Long.MIN_VALUE, rUint.toRawBits());

        UnversionedValue rDouble = read.getValues().get(3);
        assertEquals(ColumnValueType.DOUBLE, rDouble.getType());
        assertEquals(Double.doubleToRawLongBits(Math.PI), rDouble.toRawBits());

        UnversionedValue rBool = read.getValues().get(4);
        assertEquals(ColumnValueType.BOOLEAN, rBool.getType());
        assertTrue(rBool.booleanValue());

        UnversionedValue rStr = read.getValues().get(5);
        assertEquals(ColumnValueType.STRING, rStr.getType());
        assertArrayEquals(strBytes, rStr.bytesValue());

        UnversionedValue rAny = read.getValues().get(6);
        assertEquals(ColumnValueType.ANY, rAny.getType());
        assertArrayEquals(anyBytes, rAny.bytesValue());

        UnversionedValue rComp = read.getValues().get(7);
        assertEquals(ColumnValueType.COMPOSITE, rComp.getType());
        assertArrayEquals(compositeBytes, rComp.bytesValue());

        // And full equality at the row level.
        assertEquals(row, read);
    }

    @ParameterizedTest(name = "reject >16MB output: {0}")
    @EnumSource(value = ColumnValueType.class, names = {"STRING", "ANY", "COMPOSITE"})
    void rejectsStringLikeValueLargerThan16Mb(ColumnValueType type) {
        byte[] payload = new byte[WireProtocol.MAX_STRING_VALUE_LENGTH + 1];
        UnversionedRow row = new UnversionedRow(List.of(new UnversionedValue(0, type, false, payload)));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> new ProtoWireProtocolWriter(row));
        assertTrue(ex.getMessage().contains(type.toString()));
    }

    @ParameterizedTest(name = "allow exactly 16MB output: {0}")
    @EnumSource(value = ColumnValueType.class, names = {"STRING", "ANY", "COMPOSITE"})
    void acceptsStringLikeValueOfExactly16Mb(ColumnValueType type) {
        byte[] payload = new byte[WireProtocol.MAX_STRING_VALUE_LENGTH];
        UnversionedRow row = new UnversionedRow(List.of(new UnversionedValue(0, type, false, payload)));
        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        assertTrue(writer.getBinary().size() >= WireProtocol.MAX_STRING_VALUE_LENGTH);
    }

    private static UnversionedValue roundTripSingleValue(UnversionedValue value) {
        UnversionedRow row = new UnversionedRow(List.of(value));
        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(row);
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(writer.getBinary().toByteArray());
        UnversionedRow read = reader.readUnversionedRow();
        assertEquals(1, read.getValues().size());
        return read.getValues().get(0);
    }
}
