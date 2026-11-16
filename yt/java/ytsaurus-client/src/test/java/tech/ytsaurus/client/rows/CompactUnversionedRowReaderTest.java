package tech.ytsaurus.client.rows;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.Test;
import tech.ytsaurus.core.tables.ColumnValueType;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class CompactUnversionedRowReaderTest {
    private static final int FORMAT_VERSION = 0;
    // One INT64 value at id 0; ZigZag encodes -5 as 9.
    private static final byte[] NEGATIVE_INT64_ROW = bytes(FORMAT_VERSION, 1, 0, ColumnValueType.INT64.getValue(), 9);

    @Test
    public void emptyRows() {
        assertNull(new CompactUnversionedRowReader(ByteString.EMPTY).readUnversionedRow());
        assertEquals(List.of(), decode(FORMAT_VERSION, 0));
    }

    @Test
    public void byteArrayInput() {
        UnversionedRow row = new CompactUnversionedRowReader(NEGATIVE_INT64_ROW)
                .readUnversionedRow();
        assertEquals(-5L, row.getValues().get(0).longValue());
    }

    @Test
    public void customRowDeserializer() {
        UnversionedValueDeserializer valueDeserializer = new UnversionedValueDeserializer();
        valueDeserializer.setAggregate(true);
        WireRowDeserializer<Long> deserializer = new WireRowDeserializer<>() {
            @Override
            public WireValueDeserializer<?> onNewRow(int columnCount) {
                assertEquals(1, columnCount);
                return valueDeserializer;
            }

            @Override
            public Long onCompleteRow() {
                UnversionedValue value = valueDeserializer.build();
                assertFalse(value.isAggregate());
                return value.longValue();
            }

            @Override
            public Long onNullRow() {
                return -1L;
            }
        };

        assertEquals(Long.valueOf(-5), new CompactUnversionedRowReader(NEGATIVE_INT64_ROW)
                .readUnversionedRow(deserializer));
        assertEquals(Long.valueOf(-1), new CompactUnversionedRowReader(new byte[0])
                .readUnversionedRow(deserializer));
    }

    @Test
    public void scalarValuesKeepTypesIdsAndOrder() {
        // C++ compact format: version, count, then (id, type, payload) for each value.
        List<UnversionedValue> values = decode(
                FORMAT_VERSION, 6,
                // id 300 = varint [ac 02]; ZigZag(-1) = 1.
                0xac, 0x02, ColumnValueType.INT64.getValue(), 1,
                // UInt64.MAX_VALUE = 2^64 - 1: nine continuation bytes and a final 1.
                1, ColumnValueType.UINT64.getValue(),
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1,
                // IEEE 754 double 1.0 = 0x3ff0000000000000, written little-endian.
                2, ColumnValueType.DOUBLE.getValue(), 0, 0, 0, 0, 0, 0, 0xf0, 0x3f,
                3, ColumnValueType.BOOLEAN.getValue(), 1,
                4, ColumnValueType.BOOLEAN.getValue(), 0,
                5, ColumnValueType.STRING.getValue(), 2, 'o', 'k');

        assertEquals(List.of(
                new UnversionedValue(300, ColumnValueType.INT64, false, -1L),
                new UnversionedValue(1, ColumnValueType.UINT64, false, -1L),
                new UnversionedValue(2, ColumnValueType.DOUBLE, false, 1.0),
                new UnversionedValue(3, ColumnValueType.BOOLEAN, false, true),
                new UnversionedValue(4, ColumnValueType.BOOLEAN, false, false),
                new UnversionedValue(5, ColumnValueType.STRING, false, "ok".getBytes(StandardCharsets.UTF_8))
        ), values);
        assertThrows(UnsupportedOperationException.class, values::clear);
    }

    @Test
    public void signedIntegerLimits() {
        List<UnversionedValue> values = decode(
                FORMAT_VERSION, 2,
                // ZigZag(Long.MIN_VALUE) = unsigned 2^64 - 1.
                0, ColumnValueType.INT64.getValue(),
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1,
                // ZigZag(Long.MAX_VALUE) = unsigned 2^64 - 2.
                1, ColumnValueType.INT64.getValue(),
                0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1);

        assertEquals(Long.MIN_VALUE, values.get(0).longValue());
        assertEquals(Long.MAX_VALUE, values.get(1).longValue());
    }

    @Test
    public void stringLikeValuesKeepRawBytes() {
        List<UnversionedValue> values = decode(
                FORMAT_VERSION, 4,
                0, ColumnValueType.STRING.getValue(), 2, 0xc3, 0xa9, // UTF-8 for U+00E9 (e with acute accent)
                1, ColumnValueType.STRING.getValue(), 0,
                2, ColumnValueType.ANY.getValue(), 2, 0xff, 0, // Arbitrary bytes, preserved without parsing.
                3, ColumnValueType.COMPOSITE.getValue(), 3, '[', ']', ';');

        assertEquals("\u00e9", values.get(0).stringValue());
        assertArrayEquals(new byte[0], values.get(1).bytesValue());
        assertEquals(ColumnValueType.ANY, values.get(2).getType());
        assertArrayEquals(new byte[]{(byte) 0xff, 0}, values.get(2).bytesValue());
        assertEquals(ColumnValueType.COMPOSITE, values.get(3).getType());
        assertArrayEquals(new byte[]{'[', ']', ';'}, values.get(3).bytesValue());
    }

    @Test
    public void sentinelValues() {
        assertEquals(List.of(
                new UnversionedValue(0, ColumnValueType.MAX, false, null),
                new UnversionedValue(1, ColumnValueType.NULL, false, null),
                new UnversionedValue(2, ColumnValueType.MIN, false, null),
                new UnversionedValue(3, ColumnValueType.THE_BOTTOM, false, null)
        ), decode(FORMAT_VERSION, 4,
                0, 0xef, 0x01, // MAX = 239 needs two varint bytes; the next value must stay aligned.
                1, ColumnValueType.NULL.getValue(),
                2, ColumnValueType.MIN.getValue(),
                3, ColumnValueType.THE_BOTTOM.getValue()));
    }

    @Test
    public void invalidHeaderOrType() {
        assertThrows(IllegalArgumentException.class, () -> decode(FORMAT_VERSION + 1, 0));
        // Count 2^32 - 1 overflows a signed int.
        assertThrows(IllegalArgumentException.class, () -> decode(FORMAT_VERSION, 0xff, 0xff, 0xff, 0xff, 0x0f));
        // Count Integer.MAX_VALUE is too large for an input with no values.
        assertThrows(IllegalArgumentException.class, () -> decode(FORMAT_VERSION, 0xff, 0xff, 0xff, 0xff, 0x07));
        int unknownType = 7; // No ColumnValueType has this code.
        assertThrows(IllegalArgumentException.class, () -> decode(FORMAT_VERSION, 1, 0, unknownType));
    }

    @Test
    public void truncatedInput() {
        // Two values declared, only one present.
        assertThrows(IllegalArgumentException.class,
                () -> decode(FORMAT_VERSION, 2, 0, ColumnValueType.NULL.getValue()));
        // Five string bytes declared, only two present.
        assertThrows(IllegalArgumentException.class,
                () -> decode(FORMAT_VERSION, 1, 0, ColumnValueType.STRING.getValue(), 5, 'a', 'b'));
        // A double needs eight bytes, only three present.
        assertThrows(IllegalArgumentException.class,
                () -> decode(FORMAT_VERSION, 1, 0, ColumnValueType.DOUBLE.getValue(), 0, 0, 0));
    }

    private static List<UnversionedValue> decode(int... data) {
        return new CompactUnversionedRowReader(ByteString.copyFrom(bytes(data))).readUnversionedRow().getValues();
    }

    private static byte[] bytes(int... data) {
        byte[] result = new byte[data.length];
        for (int i = 0; i < data.length; ++i) {
            result[i] = (byte) data[i];
        }
        return result;
    }
}
