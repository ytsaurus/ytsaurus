package tech.ytsaurus.flow.internal.row;

import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.ytsaurus.client.rows.UnversionedRow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("checkstyle:LineLength")
public class ProtoWireReaderWriterTest {

    private static final byte[] DATA_PREFIX = new byte[] {
            0x00, //0 - version
            0x01, //1 - number of values in row
            0x00, //0 - id of value
            0x04  //4 - UINT64 (tech.ytsaurus.core.tables.ColumnValueType.UINT64)
    };

    @ParameterizedTest
    @MethodSource("numbers")
    public void testUint64(long value, byte[] bytes) {
        byte[] payload = new byte[DATA_PREFIX.length + bytes.length];
        System.arraycopy(DATA_PREFIX, 0, payload, 0, DATA_PREFIX.length);
        System.arraycopy(bytes, 0, payload, DATA_PREFIX.length, bytes.length);

        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(payload);
        UnversionedRow unversionedRow = reader.readUnversionedRow();
        long deserializedValue = unversionedRow.getValues().get(0).longValue();
        assertEquals(value, deserializedValue);

        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(unversionedRow);
        byte[] reSerializedPayload = writer.getBinary().toByteArray();
        assertArrayEquals(
                payload,
                reSerializedPayload
        );
    }

    @Test
    public void testReadAndWriteUnversionedRow() {
        byte[] data = new byte[] {
                0x00, //version
                0x02, //number of value in row
                0x00, //first value id
                0x10, //tech.ytsaurus.core.tables.ColumnValueType.STRING
                0x04, //4 bytes length of string
                0x66, //f
                0x6c, //l
                0x6f, //0
                0x77, //w
                0x01, //second value id
                0x04, //tech.ytsaurus.core.tables.ColumnValueType.UINT64
                0x01  //value
        };
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(data);
        UnversionedRow unversionedRow = reader.readUnversionedRow();
        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(unversionedRow);
        ByteString byteString = writer.getBinary();
        assertArrayEquals(data, byteString.toByteArray());
    }

    @Test
    public void testDoubleUsesFixed64Encoding() {
        // 1.0 has raw bits 0x3FF0000000000000; doubles are stored as fixed
        // 8 little-endian bytes (the C++ counterpart format), not as varints.
        byte[] data = new byte[] {
                0x00, //version
                0x01, //number of values in row
                0x00, //value id
                0x05, //tech.ytsaurus.core.tables.ColumnValueType.DOUBLE
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xf0, 0x3f //1.0, little-endian
        };
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(data);
        UnversionedRow unversionedRow = reader.readUnversionedRow();
        assertEquals(1.0, unversionedRow.getValues().get(0).doubleValue());

        ProtoWireProtocolWriter writer = new ProtoWireProtocolWriter(unversionedRow);
        assertArrayEquals(data, writer.getBinary().toByteArray());
    }

    public static Stream<Arguments> numbers() {
        return Stream.of(
                Arguments.of(0,               new byte[] {0x00}),
                Arguments.of(1,               new byte[] {0x01}),
                Arguments.of(2,               new byte[] {0x02}),
                Arguments.of(3,               new byte[] {0x03}),
                Arguments.of(4,               new byte[] {0x04}),
                Arguments.of((1L << 7) - 1,   new byte[] {(byte) 0x7f}),
                Arguments.of((1L << 7),       new byte[] {(byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 14) - 1,  new byte[] {(byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 14),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 21) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 21),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 28) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 28),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 35) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 35),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 42) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 42),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 49) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 49),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 56) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 56),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01}),
                Arguments.of((1L << 63) - 1,  new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f}),
                Arguments.of((1L << 63),      new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01})
        );
    }
}
