package tech.ytsaurus.flow.internal.row;

import java.io.IOException;
import java.util.Arrays;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnValueType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtoWireProtocolReaderTest {

    @Test
    public void readUnversionedRowFromByteArray() {
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(new byte[]{
                0x00, //version
                0x01, //number of values in row
                0x00, //value id
                0x10, //tech.ytsaurus.core.tables.ColumnValueType.STRING
                0x02, //2 bytes length of string
                0x74, //t
                0x70  //p
        });

        UnversionedRow unversionedRow = reader.readUnversionedRow();
        assertEquals(1, unversionedRow.getValues().size());
        assertEquals("tp", unversionedRow.getValues().get(0).stringValue());
    }

    @Test
    public void rejectsValueLengthExceedingIntRange() {
        // STRING value whose length field is 2^31 (> Integer.MAX_VALUE): must be rejected rather than
        // silently truncated by the (int) cast into a negative array length.
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(new byte[]{
                0x00, // version
                0x01, // number of values in row
                0x00, // value id
                0x10, // ColumnValueType.STRING
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x08 // length varint = 2^31
        });

        assertThrows(IllegalStateException.class, reader::readUnversionedRow);
    }

    @Test
    public void readMultiValueUnversionedRowFromByteArray() {
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(new byte[]{
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
        });

        UnversionedRow unversionedRow = reader.readUnversionedRow();
        assertEquals(2, unversionedRow.getValues().size());
        assertEquals("flow", unversionedRow.getValues().get(0).stringValue());
        assertEquals(1, unversionedRow.getValues().get(1).longValue());
    }

    @Test
    public void readSingleInt64() {
        byte[] data = new byte[] {
                0x00, 0x01, 0x00, 0x03, 0x06
        };
        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(data);
        UnversionedRow unversionedRow = reader.readUnversionedRow();
        assertEquals(1, unversionedRow.getValues().size());
        assertEquals(3, unversionedRow.getValues().get(0).longValue());
    }

    /**
     * Input is intentionally unbounded (beyond the JVM 2GB array limit): the reader must accept
     * STRING/ANY/COMPOSITE payloads larger than the 16MB output limit imposed by the writers.
     */
    @ParameterizedTest
    @EnumSource(value = ColumnValueType.class, names = {"STRING", "ANY", "COMPOSITE"})
    public void readsStringLikeValueLargerThan16Mb(ColumnValueType type) throws IOException {
        int length = WireProtocol.MAX_STRING_VALUE_LENGTH + 1;
        byte[] payload = new byte[length];
        payload[0] = 0x7f;
        payload[length - 1] = (byte) 0xab;

        ProtoWireProtocolReader reader = new ProtoWireProtocolReader(buildSingleStringLikeRow(type, payload));
        UnversionedRow row = reader.readUnversionedRow();

        assertEquals(1, row.getValues().size());
        UnversionedValue value = row.getValues().getFirst();
        assertEquals(type, value.getType());
        assertArrayEquals(payload, value.bytesValue());
    }

    private static byte[] buildSingleStringLikeRow(ColumnValueType type, byte[] payload) throws IOException {
        byte[] buffer = new byte[payload.length + 16];
        CodedOutputStream out = CodedOutputStream.newInstance(buffer);
        out.writeUInt32NoTag(0);                            // version
        out.writeUInt32NoTag(1);                            // value count
        out.writeUInt32NoTag(0);                            // value id
        out.writeRawByte((byte) (type.getValue() & 0xFF));  // value type
        out.writeUInt64NoTag(payload.length);               // value length
        out.writeRawBytes(payload);                         // value payload
        out.flush();
        return Arrays.copyOf(buffer, out.getTotalBytesWritten());
    }
}
