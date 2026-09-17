package tech.ytsaurus.client;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import NYT.NChunkClient.NProto.DataStatistics;
import org.junit.Test;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;
import tech.ytsaurus.rpcproxy.TRowsetStatistics;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TableAttachmentByteBufferReaderTest {
    @Test
    public void preservesPayloadIncludingArrowPageMarkers() throws Exception {
        byte[] payload = {1, 2, 3, -1, -1, -1, -1, 0, 0, 0, 0, 4, 5, 6};
        for (boolean partition : new boolean[]{false, true}) {
            var reader = reader(partition);
            ByteBuffer result = reader.parse(attachment(payload, partition)).get(0);
            byte[] actual = new byte[result.remaining()];
            result.get(actual);
            assertArrayEquals(payload, actual);
            assertFalse(reader.isEndOfStream());
        }
    }

    @Test
    public void rejectsEmptyRpcBlockWithoutEndingStream() {
        for (boolean partition : new boolean[]{false, true}) {
            var reader = reader(partition);
            assertThrows(BufferUnderflowException.class, () -> reader.parse(new byte[0]));
            assertFalse(reader.isEndOfStream());
        }
    }

    @Test
    public void completesStreamOnceAtRpcEof() {
        for (boolean partition : new boolean[]{false, true}) {
            var reader = reader(partition);
            assertArrayEquals(new byte[8], bytes(reader.endOfStream().get(0)));
            assertTrue(reader.isEndOfStream());
            assertNull(reader.endOfStream());
        }
    }

    @Test
    public void missingAttachmentDoesNotEndStream() throws Exception {
        for (boolean partition : new boolean[]{false, true}) {
            var reader = reader(partition);
            assertNull(reader.parse(null));
            assertFalse(reader.isEndOfStream());
            assertArrayEquals(new byte[]{1}, bytes(reader.parse(attachment(new byte[]{1}, partition)).get(0)));
        }
    }

    @Test
    public void emptyPayloadIsNotRpcEndOfStream() throws Exception {
        for (boolean partition : new boolean[]{false, true}) {
            var reader = reader(partition);
            assertFalse(reader.parse(attachment(new byte[0], partition)).get(0).hasRemaining());
            assertFalse(reader.isEndOfStream());
        }
    }

    private static TableAttachmentByteBufferReader reader(boolean partition) {
        return partition ? new TableAttachmentByteBufferPartitionReader() : new TableAttachmentByteBufferReader();
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private static byte[] attachment(byte[] payload, boolean partition) {
        byte[] descriptor = TRowsetDescriptor.newBuilder()
                .setWireFormatVersion(1)
                .setRowsetFormat(ERowsetFormat.RF_FORMAT)
                .build().toByteArray();
        byte[] rows = pack(descriptor, payload);
        return partition ? rows : pack(rows, TRowsetStatistics.newBuilder()
                .setTotalRowCount(100)
                .setDataStatistics(DataStatistics.TDataStatistics.getDefaultInstance())
                .build().toByteArray());
    }

    private static byte[] pack(byte[] first, byte[] second) {
        return ByteBuffer.allocate(Integer.BYTES + 2 * Long.BYTES + first.length + second.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(2).putLong(first.length).put(first).putLong(second.length).put(second).array();
    }
}
