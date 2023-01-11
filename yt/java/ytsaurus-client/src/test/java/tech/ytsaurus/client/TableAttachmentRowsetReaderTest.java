package tech.ytsaurus.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableAttachmentRowsetReaderTest {
    @Test
    public void testParse() throws Exception {
        byte[] descriptor = {8, 1, 16, 1, 26, 3, 10, 1, 97, 26, 3, 10, 1, 98, 26, 3, 10, 1, 99};
        byte[] rowset = {1, 2, 3, 4, 5};
        byte[] stats = {8, 2, 18, 18, 8, 44, 16, 69, 24, 2, 32, 1, 48, 0, 56, 0, 64, 36, 72, 0, 80, 0};
        byte[] data = concatenate(
                intToBytes(2), // two parts
                //part 1
                longToBytes(4 + 8 + descriptor.length + 8 + rowset.length), // rowDataSize
                intToBytes(2), // two parts,
                //part 1.1
                longToBytes(descriptor.length),
                descriptor,
                //part 1.2
                longToBytes(rowset.length), // <--- parseMergedRow size arg
                // <--- parseMergedRow starts
                rowset,
                //part 2
                longToBytes(stats.length),
                stats
        );
        int expectedPosition = 4 + 8 + 4 + 8 + descriptor.length + 8;
        int expectedSize = rowset.length;
        TableAttachmentTestReader reader = new TableAttachmentTestReader(expectedPosition, expectedSize);
        reader.parse(data);
    }

    private static class TableAttachmentTestReader extends TableAttachmentRowsetReader<Boolean> {
        private final int expectedPosition;
        private final int expectedSize;

        TableAttachmentTestReader(int expectedPosition, int expectedSize) {
            this.expectedPosition = expectedPosition;
            this.expectedSize = expectedSize;
        }

        @Override
        protected List<Boolean> parseMergedRow(ByteBuffer bb, int size) {
            assertEquals(bb.position(), expectedPosition);
            assertEquals(size, expectedSize);
            bb.position(bb.position() + size);
            return null;
        }
    }

    private byte[] intToBytes(int x) {
        ByteBuffer b = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(x);
        return b.array();
    }

    private byte[] longToBytes(long x) {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(x);
        return b.array();
    }

    private byte[] concatenate(byte[]... arrays) {
        int length = 0;
        for (byte[] a : arrays) {
            length += a.length;
        }
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, result, offset, a.length);
            offset += a.length;
        }
        return result;
    }
}
