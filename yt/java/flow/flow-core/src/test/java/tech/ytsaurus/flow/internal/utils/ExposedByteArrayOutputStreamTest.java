package tech.ytsaurus.flow.internal.utils;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExposedByteArrayOutputStreamTest {

    @Test
    void exposesBufferWithoutCopyWhenWithinCapacity() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(8);
        byte[] before = stream.buffer();

        stream.write(new byte[]{1, 2, 3}, 0, 3);

        assertSame(before, stream.buffer(), "buffer must not be reallocated while data fits");
        assertEquals(3, stream.length());
        assertEquals(8, stream.buffer().length);
    }

    @Test
    void bufferIsReallocatedWhenCapacityExceeded() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(4);
        byte[] initial = stream.buffer();
        assertEquals(4, initial.length);

        stream.write(new byte[]{1, 2, 3, 4, 5}, 0, 5);

        byte[] grown = stream.buffer();
        assertEquals(5, stream.length());
        assertNotSame(grown, initial, "buffer must be reallocated when capacity is exceeded");
        assertTrue(grown.length >= 5, "grown buffer must hold all written bytes");
    }

    @Test
    void staleBufferReferenceIsNotUpdatedByLaterGrowth() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(2);
        stream.write(new byte[]{1, 2}, 0, 2);
        byte[] stale = stream.buffer();

        stream.write(new byte[]{3, 4, 5, 6}, 0, 4);

        assertNotSame(stale, stream.buffer(), "growth must hand out a fresh buffer");
        assertArrayEquals(new byte[]{1, 2}, Arrays.copyOf(stale, 2),
                "the stale reference still holds only the pre-growth bytes");
    }

    @Test
    void lengthTracksWrittenBytesAcrossGrowth() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(1);
        assertEquals(0, stream.length());

        for (int i = 0; i < 100; i++) {
            stream.write(i);
        }

        assertEquals(100, stream.length());
        assertTrue(stream.buffer().length >= 100);
    }

    @Test
    void writtenBytesArePreservedAfterMultipleReallocations() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(1);
        byte[] expected = new byte[64];
        for (int i = 0; i < expected.length; i++) {
            expected[i] = (byte) i;
            stream.write(i);
        }

        assertEquals(64, stream.length());
        assertArrayEquals(expected, Arrays.copyOf(stream.buffer(), stream.length()),
                "content must survive the doubling reallocations that grew the buffer");
    }

    @Test
    void doublingGrowthLeavesSpareCapacity() {
        ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream(8);

        // Overflow capacity 8 by a single byte: ByteArrayOutputStream doubles to 16, well beyond
        // the 9 valid bytes, so the array ends up larger than length().
        stream.write(new byte[9], 0, 9);

        assertEquals(9, stream.length());
        assertEquals(16, stream.buffer().length);
        assertTrue(stream.buffer().length > stream.length(),
                "doubling from 8 leaves spare capacity beyond the valid length");
    }
}
