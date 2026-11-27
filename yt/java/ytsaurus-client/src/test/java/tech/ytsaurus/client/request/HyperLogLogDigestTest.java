package tech.ytsaurus.client.request;

import java.util.Arrays;

import com.google.protobuf.ByteString;
import org.junit.Test;
import tech.ytsaurus.THyperLogLogDigest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class HyperLogLogDigestTest {
    @Test
    public void estimatesMatchYtForDifferentRegisterDistributions() {
        assertEquals(0, digest(0).estimateCardinality());
        assertEquals(18, digest(1).estimateCardinality());
        assertEquals(86, digest(3).estimateCardinality());

        byte[] partial = new byte[16];
        Arrays.fill(partial, 0, 7, (byte) 1);
        assertEquals(9, digest(partial).estimateCardinality());
        Arrays.fill(partial, 0, 8, (byte) 1);
        assertEquals(7, digest(partial).estimateCardinality());
        assertEquals(46, digest(new byte[] {
                1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4
        }).estimateCardinality());
    }

    @Test
    public void mergesRegistersWithoutMutatingEitherInput() {
        byte[] left = new byte[16];
        byte[] right = new byte[16];
        left[0] = 1;
        left[1] = 3;
        right[1] = 2;
        right[2] = 4;
        HyperLogLogDigest first = digest(left);
        HyperLogLogDigest second = digest(right);

        HyperLogLogDigest merged = first.merge(second);
        byte[] expected = left.clone();
        expected[2] = 4;
        assertArrayEquals(expected, merged.getData());
        assertArrayEquals(expected, second.merge(first).getData());
        assertArrayEquals(left, first.getData());
        assertArrayEquals(right, second.getData());
    }

    @Test
    public void rejectsEstimatesOutsideSignedLongRange() {
        assertThrows(ArithmeticException.class, () -> digest(60).estimateCardinality());
    }

    private static HyperLogLogDigest digest(int value) {
        byte[] registers = new byte[16];
        Arrays.fill(registers, (byte) value);
        return digest(registers);
    }

    private static HyperLogLogDigest digest(byte[] registers) {
        return new HyperLogLogDigest(THyperLogLogDigest.newBuilder().setData(ByteString.copyFrom(registers)).build());
    }
}
