package tech.ytsaurus.client.rpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CodecTest {
    @Parameterized.Parameter(0)
    public Compression compression;

    @Parameterized.Parameter(1)
    public int blockSize;

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Object[] parameters() {
        final Collection<Object[]> params = new ArrayList<>();
        for (Compression compression : Compression.values()) {
            for (int blockSize : new int[]{0, 127, 1024, 32798, 1024 * 1024}) {
                params.add(new Object[]{compression, blockSize});
            }
        }
        return params.toArray();
    }

    @Test
    public void testIncompressible() {
        Random rd = new Random();
        byte[] arr = new byte[blockSize];
        rd.nextBytes(arr);
        testCompression(arr);
    }

    @Test
    public void testCompressible() {
        final byte[] bytes = new byte[blockSize];
        Arrays.fill(bytes, (byte) 'A');
        testCompression(bytes);
    }

    private void testCompression(byte[] bytes) {
        final Codec codec = Codec.codecFor(compression);
        Assert.assertNotNull(codec);
        final byte[] compressed = codec.compress(bytes);
        final byte[] decompressed = codec.decompress(compressed);
        Assert.assertArrayEquals(bytes, decompressed);
    }
}
