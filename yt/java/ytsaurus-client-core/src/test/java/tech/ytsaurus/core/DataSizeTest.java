package tech.ytsaurus.core;

import org.junit.Assert;
import org.junit.Test;

public class DataSizeTest {
    @Test
    public void testToStringShort() {
        Assert.assertEquals("0", DataSize.ZERO.toStringShort());
        Assert.assertEquals("10", DataSize.fromBytes(10).toStringShort());
        Assert.assertEquals("-10", DataSize.fromBytes(-10).toStringShort());
        Assert.assertEquals("1K", DataSize.fromBytes(1024).toStringShort());
        Assert.assertEquals("-1K", DataSize.fromBytes(-1024).toStringShort());
        Assert.assertEquals("1K", DataSize.fromBytes(1026).toStringShort());
        Assert.assertEquals("-1K", DataSize.fromBytes(-1026).toStringShort());
        Assert.assertEquals("3M", DataSize.fromBytes((1 << 20) * 3 + 20).toStringShort());
        Assert.assertEquals("-3M", DataSize.fromBytes(-((1 << 20) * 3 + 20)).toStringShort());

        Assert.assertEquals("7E", DataSize.fromBytes(Long.MAX_VALUE).toStringShort());
        Assert.assertEquals("-8E", DataSize.fromBytes(Long.MIN_VALUE).toStringShort());
    }

    @Test
    public void testToPrettyString() {
        Assert.assertEquals("0", DataSize.ZERO.toPrettyString());
        Assert.assertEquals("656 bytes", DataSize.fromBytes(656).toPrettyString());
        Assert.assertEquals("-656 bytes", DataSize.fromBytes(-656).toPrettyString());
        Assert.assertEquals("1.6 KB", DataSize.fromBytes(1656).toPrettyString());
        Assert.assertEquals("-1.6 KB", DataSize.fromBytes(-1656).toPrettyString());
        Assert.assertEquals("17 KB", DataSize.fromBytes(16856).toPrettyString());
        Assert.assertEquals("-17 KB", DataSize.fromBytes(-16856).toPrettyString());
        Assert.assertEquals("2 KB", DataSize.fromKiloBytes(2).toPrettyString());
        Assert.assertEquals("-2 KB", DataSize.fromKiloBytes(-2).toPrettyString());
        Assert.assertEquals("2 MB", DataSize.fromMegaBytes(2).toPrettyString());
        Assert.assertEquals("-2 MB", DataSize.fromMegaBytes(-2).toPrettyString());
        Assert.assertEquals("1.9 GB", DataSize.fromMegaBytes(1954).toPrettyString());
        Assert.assertEquals("-1.9 GB", DataSize.fromMegaBytes(-1954).toPrettyString());
        Assert.assertEquals("11 GB", DataSize.fromMegaBytes(10954).toPrettyString());
        Assert.assertEquals("-11 GB", DataSize.fromMegaBytes(-10954).toPrettyString());
        Assert.assertEquals("10.7 GB", DataSize.fromMegaBytes(10954).toPrettyString(50));
        Assert.assertEquals("-10.7 GB", DataSize.fromMegaBytes(-10954).toPrettyString(50));

        Assert.assertEquals("3.4 TB", DataSize.fromGigaBytes(3457).toPrettyString());
        Assert.assertEquals("-3.4 TB", DataSize.fromGigaBytes(-3457).toPrettyString());
        Assert.assertEquals("45 TB", DataSize.fromTeraBytes(45).toPrettyString());
        Assert.assertEquals("-45 TB", DataSize.fromTeraBytes(-45).toPrettyString());
        Assert.assertEquals("45 PB", DataSize.fromPetaBytes(45).toPrettyString());
        Assert.assertEquals("-45 PB", DataSize.fromPetaBytes(-45).toPrettyString());
        Assert.assertEquals("4 EB", DataSize.fromExaBytes(4).toPrettyString());
        Assert.assertEquals("-4 EB", DataSize.fromExaBytes(-4).toPrettyString());

        Assert.assertEquals("8191P", DataSize.fromBytes(Long.MAX_VALUE).toStringPetaBytes());
        Assert.assertEquals("-8192P", DataSize.fromBytes(Long.MIN_VALUE).toStringPetaBytes());
        Assert.assertEquals("7 EB", DataSize.fromBytes(Long.MAX_VALUE).toPrettyString());
        Assert.assertEquals("-8 EB", DataSize.fromBytes(Long.MIN_VALUE).toPrettyString());
    }
}
