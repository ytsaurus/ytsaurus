package tech.ytsaurus.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataSizeTest {
    @Test
    public void testToStringShort() {
        assertEquals("0", DataSize.ZERO.toStringShort());
        assertEquals("10", DataSize.fromBytes(10).toStringShort());
        assertEquals("-10", DataSize.fromBytes(-10).toStringShort());
        assertEquals("1K", DataSize.fromBytes(1024).toStringShort());
        assertEquals("-1K", DataSize.fromBytes(-1024).toStringShort());
        assertEquals("1K", DataSize.fromBytes(1026).toStringShort());
        assertEquals("-1K", DataSize.fromBytes(-1026).toStringShort());
        assertEquals("3M", DataSize.fromBytes((1 << 20) * 3 + 20).toStringShort());
        assertEquals("-3M", DataSize.fromBytes(-((1 << 20) * 3 + 20)).toStringShort());

        assertEquals("7E", DataSize.fromBytes(Long.MAX_VALUE).toStringShort());
        assertEquals("-8E", DataSize.fromBytes(Long.MIN_VALUE).toStringShort());
    }

    @Test
    public void testToPrettyString() {
        assertEquals("0", DataSize.ZERO.toPrettyString());
        assertEquals("656 bytes", DataSize.fromBytes(656).toPrettyString());
        assertEquals("-656 bytes", DataSize.fromBytes(-656).toPrettyString());
        assertEquals("1.6 KB", DataSize.fromBytes(1656).toPrettyString());
        assertEquals("-1.6 KB", DataSize.fromBytes(-1656).toPrettyString());
        assertEquals("17 KB", DataSize.fromBytes(16856).toPrettyString());
        assertEquals("-17 KB", DataSize.fromBytes(-16856).toPrettyString());
        assertEquals("2 KB", DataSize.fromKiloBytes(2).toPrettyString());
        assertEquals("-2 KB", DataSize.fromKiloBytes(-2).toPrettyString());
        assertEquals("2 MB", DataSize.fromMegaBytes(2).toPrettyString());
        assertEquals("-2 MB", DataSize.fromMegaBytes(-2).toPrettyString());
        assertEquals("1.9 GB", DataSize.fromMegaBytes(1954).toPrettyString());
        assertEquals("-1.9 GB", DataSize.fromMegaBytes(-1954).toPrettyString());
        assertEquals("11 GB", DataSize.fromMegaBytes(10954).toPrettyString());
        assertEquals("-11 GB", DataSize.fromMegaBytes(-10954).toPrettyString());
        assertEquals("10.7 GB", DataSize.fromMegaBytes(10954).toPrettyString(50));
        assertEquals("-10.7 GB", DataSize.fromMegaBytes(-10954).toPrettyString(50));

        assertEquals("3.4 TB", DataSize.fromGigaBytes(3457).toPrettyString());
        assertEquals("-3.4 TB", DataSize.fromGigaBytes(-3457).toPrettyString());
        assertEquals("45 TB", DataSize.fromTeraBytes(45).toPrettyString());
        assertEquals("-45 TB", DataSize.fromTeraBytes(-45).toPrettyString());
        assertEquals("45 PB", DataSize.fromPetaBytes(45).toPrettyString());
        assertEquals("-45 PB", DataSize.fromPetaBytes(-45).toPrettyString());
        assertEquals("4 EB", DataSize.fromExaBytes(4).toPrettyString());
        assertEquals("-4 EB", DataSize.fromExaBytes(-4).toPrettyString());

        assertEquals("8191P", DataSize.fromBytes(Long.MAX_VALUE).toStringPetaBytes());
        assertEquals("-8192P", DataSize.fromBytes(Long.MIN_VALUE).toStringPetaBytes());
        assertEquals("7 EB", DataSize.fromBytes(Long.MAX_VALUE).toPrettyString());
        assertEquals("-8 EB", DataSize.fromBytes(Long.MIN_VALUE).toPrettyString());
    }
}
