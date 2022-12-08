package tech.ytsaurus.client.misc;

import io.netty.buffer.ByteBuf;

/**
 * Класс для подсчёта crc64 аналогично тому, как это делается в yt
 * <p>
 * Основано на реализации yt/19_2/yt/core/misc/checksum.cpp
 */
public final class YtCrc64 {
    private static final long POLYNOMIAL = Long.parseUnsignedLong("E543279765927881", 16);
    private static final long[] TABLE = createTable(POLYNOMIAL);

    private long crc;

    public YtCrc64() {
        this(0L);
    }

    public YtCrc64(long seed) {
        reset(seed);
    }

    public YtCrc64 reset(long seed) {
        crc = Long.reverseBytes(seed);
        return this;
    }

    public YtCrc64 reset() {
        return reset(0L);
    }

    public YtCrc64 update(byte b) {
        crc = TABLE[(int) (crc & 0xff) ^ (b & 0xff)] ^ (crc >>> 8);
        return this;
    }

    public YtCrc64 update(byte[] data, int offset, int length) {
        while (length-- > 0) {
            update(data[offset++]);
        }
        return this;
    }

    public YtCrc64 update(byte[] data) {
        return update(data, 0, data.length);
    }

    public YtCrc64 update(ByteBuf in) {
        while (in.isReadable()) {
            update(in.readByte());
        }
        return this;
    }

    public long getValue() {
        return Long.reverseBytes(crc);
    }

    public static long fromBytes(byte[] data, int offset, int length) {
        return new YtCrc64().update(data, offset, length).getValue();
    }

    public static long fromBytes(byte[] data) {
        return fromBytes(data, 0, data.length);
    }

    public static long fromBytes(ByteBuf in) {
        return new YtCrc64().update(in).getValue();
    }

    private static long[] createTable(long polynomial) {
        long invPolynomial = Long.reverse(polynomial);
        long[] t = new long[256];
        for (int i = 0; i < 256; i++) {
            long crc = Integer.reverse(i) >>> 24;
            for (int j = 0; j < 8; j++) {
                crc = (crc >>> 1) ^ ((crc & 1) * invPolynomial);
            }
            t[i] = Long.reverseBytes(Long.reverse(crc));
        }
        return t;
    }
}
