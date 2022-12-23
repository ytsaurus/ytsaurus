package tech.ytsaurus.skiff.deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;

public class SkiffParser {
    private final InputStream input;

    /**
     * @param input input stream with Little Endian byte order
     */
    public SkiffParser(InputStream input) {
        this.input = input;
    }

    public byte parseInt8() {
        return getDataInLittleEndian(1)[0];
    }

    public short parseInt16() {
        return ByteBuffer.wrap(getDataInLittleEndian(2))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getShort();
    }

    public int parseInt32() {
        return ByteBuffer.wrap(getDataInLittleEndian(4))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
    }

    public long parseInt64() {
        return ByteBuffer.wrap(getDataInLittleEndian(8))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    public BigInteger parseInt128() {
        byte[] data = getDataInBigEndian(16);
        int sign = (data[0] < 0 ? -1 : 1);
        return new BigInteger(sign, data);
    }

    public short parseUint8() {
        return (short) (getDataInBigEndian(1)[0] & 0xff);
    }

    public int parseUint16() {
        return new BigInteger(1, getDataInBigEndian(2)).intValue();
    }

    public long parseUint32() {
        return new BigInteger(1, getDataInBigEndian(4)).longValue();
    }

    public BigInteger parseUint64() {
        return new BigInteger(1, getDataInBigEndian(8));
    }

    public BigInteger parseUint128() {
        return new BigInteger(1, getDataInBigEndian(16));
    }

    public double parseDouble() {
        return ByteBuffer.wrap(getDataInLittleEndian(8))
                .order(ByteOrder.LITTLE_ENDIAN)
                .getDouble();
    }

    public boolean parseBoolean() {
        return (getDataInLittleEndian(1)[0] != 0);
    }

    public byte[] parseString32() {
        int length = parseInt32();
        if (length < 0) {
            throw new IllegalArgumentException("Max length of string in Java = Integer.MAX_VALUE");
        }
        return getDataInLittleEndian(length);
    }

    public byte[] parseYson32() {
        return parseString32();
    }

    public short parseVariant8Tag() {
        return parseUint8();
    }

    public int parseVariant16Tag() {
        return parseUint16();
    }

    public boolean hasMoreData() {
        if (!input.markSupported()) {
            throw new IllegalArgumentException("Input stream is not support mark()");
        }
        input.mark(1);
        boolean isNotEOF;
        try {
            isNotEOF = input.read() != -1;
            input.reset();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return isNotEOF;
    }

    private byte[] getDataInLittleEndian(int length) {
        try {
            return input.readNBytes(length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] getDataInBigEndian(int length) {
        byte[] data = getDataInLittleEndian(length);
        Collections.reverse(Arrays.asList(data));
        return data;
    }
}
