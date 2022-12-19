package tech.ytsaurus.skiff.deserializer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;

public class SkiffParser {
    private final ByteBuffer buffer;

    public SkiffParser(byte[] buffer) {
        this.buffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
    }

    public byte parseInt8() {
        return buffer.get();
    }

    public short parseInt16() {
        return buffer.getShort();
    }

    public int parseInt32() {
        return buffer.getInt();
    }

    public long parseInt64() {
        return buffer.getLong();
    }

    public BigInteger parseInt128() {
        byte[] data = getDataInBigEndian(16);
        int sign = (data[0] < 0 ? -1 : 1);
        return new BigInteger(sign, data);
    }

    public short parseUint8() {
        return (short) (buffer.get() & 0xff);
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
        return buffer.getDouble();
    }

    public boolean parseBoolean() {
        return (buffer.get() != 0);
    }

    public byte[] parseString32() {
        int length = buffer.getInt();
        if (length < 0) {
            throw new IllegalArgumentException("Max length of string in Java = Integer.MAX_VALUE");
        }
        return getData(length);
    }

    public byte[] parseYson32() {
        return parseString32();
    }

    public short parseVariant8Tag() {
        return (short) (buffer.get() & 0xff);
    }

    public int parseVariant16Tag() {
        byte firstByte = buffer.get();
        byte secondByte = buffer.get();
        return ((secondByte & 0xff) << 8) | (firstByte & 0xff);
    }

    public boolean hasMoreData() {
        return buffer.hasRemaining();
    }

    private byte[] getDataInBigEndian(int length) {
        byte[] data = getData(length);
        Collections.reverse(Arrays.asList(data));
        return data;
    }

    private byte[] getData(int length) {
        byte[] byteArray = new byte[length];
        buffer.get(byteArray);
        return byteArray;
    }
}
