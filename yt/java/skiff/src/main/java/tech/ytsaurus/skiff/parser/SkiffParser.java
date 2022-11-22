package tech.ytsaurus.skiff.parser;

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

    byte parseInt8() {
        return buffer.get();
    }

    short parseInt16() {
        return buffer.getShort();
    }

    int parseInt32() {
        return buffer.getInt();
    }

    long parseInt64() {
        return buffer.getLong();
    }

    BigInteger parseInt128() {
        byte[] data = getDataInBigEndian(16);
        int sign = (data[0] < 0 ? -1 : 1);
        return new BigInteger(sign, data);
    }

    short parseUint8() {
        return (short) (buffer.get() & 0xff);
    }

    int parseUint16() {
        return new BigInteger(1, getDataInBigEndian(2)).intValue();
    }

    long parseUint32() {
        return new BigInteger(1, getDataInBigEndian(4)).longValue();
    }

    BigInteger parseUint64() {
        return new BigInteger(1, getDataInBigEndian(8));
    }

    BigInteger parseUint128() {
        return new BigInteger(1, getDataInBigEndian(16));
    }

    double parseDouble() {
        return buffer.getDouble();
    }

    boolean parseBoolean() {
        return (buffer.get() != 0);
    }

    byte[] parseString32() {
        int length = buffer.getInt();
        if (length < 0) {
            throw new IllegalArgumentException("Max length of string in Java = Integer.MAX_VALUE");
        }
        return getData(length);
    }

    byte[] parseYson32() {
        return parseString32();
    }

    short parseVariant8Tag() {
        return (short) (buffer.get() & 0xff);
    }

    int parseVariant16Tag() {
        byte firstByte = buffer.get();
        byte secondByte = buffer.get();
        return ((secondByte & 0xff) << 8) | (firstByte & 0xff);
    }

    boolean hasMoreData() {
        return buffer.hasRemaining();
    }

    private byte[] getDataInBigEndian(int length) {
        byte[] data = getData(length);
        Collections.reverse(Arrays.asList(data));
        return data;
    }

    private byte[] getData(int length) {
        byte[] byteArray = new byte[length];
        buffer.get(byteArray, buffer.position(), length);
        return byteArray;
    }
}
