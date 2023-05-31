package tech.ytsaurus.skiff;

import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import tech.ytsaurus.yson.BufferReference;
import tech.ytsaurus.yson.BufferedStreamZeroCopyInput;
import tech.ytsaurus.yson.StreamReader;

public class SkiffParser {
    private final StreamReader reader;
    private final BufferReference bufferReference = new BufferReference();

    /**
     * @param input input stream with Little Endian byte order
     */
    public SkiffParser(InputStream input) {
        this.reader = new StreamReader(new BufferedStreamZeroCopyInput(input, 1 << 16));
    }

    public byte parseInt8() {
        return reader.readByte();
    }

    public short parseInt16() {
        getDataInLittleEndian(2);
        byte[] buffer = bufferReference.getBuffer();
        int bufferOffset = bufferReference.getOffset();
        return (short) ((buffer[bufferOffset] & 0xFF) |
                ((buffer[bufferOffset + 1] & 0xFF) << 8));

    }

    public int parseInt32() {
        getDataInLittleEndian(4);
        byte[] buffer = bufferReference.getBuffer();
        int bufferOffset = bufferReference.getOffset();
        return (buffer[bufferOffset] & 0xFF) |
                ((buffer[bufferOffset + 1] & 0xFF) << 8) |
                ((buffer[bufferOffset + 2] & 0xFF) << 16) |
                ((buffer[bufferOffset + 3] & 0xFF) << 24);
    }

    public long parseInt64() {
        getDataInLittleEndian(8);
        long result = 0;
        for (int i = 7; i >= 0; i--) {
            result <<= 8;
            result |= (bufferReference.getBuffer()[bufferReference.getOffset() + i] & 0xFF);
        }
        return result;
    }

    public BigInteger parseInt128() {
        byte[] data = getDataInBigEndian(16);
        int sign = (data[0] < 0 ? -1 : 1);
        return new BigInteger(sign, data);
    }

    public long parseUint8() {
        return reader.readByte() & 0xff;
    }

    public long parseUint16() {
        getDataInLittleEndian(2);
        byte[] buffer = bufferReference.getBuffer();
        int bufferOffset = bufferReference.getOffset();
        return (buffer[bufferOffset] & 0xFF) |
                ((buffer[bufferOffset + 1] & 0xFF) << 8);
    }

    public long parseUint32() {
        return new BigInteger(1, getDataInBigEndian(4)).longValue();
    }

    public long parseUint64() {
        return new BigInteger(1, getDataInBigEndian(8)).longValue();
    }

    public BigInteger parseUint128() {
        return new BigInteger(1, getDataInBigEndian(16));
    }

    public double parseDouble() {
        return ByteBuffer.wrap(getDataInBigEndian(8))
                .order(ByteOrder.BIG_ENDIAN)
                .getDouble();
    }

    public boolean parseBoolean() {
        return (reader.readByte() != 0);
    }

    public BufferReference parseString32() {
        int length = parseInt32();
        if (length < 0) {
            throw new IllegalArgumentException("Max length of string in Java = Integer.MAX_VALUE");
        }
        getDataInLittleEndian(length);
        return bufferReference;
    }

    public BufferReference parseYson32() {
        return parseString32();
    }

    public short parseVariant8Tag() {
        return (short) (reader.readByte() & 0xff);
    }

    public int parseVariant16Tag() {
        return (int) parseUint16();
    }

    public byte[] getDataInBigEndian(int length) {
        getDataInLittleEndian(length);
        var data = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            data[length - i - 1] = bufferReference.getBuffer()[bufferReference.getOffset() + i];
        }
        return data;
    }

    public boolean hasMoreData() {
        boolean isNotEOF;
        isNotEOF = reader.tryReadByte() != StreamReader.END_OF_STREAM;
        if (isNotEOF) {
            reader.unreadByte();
        }
        return isNotEOF;
    }

    private void getDataInLittleEndian(int length) {
        reader.readBytes(length, bufferReference);
    }
}
