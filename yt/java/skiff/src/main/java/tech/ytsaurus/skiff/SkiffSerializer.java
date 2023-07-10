package tech.ytsaurus.skiff;

import java.io.ByteArrayOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class SkiffSerializer implements AutoCloseable, Flushable {
    private final OutputStream byteOS;

    public SkiffSerializer(OutputStream byteOS) {
        this.byteOS = byteOS;
    }

    public void write(byte[] bytes) {
        try {
            byteOS.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeByte(byte number) {
        try {
            byteOS.write(number);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeShort(short number) {
        try {
            byteOS.write((number & 0xFF));
            byteOS.write((number >> 8) & 0xFF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeInt(int number) {
        try {
            byteOS.write((number & 0xFF));
            byteOS.write((number >> 8) & 0xFF);
            byteOS.write((number >> 16) & 0xFF);
            byteOS.write((number >> 24) & 0xFF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeLong(long number) {
        try {
            byteOS.write((int) (number & 0xFF));
            byteOS.write((int) ((number >> 8) & 0xFF));
            byteOS.write((int) ((number >> 16) & 0xFF));
            byteOS.write((int) ((number >> 24) & 0xFF));
            byteOS.write((int) ((number >> 32) & 0xFF));
            byteOS.write((int) ((number >> 40) & 0xFF));
            byteOS.write((int) ((number >> 48) & 0xFF));
            byteOS.write((int) ((number >> 56) & 0xFF));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeUint8(Long number) {
        serializeByte((byte) (number & 0xFF));
    }

    public void serializeUint16(Long number) {
        serializeShort((short) (number & 0xFF_FF));
    }

    public void serializeUint32(Long number) {
        serializeInt((int) (number & 0xFF_FF_FF_FFL));
    }

    public void serializeUint64(Long number) {
        serializeLong(number);
    }

    public void serializeDouble(double number) {
        try {
            byteOS.write(ByteBuffer
                    .allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble(number)
                    .array());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeBoolean(boolean bool) {
        try {
            byteOS.write(bool ? 1 : 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeUtf8(String string) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        serializeInt(bytes.length);
        try {
            byteOS.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeBytes(byte[] bytes) {
        serializeInt(bytes.length);
        try {
            byteOS.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void serializeGuid(GUID guid) {
        serializeInt(16);
        serializeLong(guid.getFirst());
        serializeLong(guid.getSecond());
    }

    public void serializeTimestamp(Instant instant) {
        serializeUint64(instant.toEpochMilli());
    }

    public void serializeYson(YTreeNode node) {
        var byteOutputStreamForYson = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(node, byteOutputStreamForYson);
        byte[] bytes = byteOutputStreamForYson.toByteArray();
        serializeInt(bytes.length);
        try {
            byteOS.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Flushes underlying output stream
     */
    @Override
    public void flush() throws IOException {
        byteOS.flush();
    }

    /**
     * Closes underlying output stream
     */
    @Override
    public void close() throws IOException {
        byteOS.close();
    }
}
