package tech.ytsaurus.yson;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Writer that generates binary yson.
 *
 * All underlying output errors are translated to UncheckedIOException
 */
public class YsonBinaryWriter implements ClosableYsonConsumer {
    // Note about implementation:
    // this class has a bunch of methods (related to varints)
    // that are copy-pasted from com.google.protobuf.OutputStreamEncoder
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final OutputStream output;
    private final byte[] buffer;
    final int limit;
    private int position = 0;
    private boolean firstItem = true;

    @SuppressWarnings("unused")
    public YsonBinaryWriter(OutputStream output) {
        this(output, DEFAULT_BUFFER_SIZE);
    }

    public YsonBinaryWriter(OutputStream output, int bufferSize) {
        this.output = output;

        // We want buffer to be able to store at least yson tag and varint of maximum size (10 bytes)
        limit = Math.max(bufferSize, 16);
        buffer = new byte[limit];
    }

    /**
     * Flushes all buffered data to underlying stream
     * and closes both YsonBinary writer and underlying stream.
     */
    @Override
    public void close() {
        try {
            doFlush();
            output.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onString(byte[] value, int offset, int length) {
        try {
            flushIfNotAvailable(1 + VarintUtils.MAX_VARINT32_SIZE);

            buffer[position++] = YsonTags.BINARY_STRING;
            writeSInt32Unchecked(length);
            // TODO: improve performance
            writeRawBytesChecked(Arrays.copyOfRange(value, offset, offset + length));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onInteger(long value) {
        try {
            flushIfNotAvailable(1 + VarintUtils.MAX_VARINT64_SIZE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer[position++] = YsonTags.BINARY_INT;
        writeSInt64Unchecked(value);
    }

    @Override
    public void onUnsignedInteger(long value) {
        try {
            flushIfNotAvailable(1 + VarintUtils.MAX_VARINT64_SIZE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer[position++] = YsonTags.BINARY_UINT;
        writeUInt64Unchecked(value);
    }

    @Override
    public void onDouble(double value) {
        try {
            flushIfNotAvailable(1 + Double.BYTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer[position++] = YsonTags.BINARY_DOUBLE;
        writeDoubleUnchecked(value);
    }

    @Override
    public void onBoolean(boolean value) {
        try {
            writeByteChecked(value ? YsonTags.BINARY_TRUE : YsonTags.BINARY_FALSE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEntity() {
        try {
            writeByteChecked(YsonTags.ENTITY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onBeginList() {
        try {
            writeByteChecked(YsonTags.BEGIN_LIST);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = true;
    }

    @Override
    public void onListItem() {
        try {
            writeItemSeparator();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEndList() {
        try {
            writeByteChecked(YsonTags.END_LIST);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    @Override
    public void onBeginMap() {
        try {
            writeByteChecked(YsonTags.BEGIN_MAP);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = true;
    }

    @Override
    public void onKeyedItem(byte[] key, int offset, int length) {
        try {
            writeItemSeparator();
            onString(key, offset, length);
            writeByteChecked(YsonTags.KEY_VALUE_SEPARATOR);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEndMap() {
        try {
            writeByteChecked(YsonTags.END_MAP);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    @Override
    public void onBeginAttributes() {
        try {
            writeByteChecked(YsonTags.BEGIN_ATTRIBUTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = true;
    }

    @Override
    public void onEndAttributes() {
        try {
            writeByteChecked(YsonTags.END_ATTRIBUTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    /**
     * Flushes all buffered data to underlying stream.
     */
    public void flush() {
        try {
            doFlush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeItemSeparator() throws IOException {
        if (firstItem) {
            firstItem = false;
        } else {
            writeByteChecked(YsonTags.ITEM_SEPARATOR);
        }
    }

    // copy-pasted from com.google.protobuf.OutputStreamEncoder
    private void doFlush() throws IOException {
        output.write(buffer, 0, position);
        position = 0;
    }

    // copy-pasted from com.google.protobuf.OutputStreamEncoder
    private void flushIfNotAvailable(int requiredSize) throws IOException {
        if (limit - position < requiredSize) {
            doFlush();
        }
    }

    // copy-pasted from com.google.protobuf.OutputStreamEncoder
    private void writeSInt32Unchecked(int value) {
        writeUInt32Unchecked(VarintUtils.encodeZigZag32(value));
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeUInt32Unchecked(int value) {
        //noinspection DuplicatedCode
        while (true) {
            if ((value & ~0x7F) == 0) {
                buffer[position++] = (byte) value;
                return;
            } else {
                buffer[position++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeSInt64Unchecked(long value) {
        writeUInt64Unchecked(VarintUtils.encodeZigZag64(value));
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeUInt64Unchecked(long value) {
        //noinspection DuplicatedCode
        while (true) {
            if ((value & ~0x7F) == 0) {
                buffer[position++] = (byte) value;
                return;
            } else {
                buffer[position++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeDoubleUnchecked(double value) {
        long bits = Double.doubleToLongBits(value);

        buffer[position++] = (byte) (bits & 0xFF);
        buffer[position++] = (byte) ((bits >> 8) & 0xFF);
        buffer[position++] = (byte) ((bits >> 16) & 0xFF);
        buffer[position++] = (byte) ((bits >> 24) & 0xFF);
        buffer[position++] = (byte) ((int) (bits >> 32) & 0xFF);
        buffer[position++] = (byte) ((int) (bits >> 40) & 0xFF);
        buffer[position++] = (byte) ((int) (bits >> 48) & 0xFF);
        buffer[position++] = (byte) ((int) (bits >> 56) & 0xFF);
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeRawBytesChecked(byte[] value) throws IOException {
        int length = value.length;
        int offset = 0;

        if (limit - position >= length) {
            // We have room in the current buffer.
            System.arraycopy(value, offset, buffer, position, length);
            position += length;
        } else {
            // Write extends past current buffer.  Fill the rest of this buffer and
            // flush.
            final int bytesWritten = limit - position;
            System.arraycopy(value, offset, buffer, position, bytesWritten);
            offset += bytesWritten;
            length -= bytesWritten;
            position = limit;
            doFlush();

            // Now deal with the rest.
            // Since we have an output stream, this is our buffer
            // and buffer offset == 0
            if (length <= limit) {
                // Fits in new buffer.
                System.arraycopy(value, offset, buffer, 0, length);
                position = length;
            } else {
                // Write is very big.  Let's do it all at once.
                output.write(value, offset, length);
            }
        }
    }

    // copy-pasted from com.google.protobuf.CodedOutputStream
    private void writeByteChecked(byte b) throws IOException {
        if (position == limit) {
            doFlush();
        }
        buffer[position++] = b;
    }
}
