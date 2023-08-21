package tech.ytsaurus.yson;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Writer that generates text yson.
 *
 * All underlying writer exceptions are transformed to UncheckedIOException.
 */
public class YsonTextWriter implements ClosableYsonConsumer {
    private static final int BUFFER_SIZE = 256;
    private final Writer writer;
    private boolean firstItem = false;
    private int depth = 0;
    private final char[] buffer = new char[BUFFER_SIZE];
    public YsonTextWriter(StringBuilder builder) {
        this(new StringBuilderWriterAdapter(builder));
    }

    public YsonTextWriter(Writer writer) {
        this.writer = writer;
    }

    public YsonTextWriter(OutputStream output) {
        this(new OutputStreamWriter(output));
    }

    /**
     * Closes underlying reader.
     */
    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void endNode() {
        if (depth > 0) {
            write(YsonTags.ITEM_SEPARATOR);
        }
    }

    @Override
    public void onInteger(long value) {
        write(Long.toString(value));
        endNode();
    }

    @Override
    public void onUnsignedInteger(long value) {
        write(Long.toUnsignedString(value));
        write("u");
        endNode();
    }

    @Override
    public void onBoolean(boolean value) {
        write(value ? "%true" : "%false");
        endNode();
    }

    @Override
    public void onDouble(double value) {
        if (Double.isFinite(value)) {
            write(Double.toString(value));
        } else if (Double.isNaN(value)) {
            write("%nan");
        } else if (value == Double.POSITIVE_INFINITY) {
            write("%+inf");
        } else if (value == Double.NEGATIVE_INFINITY) {
            write("%-inf");
        } else {
            // Actually we must never go to this case
            throw new IllegalStateException("Unexpected double: " + value);
        }

        endNode();
    }

    private void writeStringScalar(byte[] bytes, int offset, int length) {
        write('"');
        appendQuotedBytes(Arrays.copyOfRange(bytes, offset, offset + length));
        write('"');
    }

    @Override
    public void onString(byte[] bytes, int offset, int length) {
        writeStringScalar(bytes, offset, length);
        endNode();
    }

    @Override
    public void onString(String str) {
        write('"');
        appendQuotedBytes(str.getBytes(StandardCharsets.UTF_8));
        write('"');
        endNode();
    }

    @Override
    public void onEntity() {
        write(YsonTags.ENTITY);
        endNode();
    }

    @Override
    public void onListItem() {
        firstItem = false;
    }

    @Override
    public void onBeginList() {
        ++depth;
        firstItem = true;
        write(YsonTags.BEGIN_LIST);
    }

    @Override
    public void onEndList() {
        --depth;
        firstItem = false;
        write(YsonTags.END_LIST);
        endNode();
    }

    @Override
    public void onBeginAttributes() {
        ++depth;
        firstItem = true;
        write(YsonTags.BEGIN_ATTRIBUTES);
    }

    @Override
    public void onEndAttributes() {
        --depth;
        firstItem = false;
        write(YsonTags.END_ATTRIBUTES);
    }

    @Override
    public void onBeginMap() {
        ++depth;
        firstItem = true;
        write(YsonTags.BEGIN_MAP);
    }

    @Override
    public void onEndMap() {
        --depth;
        firstItem = false;
        write(YsonTags.END_MAP);
        endNode();
    }

    @Override
    public void onKeyedItem(byte[] key, int offset, int length) {
        firstItem = false;
        writeStringScalar(key, offset, length);
        write(YsonTags.KEY_VALUE_SEPARATOR);
    }

    private void appendQuotedByte(byte b) {
        YsonTextUtils.writeQuotedByte(b, writer);
    }

    private void appendQuotedBytes(byte[] bytes) {
        int offset = 0;
        for (byte b : bytes) {
            if (b > 31 && b < 127 && b != 34 && b != 92 && offset < BUFFER_SIZE) {
                this.buffer[offset++] = (char) b;
            } else {
                if (offset > 0) {
                    writeBuff(offset);
                }
                offset = 0;
                appendQuotedByte(b);
            }
        }
        if (offset > 0) {
            writeBuff(offset);
        }
    }

    private void writeBuff(int offset) {
        try {
            writer.write(this.buffer, 0, offset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void write(int b) {
        try {
            writer.write(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void write(String s) {
        try {
            writer.write(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static class StringBuilderWriterAdapter extends Writer {
        private final StringBuilder builder;

        StringBuilderWriterAdapter(StringBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void write(char[] chars, int i, int i1) {
            builder.append(chars, i, i1);
        }

        @Override
        public void write(String str) throws IOException {
            builder.append(str);
        }

        @Override
        public Writer append(char c) throws IOException {
            builder.append(c);
            return this;
        }

        @Override
        public Writer append(CharSequence csq) throws IOException {
            builder.append(csq);
            return this;
        }

        @Override
        public void write(int c) throws IOException {
            builder.append((char) c);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}

class YsonTextUtils {
    private static final char[] DIGITS = "0123456789abcdef".toCharArray();

    private YsonTextUtils() {
    }

    static void writeQuotedByte(byte b, Writer out) {
        try {
            switch (b) {
                case '\t':
                    out.append("\\t");
                    return;
                case '\n':
                    out.append("\\n");
                    return;
                case '\r':
                    out.append("\\r");
                    return;
                case '"':
                    out.append("\\\"");
                    return;
                case '\\':
                    out.append("\\\\");
                    return;
                default:
                    break;
            }
            if (b <= 0x1f || b >= 0x7f) {
                out.append("\\x");
                out.append(DIGITS[(b & 255) >>> 4]);
                out.append(DIGITS[b & 15]);
            } else {
                out.append((char) b);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
