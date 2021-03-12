package ru.yandex.yson;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Arrays;

import javax.annotation.Nonnull;

/**
 * Writer that generates text yson.
 *
 * All underlying writer exceptions are transformed to UncheckedIOException.
 */
public class YsonTextWriter implements ClosableYsonConsumer {
    private final Writer writer;
    private boolean firstItem = false;

    public YsonTextWriter(@Nonnull StringBuilder builder) {
        this(new StringBuilderWriterAdapter(builder));
    }

    public YsonTextWriter(@Nonnull Writer writer) {
        this.writer = writer;
    }

    public YsonTextWriter(@Nonnull OutputStream output) {
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

    @Override
    public void onInteger(long value) {
        write(Long.toString(value));
    }

    @Override
    public void onUnsignedInteger(long value) {
        write(Long.toUnsignedString(value));
        write("u");
    }

    @Override
    public void onBoolean(boolean value) {
        write(value ? "%true" : "%false");
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
    }

    @Override
    public void onString(@Nonnull byte[] bytes, int offset, int length) {
        write('"');
        appendQuotedBytes(Arrays.copyOfRange(bytes, offset, offset + length));
        write('"');
    }

    @Override
    public void onEntity() {
        write(YsonTags.ENTITY);
    }

    @Override
    public void onListItem() {
        if (firstItem) {
            firstItem = false;
        } else {
            write(YsonTags.ITEM_SEPARATOR);
        }
    }

    @Override
    public void onBeginList() {
        firstItem = true;
        write(YsonTags.BEGIN_LIST);
    }

    @Override
    public void onEndList() {
        firstItem = false;
        write(YsonTags.END_LIST);
    }

    @Override
    public void onBeginAttributes() {
        firstItem = true;
        write(YsonTags.BEGIN_ATTRIBUTES);
    }

    @Override
    public void onEndAttributes() {
        firstItem = false;
        write(YsonTags.END_ATTRIBUTES);
    }

    @Override
    public void onBeginMap() {
        firstItem = true;
        write(YsonTags.BEGIN_MAP);
    }

    @Override
    public void onEndMap() {
        firstItem = false;
        write(YsonTags.END_MAP);
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] key, int offset, int length) {
        if (firstItem) {
            firstItem = false;
        } else {
            write(YsonTags.ITEM_SEPARATOR);
        }
        onString(key, offset, length);
        write(YsonTags.KEY_VALUE_SEPARATOR);
    }

    private void appendQuotedByte(byte b) {
        YsonTextUtils.writeQuotedByte(b, writer);
    }

    private void appendQuotedBytes(byte[] bytes) {
        for (byte b : bytes) {
            appendQuotedByte(b);
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
        public void write(@Nonnull char[] chars, int i, int i1) {
            builder.append(chars, i, i1);
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
    private static final char[] digits = "0123456789abcdef".toCharArray();

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
            }
            if (b <= 0x1f || b >= 0x7f) {
                out.append("\\x");
                out.append(digits[(b & 255) >>> 4]);
                out.append(digits[b & 15]);
            } else {
                out.append((char)b);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

