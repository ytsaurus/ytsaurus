package ru.yandex.yt.ytclient.yson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.CodedOutputStream;

import ru.yandex.yt.ytclient.ytree.YTreeConsumer;
import ru.yandex.yt.ytclient.ytree.YTreeNode;

public class YsonBinaryEncoder implements YTreeConsumer {
    private final CodedOutputStream output;
    private boolean firstItem;

    public YsonBinaryEncoder(CodedOutputStream output) {
        this.output = output;
    }

    private void writeByte(byte b) throws IOException {
        output.writeRawByte(b);
    }

    private void writeBytes(byte[] bytes) throws IOException {
        output.writeSInt32NoTag(bytes.length);
        output.writeRawBytes(bytes);
    }

    private void writeItemSeparator() throws IOException {
        if (firstItem) {
            firstItem = false;
        } else {
            output.writeRawByte(YsonTags.ITEM_SEPARATOR);
        }
    }

    @Override
    public void onStringScalar(String value) {
        onStringScalar(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void onStringScalar(byte[] value) {
        try {
            writeByte(YsonTags.BINARY_STRING);
            writeBytes(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onInt64Scalar(long value) {
        try {
            writeByte(YsonTags.BINARY_INT);
            output.writeSInt64NoTag(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onUint64Scalar(long value) {
        try {
            writeByte(YsonTags.BINARY_UINT);
            output.writeUInt64NoTag(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onDoubleScalar(double value) {
        try {
            writeByte(YsonTags.BINARY_DOUBLE);
            output.writeDoubleNoTag(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onBooleanScalar(boolean value) {
        try {
            writeByte(value ? YsonTags.BINARY_TRUE : YsonTags.BINARY_FALSE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEntity() {
        try {
            writeByte(YsonTags.ENTITY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onBeginList() {
        try {
            writeByte(YsonTags.BEGIN_LIST);
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
            writeByte(YsonTags.END_LIST);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    @Override
    public void onBeginMap() {
        try {
            writeByte(YsonTags.BEGIN_MAP);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = true;
    }

    @Override
    public void onKeyedItem(String key) {
        try {
            writeItemSeparator();
            onStringScalar(key);
            writeByte(YsonTags.KEY_VALUE_SEPARATOR);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void onEndMap() {
        try {
            writeByte(YsonTags.END_MAP);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    @Override
    public void onBeginAttributes() {
        try {
            writeByte(YsonTags.BEGIN_ATTRIBUTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = true;
    }

    @Override
    public void onEndAttributes() {
        try {
            writeByte(YsonTags.END_ATTRIBUTES);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        firstItem = false;
    }

    public static void encode(CodedOutputStream stream, YTreeNode value) {
        YsonBinaryEncoder encoder = new YsonBinaryEncoder(stream);
        if (value != null) {
            value.writeTo(encoder);
        } else {
            encoder.onEntity();
        }
    }

    public static void encode(OutputStream stream, YTreeNode value) {
        CodedOutputStream cos = CodedOutputStream.newInstance(stream);
        encode(cos, value);
        try {
            cos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] encode(YTreeNode value) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        encode(stream, value);
        return stream.toByteArray();
    }
}
