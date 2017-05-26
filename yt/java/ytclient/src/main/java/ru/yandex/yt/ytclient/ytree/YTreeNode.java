package ru.yandex.yt.ytclient.ytree;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

import ru.yandex.yt.ytclient.yson.YsonBinaryDecoder;
import ru.yandex.yt.ytclient.yson.YsonBinaryEncoder;
import ru.yandex.yt.ytclient.yson.YsonTextEncoder;

public abstract class YTreeNode implements YTreeConvertible {
    private final Map<String, YTreeNode> attributes;

    protected YTreeNode(Map<String, YTreeNode> attributes) {
        this.attributes = Objects.requireNonNull(attributes);
    }

    public Map<String, YTreeNode> getAttributes() {
        return attributes;
    }

    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    public YTreeNode getAttribute(String key) {
        YTreeNode node = attributes.get(key);
        if (node == null && hasAttribute(key)) {
            node = YTreeEntityNode.INSTANCE;
        }
        return node;
    }

    public YTreeNode getAttribute(String key, YTreeNode defaultValue) {
        YTreeNode node = getAttribute(key);
        if (node == null) {
            node = defaultValue;
        }
        return node;
    }

    public YTreeNode getAttributeOrThrow(String key) {
        YTreeNode node = getAttribute(key);
        if (node == null) {
            throw new NoSuchElementException("Missing attribute " + key);
        }
        return node;
    }

    public String getStringAttribute(String key, String defaultValue) {
        return YTreeUtil.stringValue(getAttribute(key), defaultValue);
    }

    public byte[] getBytesAttribute(String key, byte[] defaultValue) {
        return YTreeUtil.bytesValue(getAttribute(key), defaultValue);
    }

    public long getLongAttribute(String key, long defaultValue) {
        return YTreeUtil.longValue(getAttribute(key), defaultValue);
    }

    public int getIntAttribute(String key, int defaultValue) {
        return YTreeUtil.intValue(getAttribute(key), defaultValue);
    }

    public double getDoubleAttribute(String key, double defaultValue) {
        return YTreeUtil.doubleValue(getAttribute(key), defaultValue);
    }

    public boolean getBooleanAttribute(String key, boolean defaultValue) {
        return YTreeUtil.booleanValue(getAttribute(key), defaultValue);
    }

    public final void writeTo(YTreeConsumer consumer) {
        if (!attributes.isEmpty()) {
            consumer.onBeginAttributes();
            consumer.onMapFragment(attributes);
            consumer.onEndAttributes();
        }
        writeValueTo(consumer);
    }

    public abstract YTreeNodeType getType();

    public String stringValue() {
        throw new IllegalArgumentException("Cannot get string value from " + getType());
    }

    public byte[] bytesValue() {
        throw new IllegalArgumentException("Cannot get bytes value from " + getType());
    }

    public long longValue() {
        throw new IllegalArgumentException("Cannot get long value from " + getType());
    }

    public int intValue() {
        throw new IllegalArgumentException("Cannot get int value from " + getType());
    }

    public double doubleValue() {
        throw new IllegalArgumentException("Cannot get double value from " + getType());
    }

    public boolean booleanValue() {
        throw new IllegalArgumentException("Cannot get boolean value from " + getType());
    }

    public Map<String, YTreeNode> mapValue() {
        throw new IllegalArgumentException("Cannot get map value from " + getType());
    }

    public List<YTreeNode> listValue() {
        throw new IllegalArgumentException("Cannot get list value from " + getType());
    }

    protected abstract void writeValueTo(YTreeConsumer consumer);

    @Override
    public YTreeNode toYTree() {
        return this;
    }

    @Override
    public String toString() {
        return YsonTextEncoder.encode(this);
    }

    public byte[] toBinary() {
        return YsonBinaryEncoder.encode(this);
    }

    public static YTreeNode parseStream(InputStream stream) {
        CodedInputStream input = CodedInputStream.newInstance(stream);
        input.setSizeLimit(Integer.MAX_VALUE);
        return parseStream(input);
    }

    public static YTreeNode parseStream(CodedInputStream stream) {
        try {
            return YsonBinaryDecoder.parseNode(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static YTreeNode parseString(String value) {
        return parseByteArray(value.getBytes(StandardCharsets.UTF_8));
    }

    public static YTreeNode parseByteArray(byte[] bytes) {
        return parseStream(CodedInputStream.newInstance(bytes));
    }

    public static YTreeNode parseByteString(ByteString value) {
        return parseStream(value.newCodedInput());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeNode)) {
            return false;
        }

        YTreeNode that = (YTreeNode) o;

        return attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        return attributes.hashCode();
    }
}
