package ru.yandex.yt.ytclient.ytree;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class YTreeStringNode extends YTreeNode {
    private final String value;
    private final byte[] bytes;

    public YTreeStringNode(String value) {
        this(value, Collections.emptyMap());
    }

    public YTreeStringNode(byte[] bytes) {
        this(bytes, Collections.emptyMap());
    }

    public YTreeStringNode(String value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = Objects.requireNonNull(value);
        this.bytes = this.value.getBytes(StandardCharsets.UTF_8);
    }

    public YTreeStringNode(byte[] bytes, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.bytes = Objects.requireNonNull(bytes);
        this.value = new String(this.bytes, StandardCharsets.UTF_8);
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.STRING;
    }

    @Override
    public String stringValue() {
        return value;
    }

    @Override
    public byte[] bytesValue() {
        return bytes;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        if (consumer.isBinaryPreferred()) {
            consumer.onStringScalar(bytes);
        } else {
            consumer.onStringScalar(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeStringNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeStringNode that = (YTreeStringNode) o;

        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }
}
