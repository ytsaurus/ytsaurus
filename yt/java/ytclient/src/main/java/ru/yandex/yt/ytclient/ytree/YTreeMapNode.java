package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public class YTreeMapNode extends YTreeNode {
    public static final YTreeMapNode EMPTY = new YTreeMapNode(Collections.emptyMap());

    private final Map<String, YTreeNode> value;

    public YTreeMapNode(Map<String, YTreeNode> value) {
        this(value, Collections.emptyMap());
    }

    public YTreeMapNode(Map<String, YTreeNode> value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = Objects.requireNonNull(value);
    }

    public boolean hasKey(String key) {
        return value.containsKey(key);
    }

    public YTreeNode get(String key) {
        YTreeNode node = value.get(key);
        if (node == null && hasKey(key)) {
            node = YTreeEntityNode.INSTANCE;
        }
        return node;
    }

    public YTreeNode get(String key, YTreeNode defaultValue) {
        YTreeNode node = get(key);
        if (node == null) {
            node = defaultValue;
        }
        return node;
    }

    public YTreeNode getOrThrow(String key) {
        YTreeNode node = get(key);
        if (node == null) {
            throw new NoSuchElementException("Missing key " + key);
        }
        return node;
    }

    public String getString(String key, String defaultValue) {
        return YTreeUtil.stringValue(get(key), defaultValue);
    }

    public byte[] getBytes(String key, byte[] defaultValue) {
        return YTreeUtil.bytesValue(get(key), defaultValue);
    }

    public long getLong(String key, long defaultValue) {
        return YTreeUtil.longValue(get(key), defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return YTreeUtil.intValue(get(key), defaultValue);
    }

    public double getDouble(String key, double defaultValue) {
        return YTreeUtil.doubleValue(get(key), defaultValue);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return YTreeUtil.booleanValue(get(key), defaultValue);
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.MAP;
    }

    @Override
    public Map<String, YTreeNode> mapValue() {
        return value;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        consumer.onBeginMap();
        consumer.onMapFragment(value);
        consumer.onEndMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeMapNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeMapNode that = (YTreeMapNode) o;

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
