package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;

public class YTreeInt64Node extends YTreeIntNode {
    public YTreeInt64Node(long value) {
        this(value, Collections.emptyMap());
    }

    public YTreeInt64Node(long value, Map<String, YTreeNode> attributes) {
        super(value, attributes);
    }

    public YTreeInt64Node(int value) {
        this((long) value);
    }

    public YTreeInt64Node(int value, Map<String, YTreeNode> attributes) {
        this((long) value, attributes);
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.INT64;
    }

    @Override
    public boolean isSigned() {
        return true;
    }
}
