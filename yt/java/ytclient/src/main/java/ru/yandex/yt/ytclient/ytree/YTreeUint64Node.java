package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;

public class YTreeUint64Node extends YTreeIntNode {
    public YTreeUint64Node(long value) {
        this(value, Collections.emptyMap());
    }

    public YTreeUint64Node(long value, Map<String, YTreeNode> attributes) {
        super(value, attributes);
    }

    public YTreeUint64Node(int value) {
        this(Integer.toUnsignedLong(value));
    }

    public YTreeUint64Node(int value, Map<String, YTreeNode> attributes) {
        this(Integer.toUnsignedLong(value), attributes);
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.UINT64;
    }

    @Override
    public boolean isSigned() {
        return false;
    }
}
