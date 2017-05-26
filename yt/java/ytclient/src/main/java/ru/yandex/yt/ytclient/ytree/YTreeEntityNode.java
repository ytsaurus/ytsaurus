package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.Map;

public class YTreeEntityNode extends YTreeNode {
    public static final YTreeEntityNode INSTANCE = new YTreeEntityNode();

    public YTreeEntityNode() {
        this(Collections.emptyMap());
    }

    public YTreeEntityNode(Map<String, YTreeNode> attributes) {
        super(attributes);
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.ENTITY;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        consumer.onEntity();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof YTreeEntityNode)) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
