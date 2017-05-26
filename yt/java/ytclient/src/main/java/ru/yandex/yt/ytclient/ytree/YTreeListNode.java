package ru.yandex.yt.ytclient.ytree;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class YTreeListNode extends YTreeNode {
    public static final YTreeListNode EMPTY = new YTreeListNode(Collections.emptyList());

    private final List<YTreeNode> value;

    public YTreeListNode(List<YTreeNode> value) {
        this(value, Collections.emptyMap());
    }

    public YTreeListNode(List<YTreeNode> value, Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = Objects.requireNonNull(value);
    }

    public int size() {
        return value.size();
    }

    public YTreeNode get(int index) {
        YTreeNode node = value.get(index);
        if (node == null) {
            node = YTreeEntityNode.INSTANCE;
        }
        return node;
    }

    @Override
    public YTreeNodeType getType() {
        return YTreeNodeType.LIST;
    }

    @Override
    public List<YTreeNode> listValue() {
        return value;
    }

    @Override
    protected void writeValueTo(YTreeConsumer consumer) {
        consumer.onBeginList();
        consumer.onListFragment(value);
        consumer.onEndList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YTreeListNode)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        YTreeListNode that = (YTreeListNode) o;

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
