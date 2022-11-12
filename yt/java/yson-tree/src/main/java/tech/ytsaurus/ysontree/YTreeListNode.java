package tech.ytsaurus.ysontree;

import java.util.List;
import java.util.Map;

public interface YTreeListNode extends YTreeCompositeNode<YTreeNode> {

    YTreeNode get(int index);

    YTreeNode set(int index, YTreeNode value);

    YTreeNode remove(int index);

    void add(YTreeNode value);

    default void addAll(List<YTreeNode> values) {
        values.forEach(this::add);
    }

    default String getString(int index) {
        return get(index).stringValue();
    }

    default byte[] getBytes(int index) {
        return get(index).bytesValue();
    }

    default int getInt(int index) {
        return get(index).intValue();
    }

    default long getLong(int index) {
        return get(index).longValue();
    }

    default boolean getBool(int index) {
        return get(index).boolValue();
    }

    default double getDouble(int index) {
        return get(index).doubleValue();
    }

    default YTreeListNode getList(int index) {
        return get(index).listNode();
    }

    default YTreeMapNode getMap(int index) {
        return get(index).mapNode();
    }

    default YTreeBuilder toListBuilder() {
        YTreeBuilder builder = YTree.builder();
        Map<String, YTreeNode> attributes = getAttributes();
        if (!attributes.isEmpty()) {
            builder.beginAttributes();
            for (Map.Entry<String, YTreeNode> entry : attributes.entrySet()) {
                builder.key(entry.getKey()).value(entry.getValue());
            }
            builder.endAttributes();
        }
        builder.beginList();
        for (YTreeNode node : asList()) {
            builder.value(node);
        }
        return builder;
    }
}
