package tech.ytsaurus.ysontree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import tech.ytsaurus.yson.YsonConsumer;

public class YTreeNodeUtils {
    private YTreeNodeUtils() {
    }

    public static void walk(YTreeNode node, YsonConsumer consumer, boolean stringAsBytes) {
        walk(node, consumer, stringAsBytes, false);
    }

    public static void walk(YTreeNode node, YsonConsumer consumer, boolean stringAsBytes, boolean sortKeys) {
        Function<Iterable<Map.Entry<String, YTreeNode>>, Iterable<Map.Entry<String, YTreeNode>>> walkEntries =
                 entries -> {
                    if (sortKeys) {
                        ArrayList<Map.Entry<String, YTreeNode>> copy = new ArrayList<>();
                        for (Map.Entry<String, YTreeNode> e : entries) {
                            copy.add(e);
                        }
                        copy.sort(Map.Entry.comparingByKey());
                        return copy;
                    } else {
                        return entries;
                    }
                };

        Map<String, YTreeNode> attributes = node.getAttributes();
        if (!attributes.isEmpty()) {
            consumer.onBeginAttributes();
            for (Map.Entry<String, YTreeNode> entry : walkEntries.apply(attributes.entrySet())) {
                consumer.onKeyedItem(entry.getKey());
                walk(entry.getValue(), consumer, stringAsBytes, sortKeys);
            }
            consumer.onEndAttributes();
        }
        if (node instanceof YTreeBooleanNode) {
            consumer.onBoolean(node.boolValue());
        } else if (node instanceof YTreeDoubleNode) {
            consumer.onDouble(node.doubleValue());
        } else if (node instanceof YTreeEntityNode) {
            consumer.onEntity();
        } else if (node instanceof YTreeIntegerNode) {
            YTreeIntegerNode integerNode = node.integerNode();
            if (integerNode.isSigned()) {
                consumer.onInteger(integerNode.getLong());
            } else {
                consumer.onUnsignedInteger(integerNode.longValue());
            }
        } else if (node instanceof YTreeStringNode) {
            if (stringAsBytes) {
                byte[] bytes = node.bytesValue();
                consumer.onString(bytes, 0, bytes.length);
            } else {
                consumer.onString(node.stringValue());
            }
        } else if (node instanceof YTreeListNode) {
            consumer.onBeginList();
            for (YTreeNode child : node.listNode()) {
                consumer.onListItem();
                walk(child, consumer, stringAsBytes, sortKeys);
            }
            consumer.onEndList();
        } else if (node instanceof YTreeMapNode) {
            consumer.onBeginMap();
            for (Map.Entry<String, YTreeNode> entry : walkEntries.apply(node.mapNode())) {
                consumer.onKeyedItem(entry.getKey());
                walk(entry.getValue(), consumer, stringAsBytes, sortKeys);
            }
            consumer.onEndMap();
        } else {
            throw new IllegalArgumentException("Unknown node " + node);
        }
    }

    private static void mergeMaps(Map<String, YTreeNode> m1, Map<String, YTreeNode> m2, Set<String> keys,
                                  YsonConsumer consumer, boolean stringAsBytes) {
        for (String key : keys) {
            YTreeNode n1 = m1.get(key);
            YTreeNode n2 = m2.get(key);
            consumer.onKeyedItem(key);
            if (n1 != null && n2 != null) {
                merge(n1, n2, consumer, stringAsBytes);
            } else if (n1 != null) {
                walk(n1, consumer, stringAsBytes);
            } else if (n2 != null) {
                walk(n2, consumer, stringAsBytes);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    public static void merge(YTreeNode node1, YTreeNode node2, YsonConsumer consumer, boolean stringAsBytes) {
        Map<String, YTreeNode> attributes1 = node1.getAttributes();
        Map<String, YTreeNode> attributes2 = node2.getAttributes();

        Set<String> attributeKeys = attributes1.keySet();
        attributeKeys.addAll(attributes2.keySet());
        if (!attributeKeys.isEmpty()) {
            consumer.onBeginAttributes();
            mergeMaps(attributes1, attributes2, attributeKeys, consumer, stringAsBytes);
            consumer.onEndAttributes();
        }

        if (node1 instanceof YTreeListNode && node2 instanceof YTreeListNode) {
            consumer.onBeginList();
            for (YTreeNode child : node1.listNode()) {
                consumer.onListItem();
                walk(child, consumer, stringAsBytes);
            }
            for (YTreeNode child : node2.listNode()) {
                consumer.onListItem();
                walk(child, consumer, stringAsBytes);
            }
            consumer.onEndList();
        } else if (node1 instanceof YTreeMapNode && node2 instanceof YTreeMapNode) {
            Set<String> keys = new HashSet<>(node1.mapNode().keys());
            keys.addAll(node2.mapNode().keys());
            consumer.onBeginMap();
            mergeMaps(node1.mapNode().asMap(), node2.mapNode().asMap(), keys, consumer, stringAsBytes);
            consumer.onEndMap();
        } else {
            node2.clearAttributes();
            walk(node2, consumer, stringAsBytes);
        }
    }
}
