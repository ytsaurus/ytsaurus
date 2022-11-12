package tech.ytsaurus.ysontree;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public interface YTreeMapNode extends YTreeCompositeNode<Map.Entry<String, YTreeNode>> {

    Set<String> keys();

    Collection<YTreeNode> values();

    boolean containsKey(String key);

    Optional<YTreeNode> remove(String key);

    Optional<YTreeNode> put(String key, YTreeNode value);

    void putAll(Map<? extends String, ? extends YTreeNode> map);

    Optional<YTreeNode> get(String key);

    default YTreeNode getOrThrow(String key) {
        return getOrThrow(key, () -> "Key not found: " + key);
    }

    default YTreeNode getOrThrow(String key, Supplier<String> createMessage) {
        return get(key).orElseThrow(() -> new NoSuchElementException(createMessage.get()));
    }

    default YTreeNode getOrDefault(String key, Supplier<YTreeNode> defaultNode) {
        return get(key).orElseGet(defaultNode);
    }

    default String getString(String key) {
        return getOrThrow(key).stringValue();
    }

    default Optional<String> getStringO(String key) {
        return getFilterNull(key).map(YTreeNode::stringValue);
    }

    default byte[] getBytes(String key) {
        return getOrThrow(key).bytesValue();
    }

    default Optional<byte[]> getBytesO(String key) {
        return getFilterNull(key).map(YTreeNode::bytesValue);
    }

    default int getInt(String key) {
        return getOrThrow(key).intValue();
    }

    default Optional<Integer> getIntO(String key) {
        return getFilterNull(key).map(YTreeNode::intValue);
    }

    default long getLong(String key) {
        return getOrThrow(key).longValue();
    }

    default Optional<Long> getLongO(String key) {
        return getFilterNull(key).map(YTreeNode::longValue);
    }

    default boolean getBool(String key) {
        return getOrThrow(key).boolValue();
    }

    default Optional<Boolean> getBoolO(String key) {
        return getFilterNull(key).map(YTreeNode::boolValue);
    }

    default double getDouble(String key) {
        return getOrThrow(key).doubleValue();
    }

    default Optional<Double> getDoubleO(String key) {
        return getFilterNull(key).map(YTreeNode::doubleValue);
    }

    default YTreeListNode getList(String key) {
        return getOrThrow(key).listNode();
    }

    default Optional<YTreeListNode> getListO(String key) {
        return getFilterNull(key).map(YTreeNode::listNode);
    }

    default YTreeMapNode getMap(String key) {
        return getOrThrow(key).mapNode();
    }

    default Optional<YTreeMapNode> getMapO(String key) {
        return getFilterNull(key).map(YTreeNode::mapNode);
    }

    default Optional<YTreeNode> getFilterNull(String key) {
        return get(key).filter(node -> !node.isEntityNode());
    }

    default YTreeBuilder toMapBuilder() {
        YTreeBuilder builder = YTree.builder();
        Map<String, YTreeNode> attributes = getAttributes();
        if (!attributes.isEmpty()) {
            builder.beginAttributes();
            for (Map.Entry<String, YTreeNode> entry : attributes.entrySet()) {
                builder.key(entry.getKey()).value(entry.getValue());
            }
            builder.endAttributes();
        }
        builder.beginMap();
        for (Map.Entry<String, YTreeNode> entry : asMap().entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue());
        }
        return builder;
    }
}
