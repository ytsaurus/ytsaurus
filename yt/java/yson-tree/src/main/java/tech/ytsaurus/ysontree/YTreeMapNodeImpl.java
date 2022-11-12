package tech.ytsaurus.ysontree;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

public class YTreeMapNodeImpl extends YTreeNodeImpl implements YTreeMapNode {

    private final Map<String, YTreeNode> map = new HashMap<>();

    public YTreeMapNodeImpl(Map<String, YTreeNode> data, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        map.putAll(data);
    }

    public YTreeMapNodeImpl(@Nullable Map<String, YTreeNode> attributes) {
        this(Collections.emptyMap(), attributes);
    }

    @Override
    public Map<String, YTreeNode> asMap() {
        return map;
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Optional<YTreeNode> remove(String key) {
        return Optional.ofNullable(map.remove(key));
    }

    @Override
    public Optional<YTreeNode> put(String key, YTreeNode value) {
        return Optional.ofNullable(map.put(key, value));
    }

    @Override
    public void putAll(Map<? extends String, ? extends YTreeNode> data) {
        map.putAll(data);
    }

    @Override
    public Iterator<Map.Entry<String, YTreeNode>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Set<String> keys() {
        return map.keySet();
    }

    @Override
    public Collection<YTreeNode> values() {
        return map.values();
    }

    @Override
    public Optional<YTreeNode> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + map.hashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeMapNode)) {
            return false;
        }
        YTreeMapNode node = (YTreeMapNode) another;
        return map.equals(node.asMap()) && equalsBase(node);
    }

}
