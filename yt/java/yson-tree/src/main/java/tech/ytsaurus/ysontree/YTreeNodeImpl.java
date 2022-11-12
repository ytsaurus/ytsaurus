package tech.ytsaurus.ysontree;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nullable;

public abstract class YTreeNodeImpl implements YTreeNode {

    private Map<String, YTreeNode> attributes;

    public YTreeNodeImpl(@Nullable Map<String, YTreeNode> attributes) {
        this.attributes = attributes == null ? null : new HashMap<>(attributes);
    }

    @Override
    public Map<String, YTreeNode> getAttributes() {
        init();
        return attributes;
    }

    @Override
    public void clearAttributes() {
        if (attributes != null) {
            attributes.clear();
        }
    }

    @Override
    public boolean containsAttributes() {
        return (attributes != null && !attributes.isEmpty());
    }

    @Override
    public boolean containsAttribute(String key) {
        return (attributes != null && attributes.containsKey(key));
    }

    @Override
    public Optional<YTreeNode> removeAttribute(String key) {
        if (attributes == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(attributes.remove(key));
    }

    @Override
    public Optional<YTreeNode> putAttribute(String key, YTreeNode value) {
        init();
        return Optional.ofNullable(attributes.put(key, value));
    }

    @Override
    public Optional<YTreeNode> getAttribute(String key) {
        if (attributes == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(attributes.get(key));
    }

    @Override
    public YTreeNode getAttributeOrThrow(String key) {
        if (attributes == null || !attributes.containsKey(key)) {
            throw new NoSuchElementException("Key " + key);
        }
        return attributes.get(key);
    }

    @Override
    public YTreeNode getAttributeOrThrow(String key, Supplier<String> createMessage) {
        if (attributes == null || !attributes.containsKey(key)) {
            throw new NoSuchElementException(createMessage.get());
        }
        return attributes.get(key);
    }

    @Override
    public String toString() {
        return YTreeTextSerializer.serialize(this);
    }

    @Override
    public byte[] toBinary() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(this, output);
        return output.toByteArray();
    }

    protected int hashCodeBase() {
        if (attributes == null) {
            return 0;
        }
        return attributes.hashCode();
    }

    protected boolean equalsBase(YTreeNode another0) {
        YTreeNodeImpl another = (YTreeNodeImpl) another0;
        if (attributes == null) {
            return another.attributes == null || another.attributes.isEmpty();
        }
        if (another.attributes == null) {
            return attributes.isEmpty();
        }
        return attributes.equals(another.attributes);
    }

    private void init() {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
    }
}
