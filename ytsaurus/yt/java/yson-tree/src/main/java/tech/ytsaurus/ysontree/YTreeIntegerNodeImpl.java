package tech.ytsaurus.ysontree;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class YTreeIntegerNodeImpl extends YTreeNodeImpl implements YTreeIntegerNode {

    private boolean signed;
    private long value;

    public YTreeIntegerNodeImpl(boolean signed, long value, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        this.signed = signed;
        this.value = value;
    }

    @Override
    public boolean isSigned() {
        return signed;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public long setLong(long newValue) {
        long ret = value;
        signed = true;
        value = newValue;
        return ret;
    }

    @Override
    public long setUnsignedLong(long newValue) {
        long ret = value;
        signed = false;
        value = newValue;
        return ret;
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + Long.hashCode(value);
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeIntegerNode)) {
            return false;
        }
        YTreeIntegerNode node = (YTreeIntegerNode) another;
        return value == node.getLong() && (value >= 0 || isSigned() == node.isSigned()) && equalsBase(node);
    }

    @Nonnull
    @Override
    public Long getBoxedValue() {
        return value;
    }

}
