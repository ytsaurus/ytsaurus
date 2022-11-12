package tech.ytsaurus.ysontree;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class YTreeBooleanNodeImpl extends YTreeNodeImpl implements YTreeBooleanNode {

    private boolean value;

    public YTreeBooleanNodeImpl(boolean value, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    @Override
    public boolean getValue() {
        return value;
    }

    @Override
    public boolean setValue(boolean newValue) {
        boolean ret = value;
        value = newValue;
        return ret;
    }

    @Nonnull
    @Override
    public Boolean getBoxedValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + Boolean.hashCode(value);
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeBooleanNode)) {
            return false;
        }
        YTreeBooleanNode node = (YTreeBooleanNode) another;
        return value == node.getValue() && equalsBase(node);
    }

}
