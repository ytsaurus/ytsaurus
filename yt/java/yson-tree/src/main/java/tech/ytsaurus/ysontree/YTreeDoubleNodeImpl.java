package tech.ytsaurus.ysontree;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author sankear
 */
public class YTreeDoubleNodeImpl extends YTreeNodeImpl implements YTreeDoubleNode {

    private double value;

    public YTreeDoubleNodeImpl(double value, @Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
        this.value = value;
    }

    @Override
    public double getValue() {
        return value;
    }

    @Override
    public double setValue(double newValue) {
        double ret = value;
        value = newValue;
        return ret;
    }

    @Nonnull
    @Override
    public Double getBoxedValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + Double.hashCode(value);
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeDoubleNode)) {
            return false;
        }
        YTreeDoubleNode node = (YTreeDoubleNode) another;
        return Double.doubleToLongBits(value) == Double.doubleToLongBits(node.getValue()) && equalsBase(node);
    }

}
