package tech.ytsaurus.ysontree;

import java.util.Map;

import javax.annotation.Nullable;

public class YTreeEntityNodeImpl extends YTreeNodeImpl implements YTreeEntityNode {

    public YTreeEntityNodeImpl(@Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
    }

    @Override
    public int hashCode() {
        return hashCodeBase();
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeEntityNode)) {
            return false;
        }
        return equalsBase((YTreeEntityNode) another);
    }

}
