package tech.ytsaurus.ysontree;

import javax.annotation.Nonnull;

public interface YTreeScalarNode<V> extends YTreeNode {
    @Nonnull
    V getBoxedValue();
}
