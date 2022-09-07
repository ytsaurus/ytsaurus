package ru.yandex.inside.yt.kosher.ytree;

import javax.annotation.Nonnull;

/**
 * @author sankear
 */
public interface YTreeScalarNode<V> extends YTreeNode {
    @Nonnull
    V getBoxedValue();
}
