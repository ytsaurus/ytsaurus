package ru.yandex.yt.ytclient.ytree;

import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Любое значение, которое можно конвертировать в YTreeNode
 */
public interface YTreeConvertible {
    YTreeNode toYTree();
}
