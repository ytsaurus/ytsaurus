package ru.yandex.yt.ytclient.ytree;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

/**
 * Любое значение, которое можно конвертировать в YTreeNode
 */
public interface YTreeConvertible {
    YTreeNode toYTree();
}
