package ru.yandex.yt.ytclient.ytree;

/**
 * Любое значение, которое можно конвертировать в YTreeNode
 */
public interface YTreeConvertible {
    YTreeNode toYTree();
}
