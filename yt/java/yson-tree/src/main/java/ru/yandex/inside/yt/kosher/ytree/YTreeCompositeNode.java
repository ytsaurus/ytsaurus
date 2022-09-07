package ru.yandex.inside.yt.kosher.ytree;

/**
 * @author sankear
 */
public interface YTreeCompositeNode<T> extends YTreeNode, Iterable<T> {

    default boolean isEmpty() {
        return size() == 0;
    }

    int size();

    void clear();

}
