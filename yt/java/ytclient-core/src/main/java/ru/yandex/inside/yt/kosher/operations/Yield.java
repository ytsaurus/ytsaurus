package ru.yandex.inside.yt.kosher.operations;

import java.io.Closeable;

/**
 * @author sankear
 */
public interface Yield<T> extends Closeable {

    default void yield(T value) {
        this.yield(0, value);
    }

    void yield(int index, T value);

}
