package tech.ytsaurus.core.operations;

import java.io.Closeable;

public interface Yield<T> extends Closeable {

    default void yield(T value) {
        this.yield(0, value);
    }

    void yield(int index, T value);

}
