package tech.ytsaurus.client.sync;

import java.io.Flushable;
import java.util.function.Consumer;

public interface SyncTableWriter<T> extends Consumer<T>, Flushable, AutoCloseable {
    @Override
    void flush();

    @Override
    void close();

    /**
     * Starts a process of cancellation
     */
    void cancel();
}
