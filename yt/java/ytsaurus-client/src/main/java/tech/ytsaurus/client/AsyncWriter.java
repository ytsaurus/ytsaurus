package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncWriter<T> {
    /**
     * Writes next batch of data
     *
     * @return future that is set once writer is ready to accept more data
     * @throws RuntimeException if writer is not ready to write data (future returned by previous write is not
     * completed yet).
     */
    CompletableFuture<Void> write(List<T> rows);

    /**
     * Completes write session and closes the writer
     */
    CompletableFuture<?> finish();

    /**
     * Starts a process of cancellation
     */
    void cancel();
}
