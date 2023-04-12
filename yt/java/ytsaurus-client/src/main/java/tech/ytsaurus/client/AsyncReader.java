package tech.ytsaurus.client;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public interface AsyncReader<T> extends Closeable {
    /**
     * Read all data pass it to supplied consumer.
     *
     * @return future that is completed once all data is read or error occurred
     */
    CompletableFuture<Void> acceptAllAsync(Consumer<? super T> consumer, Executor executor);

    /**
     * Low level interface: read next batch of data.
     * <p>
     * Once reader is exhausted returned future is set with null.
     */
    CompletableFuture<List<T>> next();
}
