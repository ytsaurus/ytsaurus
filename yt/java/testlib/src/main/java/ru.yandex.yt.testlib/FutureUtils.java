package ru.yandex.yt.testlib;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Miscellaneous stuff to work with futures.
 */
public class FutureUtils {
    /**
     * Wait until future is completed successfully or erroneously for specified amount of time.
     *
     * @param future future to wait
     * @param timeoutMs timeout in milliseconds
     *
     * @throws RuntimeException if wait time is exceeded.
     */
    static public void waitFuture(CompletableFuture<?> future, long timeoutMs) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ignored) {
        } catch (InterruptedException | TimeoutException error) {
            throw new RuntimeException(error);
        }
    }

    /**
     * Wait until future is completed successfully for specified amount of time.
     *
     * @param future future to wait
     * @param timeoutMs timeout in milliseconds
     *
     * @throws RuntimeException if wait time is exceeded or future is completed with error.
     */
    static public void waitOkResult(CompletableFuture<?> future, long timeoutMs) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException error) {
            throw new RuntimeException(error);
        }
    }

    /**
     * Get the error that the given future was completed with.
     *
     * @throws RuntimeException if future is not completed or it is completed successfully.
     */
    public static Throwable getError(CompletableFuture<?> future) {
        if (!future.isDone()) {
            throw new RuntimeException("Future is not completed.");
        }

        try {
            future.getNow(null);
            throw new RuntimeException("Future is not completed exceptionally.");
        } catch (CancellationException | CompletionException error) {
            return error;
        }
    }
}
