package tech.ytsaurus.testlib;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureUtils {
    private FutureUtils() {
    }

    public static void waitFuture(CompletableFuture<?> future, long timeoutMs) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ignored) {
        } catch (TimeoutException | InterruptedException var5) {
            throw new RuntimeException(var5);
        }

    }

    public static void waitOkResult(CompletableFuture<?> future, long timeoutMs) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException var4) {
            throw new RuntimeException(var4);
        }
    }

    public static Throwable getError(CompletableFuture<?> future) {
        if (!future.isDone()) {
            throw new RuntimeException("Future is not completed.");
        } else {
            try {
                future.getNow(null);
                throw new RuntimeException("Future is not completed exceptionally.");
            } catch (CompletionException | CancellationException e) {
                return e;
            }
        }
    }
}
