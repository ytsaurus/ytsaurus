package tech.ytsaurus.client.misc;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

/**
 * An executor than ensurers serial execution of submitted tasks
 * i.e. no two tasks submitted to it will be executed simultaneously.
 */
public class SerializedExecutorService implements ExecutorService {
    private final ExecutorService underlying;
    private final AtomicBoolean lock = new AtomicBoolean();
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();

    public SerializedExecutorService(@Nonnull ExecutorService underlying) {
        this.underlying = underlying;
    }

    @Override
    public void shutdown() {
        underlying.shutdown();
    }

    @Nonnull
    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> result = underlying.shutdownNow();
        result.addAll(taskQueue);
        return result;
    }

    @Override
    public boolean isShutdown() {
        return underlying.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return underlying.isTerminated();
    }

    @Override
    public boolean awaitTermination(long l, @Nonnull TimeUnit timeUnit) throws InterruptedException {
        return underlying.awaitTermination(l, timeUnit);
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> submit(@Nonnull Callable<T> callable) {
        CompletableFuture<T> result = new CompletableFuture<>();
        execute(() -> {
            if (!result.isDone()) {
                try {
                    result.complete(callable.call());
                } catch (Throwable exception) {
                    result.completeExceptionally(exception);
                }
            }
        });
        return result;
    }

    @Nonnull
    @Override
    public <T> CompletableFuture<T> submit(@Nonnull Runnable runnable, T t) {
        return submit(() -> {
            runnable.run();
            return t;
        });
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> submit(@Nonnull Runnable runnable) {
        return submit(() -> {
            runnable.run();
            return null;
        });
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> collection) {
        throw new RuntimeException("Not implemented yet");
    }

    @Nonnull
    @Override
    public <T> List<Future<T>> invokeAll(
            @Nonnull Collection<? extends Callable<T>> collection,
            long l,
            @Nonnull TimeUnit timeUnit
    ) {
        throw new RuntimeException("Not implemented yet");
    }

    @Nonnull
    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> collection) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> collection, long l, @Nonnull TimeUnit timeUnit) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void execute(@Nonnull Runnable runnable) {
        taskQueue.add(runnable);
        trySchedule();
    }

    private void trySchedule() {
        if (lock.compareAndSet(false, true)) {
            underlying.execute(() -> {
                Runnable r = taskQueue.poll();
                if (r != null) {
                    r.run();
                }
                lock.set(false);
                if (!taskQueue.isEmpty()) {
                    trySchedule();
                }
            });
        }
    }
}
