package tech.ytsaurus.client.misc;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

/**
 * An executor than ensurers serial execution of submitted tasks
 * i.e. no two tasks submitted to it will be executed simultaneously.
 * <p>
 * It also can schedule commands to run after a given delay, or to execute them periodically.
 *
 * @see tech.ytsaurus.client.misc.SerializedExecutorService
 * @see java.util.concurrent.ScheduledExecutorService
 */
public class ScheduledSerializedExecutorService extends SerializedExecutorService implements ScheduledExecutorService {
    private final ScheduledExecutorService underlying;

    public ScheduledSerializedExecutorService(@Nonnull ScheduledExecutorService underlying) {
        super(underlying);
        this.underlying = underlying;
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> schedule(@Nonnull Runnable runnable, long l, @Nonnull TimeUnit timeUnit) {
        CompletableFuture<Void> completable = new CompletableFuture<>();
        ScheduledFuture<?> scheduled = underlying.schedule(() -> {
            super.submit(runnable).handle((Void v, Throwable t) -> {
                if (t != null) {
                    completable.completeExceptionally(t);
                } else {
                    completable.complete(null);
                }
                return null;
            });
        }, l, timeUnit);
        return toScheduledFuture(scheduled, completable);
    }

    @Nonnull
    @Override
    public <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long l, @Nonnull TimeUnit timeUnit) {
        CompletableFuture<V> completable = new CompletableFuture<>();
        ScheduledFuture<Void> scheduled = underlying.schedule(() -> {
            super.submit(callable).handle((V v, Throwable t) -> {
                if (t != null) {
                    completable.completeExceptionally(t);
                } else {
                    completable.complete(v);
                }
                return null;
            });
            return null;
        }, l, timeUnit);

        return toScheduledFuture(scheduled, completable);
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            @Nonnull Runnable runnable,
            long l,
            long l1,
            @Nonnull TimeUnit timeUnit
    ) {
        return underlying.scheduleAtFixedRate(() -> execute(runnable), l, l1, timeUnit);
    }

    @Nonnull
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            @Nonnull Runnable runnable,
            long l,
            long l1,
            @Nonnull TimeUnit timeUnit
    ) {
        return underlying.scheduleWithFixedDelay(() -> execute(runnable), l, l1, timeUnit);
    }

    private <V> ScheduledFuture<V> toScheduledFuture(ScheduledFuture<?> scheduled, CompletableFuture<V> completable) {
        return new ScheduledFuture<V>() {
            @Override
            public long getDelay(@Nonnull TimeUnit timeUnit) {
                return scheduled.getDelay(timeUnit);
            }

            @Override
            public int compareTo(@Nonnull Delayed delayed) {
                return scheduled.compareTo(delayed);
            }

            @Override
            public boolean cancel(boolean b) {
                scheduled.cancel(b);
                return completable.cancel(b);
            }

            @Override
            public boolean isCancelled() {
                return completable.isCancelled();
            }

            @Override
            public boolean isDone() {
                return completable.isDone();
            }

            @Override
            public V get() throws InterruptedException, ExecutionException {
                return completable.get();
            }

            @Override
            public V get(
                    long l,
                    @Nonnull TimeUnit timeUnit
            ) throws InterruptedException, ExecutionException, TimeoutException {
                return completable.get(l, timeUnit);
            }
        };
    }
}
