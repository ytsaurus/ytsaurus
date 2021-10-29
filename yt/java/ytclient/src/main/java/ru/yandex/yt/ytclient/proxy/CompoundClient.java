package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;

public interface CompoundClient extends ApiServiceClient, Closeable {
    /**
     * Retry specified action inside tablet transaction.
     * <p>
     * This method creates tablet transaction, runs specified action and then commits transaction.
     * If error happens retryPolicy is invoked to determine if there is a need for retry and if there is such
     * need process repeats.
     * <p>
     * Retries are performed until retry policy says to stop or action is successfully executed
     *
     * @param action action to be retried; it should not call commit; action might be called multiple times.
     * @param executor executor that will run user action
     * @param retryPolicy retry policy that determines which error should be retried
     * @return future that contains:
     * <ul>
     *     <li>result of action if it was executed successfully</li>
     *     <li>most recent error if all retries have failed</li>
     * </ul>
     */
    <T> CompletableFuture<T> retryWithTabletTransaction(
            Function<ApiServiceTransaction, CompletableFuture<T>> action,
            ExecutorService executor,
            RetryPolicy retryPolicy
    );

    CompletableFuture<Void> mountTableAndWaitTablets(MountTable req);

    /**
     * @param requestTimeout applies only to request itself and does NOT apply to waiting for tablets to be mounted
     */
    CompletableFuture<Void> mountTable(
            String path,
            GUID cellId,
            boolean freeze,
            boolean waitMounted,
            @Nullable Duration requestTimeout
    );

    default CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted) {
        return mountTable(path, cellId, freeze, waitMounted, null);
    }

    default CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze) {
        return mountTable(path, cellId, freeze, null);
    }

    default CompletableFuture<Void> mountTable(
            String path,
            GUID cellId,
            boolean freeze,
            @Nullable Duration requestTimeout
    ) {
        return mountTable(path, cellId, freeze, false, requestTimeout);
    }

    default CompletableFuture<Void> mountTable(String path) {
        return mountTable(path, null);
    }

    default CompletableFuture<Void> mountTable(String path, @Nullable Duration requestTimeout) {
        return mountTable(path, null, false, requestTimeout);
    }

    default CompletableFuture<Void> mountTable(String path, boolean freeze) {
        return mountTable(path, freeze, null);
    }

    default CompletableFuture<Void> mountTable(String path, boolean freeze, @Nullable Duration requestTimeout) {
        return mountTable(path, null, freeze, requestTimeout);
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)} or {@link #unmountTable(String)}.
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(String path, boolean force) {
        return unmountTable(path, force, null, false);
    }

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see ApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     */
    CompletableFuture<Void> unmountTableAndWaitTablets(UnmountTable req);

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see UnmountTable
     * @see ApiServiceClient#unmountTable(UnmountTable)
     */
    default CompletableFuture<Void> unmountTableAndWaitTablets(String path) {
        return unmountTableAndWaitTablets(new UnmountTable(path));
    }

    /**
     * Unmount table.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)}
     * or {@link #unmountTableAndWaitTablets(UnmountTable)} .
     */
    @Deprecated
    CompletableFuture<Void> unmountTable(String path, boolean force, @Nullable Duration requestTimeout,
                                         boolean waitUnmounted);

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * @see ApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     * @see #unmountTableAndWaitTablets(UnmountTable)
     * @see #unmountTableAndWaitTablets(String)
     */
    default CompletableFuture<Void> unmountTable(String path) {
        return unmountTable(new UnmountTable(path));
    }

    /**
     * Unmount table.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated Prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)}
     * or {@link #unmountTableAndWaitTablets(UnmountTable)} .
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(String path, boolean force, boolean waitUnmounted) {
        return unmountTable(path, force, null, waitUnmounted);
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)} or {@link #unmountTable(String)}.
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(String path, @Nullable Duration requestTimeout) {
        return unmountTable(path, false, requestTimeout, false);
    }
}
