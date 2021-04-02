package ru.yandex.yt.ytclient.proxy;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.misc.ScheduledSerializedExecutorService;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

/**
 * Client that provides compound commands over YT (e.g. mount table and wait all tablets are mounted).
 */
public abstract class CompoundClient extends ApiServiceClient implements Closeable {
    private final ScheduledExecutorService executorService;

    public CompoundClient(ScheduledExecutorService executorService, RpcOptions options) {
        super(options);
        this.executorService = executorService;
    }

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
    public <T> CompletableFuture<T> retryWithTabletTransaction(
            Function<ApiServiceTransaction, CompletableFuture<T>> action,
            ExecutorService executor,
            RetryPolicy retryPolicy
    ) {
        TabletTransactionRetrier<T> tabletTransactionRetrier = new TabletTransactionRetrier<>(
                this,
                executorService,
                action,
                executor,
                retryPolicy,
                rpcOptions
        );
        return tabletTransactionRetrier.run();
    }

    public CompletableFuture<Void> mountTableAndWaitTablets(MountTable req) {
        return mountTable(req).thenCompose(res -> waitTabletState(req.getPath(), "mounted"));
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted) {
        return mountTable(path, cellId, freeze, waitMounted, null);
    }

    /**
     * @param requestTimeout applies only to request itself and does NOT apply to waiting for tablets to be mounted
     */
    public CompletableFuture<Void> mountTable(
            String path,
            GUID cellId,
            boolean freeze,
            boolean waitMounted,
            @Nullable Duration requestTimeout
    ) {
        MountTable req = new MountTable(path);
        if (cellId != null) {
            req.setCellId(cellId);
        }
        req.setFreeze(freeze);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        if (waitMounted) {
            return mountTableAndWaitTablets(req);
        } else {
            return mountTable(req);
        }
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze) {
        return mountTable(path, cellId, freeze, null);
    }

    public CompletableFuture<Void> mountTable(
            String path,
            GUID cellId,
            boolean freeze,
            @Nullable Duration requestTimeout
    ) {
        return mountTable(path, cellId, freeze, false, requestTimeout);
    }

    public CompletableFuture<Void> mountTable(String path) {
        return mountTable(path, null);
    }

    public CompletableFuture<Void> mountTable(String path, @Nullable Duration requestTimeout) {
        return mountTable(path, null, false, requestTimeout);
    }

    public CompletableFuture<Void> mountTable(String path, boolean freeze) {
        return mountTable(path, freeze, null);
    }

    public CompletableFuture<Void> mountTable(String path, boolean freeze, @Nullable Duration requestTimeout) {
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
    public CompletableFuture<Void> unmountTable(String path, boolean force) {
        return unmountTable(path, force, null, false);
    }

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see ApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     */
    public CompletableFuture<Void> unmountTableAndWaitTablets(UnmountTable req) {
        String path = req.getPath();
        return unmountTable(req).thenCompose(rsp -> waitTabletState(path, "unmounted"));
    }

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see UnmountTable
     * @see ApiServiceClient#unmountTable(UnmountTable)
     */
    public CompletableFuture<Void> unmountTableAndWaitTablets(String path) {
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
    public CompletableFuture<Void> unmountTable(String path, boolean force, @Nullable Duration requestTimeout,
                                                boolean waitUnmounted) {
        UnmountTable req = new UnmountTable(path);
        req.setForce(force);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        if (waitUnmounted) {
            return unmountTableAndWaitTablets(req);
        } else {
            return unmountTable(req);
        }
    }

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
    public CompletableFuture<Void> unmountTable(String path) {
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
    public CompletableFuture<Void> unmountTable(String path, boolean force, boolean waitUnmounted) {
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
    public CompletableFuture<Void> unmountTable(String path, @Nullable Duration requestTimeout) {
        return unmountTable(path, false, requestTimeout, false);
    }

    private void runTabletsStateChecker(String tablePath, CompletableFuture<Void> futureToComplete, String state) {
        getNode(tablePath + "/@tablet_state").thenAccept(tabletState -> {
            if (tabletState.stringValue().equals(state)) {
                futureToComplete.complete(null);
            } else {
                executorService.schedule(
                        () -> runTabletsStateChecker(tablePath, futureToComplete, state),
                        1,
                        TimeUnit.SECONDS);
            }
        }).exceptionally(e -> {
            futureToComplete.completeExceptionally(e);
            return null;
        });
    }

    private CompletableFuture<Void> waitTabletState(String tablePath, String targetState) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        runTabletsStateChecker(tablePath, result, targetState);
        return result;
    }
}

@NonNullFields
@NonNullApi
class TabletTransactionRetrier<T> {
    private static final Logger logger = LoggerFactory.getLogger(TabletTransactionRetrier.class);

    private final ApiServiceClient client;
    private final ScheduledExecutorService safeExecutor;
    private final ExecutorService userExecutor;
    private final Function<ApiServiceTransaction, CompletableFuture<T>> action;
    private final RetryPolicy retryPolicy;
    private final RpcOptions rpcOptions;
    private final CompletableFuture<T> result = new CompletableFuture<>();

    private Future<?> nextAttempt = new CompletableFuture<Void>();
    private int attemptIndex = 0;

    TabletTransactionRetrier(
            ApiServiceClient client,
            ScheduledExecutorService executor,
            Function<ApiServiceTransaction, CompletableFuture<T>> action,
            ExecutorService userExecutor,
            RetryPolicy retryPolicy,
            RpcOptions rpcOptions
    ) {
        this.client = client;
        this.safeExecutor = new ScheduledSerializedExecutorService(executor);
        this.action = action;
        this.userExecutor = userExecutor;
        this.retryPolicy = retryPolicy;
        this.rpcOptions = rpcOptions;
    }

    CompletableFuture<T> run() {
        nextAttempt = safeExecutor.submit(this::runAttemptUnsafe);
        result.whenCompleteAsync((res, error) -> nextAttempt.cancel(false), safeExecutor);
        return result;
    }

    void runAttemptUnsafe() {
        if (result.isDone()) {
            return;
        }
        retryPolicy.onNewAttempt();
        attemptIndex += 1;
        logger.debug("Starting attempt {} of {}",
                attemptIndex,
                retryPolicy.getTotalRetryCountDescription());

        client.startTransaction(StartTransaction.tablet())
                .thenComposeAsync(
                        tx -> action.apply(tx)
                                .thenCompose(res -> {
                                    if (tx.isActive()) {
                                        return tx.commit().thenApply((r) -> res);
                                    } else {
                                        tx.close();
                                        return tx.getTransactionCompleteFuture().handle((r, e) -> res);
                                    }
                                })
                                .whenComplete((res, err) -> {
                                    if (err != null) {
                                        tx.close();
                                    }
                                }),
                        userExecutor
                )
                .whenCompleteAsync((res, error) -> {
                    if (error == null) {
                        result.complete(res);
                        return;
                    }
                    Optional<Duration> backoff = retryPolicy.getBackoffDuration(error, rpcOptions);
                    if (backoff.isPresent()) {
                        safeExecutor.schedule(
                                this::runAttemptUnsafe,
                                backoff.get().toNanos(),
                                TimeUnit.NANOSECONDS);
                    } else {
                        result.completeExceptionally(error);
                    }
                }, safeExecutor);
    }
}
