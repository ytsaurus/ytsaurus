package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.misc.ScheduledSerializedExecutorService;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Client that provides compound commands over YT (e.g. mount table and wait all tablets are mounted).
 */
public abstract class CompoundClientImpl extends ApiServiceClientImpl implements CompoundClient {
    private final ScheduledExecutorService executorService;

    public CompoundClientImpl(
            ScheduledExecutorService executorService,
            YtClientConfiguration configuration,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        super(null, configuration, heavyExecutor, executorService, serializationResolver);
        this.executorService = executorService;
    }

    public CompoundClientImpl(
            RpcClient client,
            ScheduledExecutorService executorService,
            YtClientConfiguration configuration,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        super(client, configuration, heavyExecutor, executorService, serializationResolver);
        this.executorService = executorService;
    }

    public CompoundClientImpl(
            ScheduledExecutorService executorService,
            RpcOptions rpcOptions,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        this(
                executorService,
                YtClientConfiguration.builder().setRpcOptions(rpcOptions).build(),
                heavyExecutor,
                serializationResolver
        );
    }

    @Override
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

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            return CompletableFuture.completedFuture(
                    new RetryingTableWriterImpl<>(this, executorService, req, rpcOptions,
                            serializationResolver))
                    .thenCompose(writer -> writer.readyEvent().thenApply(unused -> writer));
        }

        return super.writeTable(req);
    }

    @Override
    public <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            return CompletableFuture.completedFuture(
                    new AsyncRetryingTableWriterImpl<>(
                            this, executorService, req, rpcOptions, serializationResolver));
        }
        return super.writeTableV2(req);
    }

    @Override
    public CompletableFuture<Void> mountTableAndWaitTablets(MountTable req) {
        return mountTable(req).thenCompose(res -> waitTabletState(req.getPath(), "mounted"));
    }

    @Override
    public CompletableFuture<Void> mountTable(
            String path,
            GUID cellId,
            boolean freeze,
            boolean waitMounted,
            @Nullable Duration requestTimeout
    ) {
        MountTable.Builder builder = MountTable.builder().setPath(path);
        if (cellId != null) {
            builder.setCellId(cellId);
        }
        builder.setFreeze(freeze);
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (waitMounted) {
            return mountTableAndWaitTablets(builder.build());
        } else {
            return mountTable(builder.build());
        }
    }

    @Override
    public CompletableFuture<Void> unmountTableAndWaitTablets(UnmountTable req) {
        String path = req.getPath();
        return unmountTable(req).thenCompose(rsp -> waitTabletState(path, "unmounted"));
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
