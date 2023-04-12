package tech.ytsaurus.client.sync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.RetryPolicy;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.UnmountTable;

/**
 * Synchronous client that provides compound commands over YT
 * (e.g. mount table and wait all tablets are mounted).
 */
abstract class SyncCompoundClientImpl
        extends SyncApiServiceClientImpl
        implements SyncCompoundClient {
    private final CompoundClient client;
    private final ExecutorService executorForUserTasks;

    SyncCompoundClientImpl(
            CompoundClient client
    ) {
        super(client);
        this.client = client;
        this.executorForUserTasks = Executors.newCachedThreadPool();
    }

    @Override
    public <T> T retryWithTabletTransaction(
            Function<SyncApiServiceTransaction, T> action,
            RetryPolicy retryPolicy
    ) {
        Function<ApiServiceTransaction, CompletableFuture<T>> asyncAction =
                transaction -> CompletableFuture.supplyAsync(
                        () -> action.apply(SyncApiServiceTransaction.wrap(transaction)),
                        executorForUserTasks
                );
        return client.retryWithTabletTransaction(asyncAction, executorForUserTasks, retryPolicy)
                .join();
    }

    @Override
    public void mountTableAndWaitTablets(MountTable req) {
        client.mountTableAndWaitTablets(req).join();
    }

    @Override
    public void unmountTableAndWaitTablets(UnmountTable req) {
        client.unmountTableAndWaitTablets(req).join();
    }
}
