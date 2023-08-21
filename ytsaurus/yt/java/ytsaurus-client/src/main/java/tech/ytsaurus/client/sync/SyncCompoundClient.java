package tech.ytsaurus.client.sync;

import java.util.function.Function;

import tech.ytsaurus.client.RetryPolicy;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.UnmountTable;

interface SyncCompoundClient extends SyncApiServiceClient, AutoCloseable {
    /**
     * Retry specified action inside tablet transaction.
     * <p>
     * This method creates tablet transaction, runs specified action and then commits transaction.
     * If error happens retryPolicy is invoked to determine if there is a need for retry and if there is such
     * need process repeats.
     * <p>
     * Retries are performed until retry policy says to stop or action is successfully executed
     *
     * @param action      action to be retried; it should not call commit; action might be called multiple times.
     * @param retryPolicy retry policy that determines which error should be retried
     * @return result of action if it was executed successfully
     * @throws RuntimeException most recent error if all retries have failed
     */
    <T> T retryWithTabletTransaction(
            Function<SyncApiServiceTransaction, T> action,
            RetryPolicy retryPolicy
    );

    /**
     * Mount table and wait until all tablets become mounted.
     *
     * @see SyncApiServiceClient#mountTable(MountTable)
     * @see MountTable
     */
    void mountTableAndWaitTablets(MountTable req);

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see SyncApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     */
    void unmountTableAndWaitTablets(UnmountTable req);
}
