package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.AbortJob;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.AbortTransaction;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.AlterTableReplica;
import tech.ytsaurus.client.request.BuildSnapshot;
import tech.ytsaurus.client.request.CheckClusterLiveness;
import tech.ytsaurus.client.request.CommitTransaction;
import tech.ytsaurus.client.request.CreateObject;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.GcCollect;
import tech.ytsaurus.client.request.GenerateTimestamps;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.GetJob;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetJobStderrResult;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.GetTablePivotKeys;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.client.request.ListJobsResult;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PingTransaction;
import tech.ytsaurus.client.request.RemountTable;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.ResumeOperation;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TrimTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.EAtomicity;
import tech.ytsaurus.rpcproxy.ETableReplicaMode;
import tech.ytsaurus.ysontree.YTreeNode;

public interface ApiServiceClient extends TransactionalClient {
    CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction);

    /**
     * @deprecated prefer to use {@link #startTransaction(StartTransaction)}
     */
    @Deprecated
    default CompletableFuture<ApiServiceTransaction> startTransaction(
            StartTransaction.BuilderBase<?> startTransaction) {
        return startTransaction(startTransaction.build());
    }

    /**
     * @deprecated prefer to use {@link #startTransaction(StartTransaction)}
     */
    @Deprecated
    default CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        return startTransaction(options.toStartTransaction());
    }

    CompletableFuture<Void> pingTransaction(PingTransaction req);

    default CompletableFuture<Void> pingTransaction(GUID id) {
        return pingTransaction(new PingTransaction(id));
    }

    CompletableFuture<Void> commitTransaction(CommitTransaction req);

    default CompletableFuture<Void> commitTransaction(GUID id) {
        return commitTransaction(new CommitTransaction(id));
    }

    CompletableFuture<Void> abortTransaction(AbortTransaction req);

    default CompletableFuture<Void> abortTransaction(GUID id) {
        return abortTransaction(new AbortTransaction(id));
    }

    CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req);

    CompletableFuture<GUID> createObject(CreateObject req);

    CompletableFuture<Void> checkClusterLiveness(CheckClusterLiveness req);

    <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    default <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest.Builder<?, ?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return lookupRows(request.build(), serializer, consumer);
    }

    @Deprecated
    default CompletableFuture<UnversionedRowset> lookupRows(
            LookupRowsRequest.BuilderBase<?> request, YtTimestamp timestamp) {
        return lookupRows(request.setTimestamp(timestamp));
    }

    @Deprecated
    default CompletableFuture<VersionedRowset> versionedLookupRows(
            LookupRowsRequest.BuilderBase<?> request, YtTimestamp timestamp) {
        return versionedLookupRows(request.setTimestamp(timestamp));
    }

    CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> request);

    default CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest.Builder<?, ?> request) {
        return modifyRows(transactionId, (AbstractModifyRowsRequest<?, ?>) request.build());
    }

    CompletableFuture<Long> buildSnapshot(BuildSnapshot req);

    CompletableFuture<Void> gcCollect(GcCollect req);

    default CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    CompletableFuture<Void> mountTable(MountTable req);

    /**
     * @deprecated prefer to use {@link #mountTable(MountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> mountTable(MountTable.BuilderBase<?> req) {
        return mountTable(req.build());
    }

    /**
     * Unmount table.
     * <p>
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see CompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    CompletableFuture<Void> unmountTable(UnmountTable req);

    /**
     * @deprecated prefer to use {@link #unmountTable(UnmountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(UnmountTable.BuilderBase<?> req) {
        return unmountTable(req.build());
    }

    default CompletableFuture<Void> remountTable(String path) {
        return remountTable(RemountTable.builder().setPath(path).build());
    }

    CompletableFuture<Void> remountTable(RemountTable req);

    /**
     * @deprecated prefer to use {@link #remountTable(RemountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> remountTable(RemountTable.BuilderBase<?> req) {
        return remountTable(req.build());
    }

    default CompletableFuture<Void> freezeTable(String path) {
        return freezeTable(path, null);
    }

    default CompletableFuture<Void> freezeTable(String path, @Nullable Duration requestTimeout) {
        return freezeTable(FreezeTable.builder().setPath(path).setTimeout(requestTimeout).build());
    }

    CompletableFuture<Void> freezeTable(FreezeTable req);

    /**
     * @deprecated prefer to use {@link #freezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> freezeTable(FreezeTable.BuilderBase<?> req) {
        return freezeTable(req.build());
    }

    default CompletableFuture<Void> unfreezeTable(String path) {
        return unfreezeTable(path, null);
    }

    default CompletableFuture<Void> unfreezeTable(String path, @Nullable Duration requestTimeout) {
        return unfreezeTable(UnfreezeTable.builder().setPath(path).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<Void> unfreezeTable(FreezeTable req) {
        UnfreezeTable.Builder unfreezeReqBuilder = UnfreezeTable.builder().setPath(req.getPath());
        if (req.getTimeout().isPresent()) {
            unfreezeReqBuilder.setTimeout(req.getTimeout().get());
        }
        return unfreezeTable(unfreezeReqBuilder.build());
    }

    /**
     * @deprecated prefer to use {@link #unfreezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(FreezeTable.BuilderBase<?> req) {
        return unfreezeTable(req.build());
    }

    CompletableFuture<Void> unfreezeTable(UnfreezeTable req);

    /**
     * @deprecated prefer to use {@link #unfreezeTable(UnfreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(UnfreezeTable.BuilderBase<?> req) {
        return unfreezeTable(req.build());
    }

    CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp);

    default CompletableFuture<List<GUID>> getInSyncReplicas(
            String path,
            YtTimestamp timestamp,
            TableSchema schema,
            Iterable<? extends List<?>> keys
    ) {
        return getInSyncReplicas(new GetInSyncReplicas(path, schema, keys), timestamp);
    }

    CompletableFuture<List<TabletInfo>> getTabletInfos(GetTabletInfos req);

    default CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices) {
        return getTabletInfos(GetTabletInfos.builder().setPath(path).setTabletIndexes(tabletIndices).build());
    }

    CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req);

    default CompletableFuture<YtTimestamp> generateTimestamps(int count) {
        GenerateTimestamps req = new GenerateTimestamps(count);
        return generateTimestamps(req);
    }

    default CompletableFuture<YtTimestamp> generateTimestamps() {
        return generateTimestamps(1);
    }

    CompletableFuture<Void> reshardTable(ReshardTable req);

    /**
     * @deprecated prefer to use {@link #reshardTable(ReshardTable)}
     */
    @Deprecated
    default CompletableFuture<Void> reshardTable(ReshardTable.BuilderBase<?> req) {
        return reshardTable(req.build());
    }

    default CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        TrimTable req = new TrimTable(path, tableIndex, trimmedRowCount);
        return trimTable(req);
    }

    CompletableFuture<Void> trimTable(TrimTable req);

    CompletableFuture<Void> alterTable(AlterTable req);

    /**
     * @deprecated prefer to use {@link #alterTable(AlterTable)}
     */
    @Deprecated
    default CompletableFuture<Void> alterTable(AlterTable.BuilderBase<?> req) {
        return alterTable(req.build());
    }

    CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity
    );

    CompletableFuture<Void> alterTableReplica(AlterTableReplica req);

    CompletableFuture<YTreeNode> getOperation(GetOperation req);

    /**
     * @deprecated prefer to use {@link #getOperation(GetOperation)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getOperation(GetOperation.BuilderBase<?> req) {
        return getOperation(req.build());
    }

    /**
     * Request to abort operation.
     * <p>
     * Operation will be finished in erroneous aborted state.
     * <p>
     *
     * @see AbortOperation
     * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_job">
     * abort_job documentation
     * </a>
     */
    CompletableFuture<Void> abortOperation(AbortOperation req);

    CompletableFuture<Void> suspendOperation(SuspendOperation req);

    CompletableFuture<Void> resumeOperation(ResumeOperation req);

    /**
     * @deprecated prefer to use {@link #resumeOperation(ResumeOperation)}
     */
    @Deprecated
    default CompletableFuture<Void> resumeOperation(ResumeOperation.BuilderBase<?> req) {
        return resumeOperation(req.build());
    }

    CompletableFuture<YTreeNode> getJob(GetJob req);

    /**
     * @deprecated prefer to use {@link #getJob(GetJob)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getJob(GetJob.BuilderBase<?> req) {
        return getJob(req.build());
    }

    /**
     * Request to abort job.
     * <p>
     * Job will be aborted. In the future scheduler will restart this job.
     * <p>
     *
     * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_job">
     * abort_job documentation
     * </a>
     */
    CompletableFuture<Void> abortJob(AbortJob req);

    CompletableFuture<ListJobsResult> listJobs(ListJobs req);

    CompletableFuture<GetJobStderrResult> getJobStderr(GetJobStderr req);

    CompletableFuture<Void> updateOperationParameters(UpdateOperationParameters req);

    /**
     * @deprecated prefer to use {@link #updateOperationParameters(UpdateOperationParameters)}
     */
    @Deprecated
    default CompletableFuture<Void> updateOperationParameters(
            UpdateOperationParameters.BuilderBase<?> req) {
        return updateOperationParameters(req.build());
    }
}
