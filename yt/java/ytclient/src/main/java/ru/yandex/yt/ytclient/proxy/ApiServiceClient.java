package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.request.AbortJob;
import ru.yandex.yt.ytclient.request.AbortOperation;
import ru.yandex.yt.ytclient.request.AbortTransaction;
import ru.yandex.yt.ytclient.request.AlterTable;
import ru.yandex.yt.ytclient.request.AlterTableReplica;
import ru.yandex.yt.ytclient.request.BuildSnapshot;
import ru.yandex.yt.ytclient.request.CheckClusterLiveness;
import ru.yandex.yt.ytclient.request.CommitTransaction;
import ru.yandex.yt.ytclient.request.CreateObject;
import ru.yandex.yt.ytclient.request.FreezeTable;
import ru.yandex.yt.ytclient.request.GcCollect;
import ru.yandex.yt.ytclient.request.GenerateTimestamps;
import ru.yandex.yt.ytclient.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.request.GetJob;
import ru.yandex.yt.ytclient.request.GetJobStderr;
import ru.yandex.yt.ytclient.request.GetJobStderrResult;
import ru.yandex.yt.ytclient.request.GetOperation;
import ru.yandex.yt.ytclient.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.request.GetTabletInfos;
import ru.yandex.yt.ytclient.request.ListJobs;
import ru.yandex.yt.ytclient.request.ListJobsResult;
import ru.yandex.yt.ytclient.request.LookupRowsRequest;
import ru.yandex.yt.ytclient.request.MappedLookupRowsRequest;
import ru.yandex.yt.ytclient.request.MountTable;
import ru.yandex.yt.ytclient.request.PingTransaction;
import ru.yandex.yt.ytclient.request.RemountTable;
import ru.yandex.yt.ytclient.request.ReshardTable;
import ru.yandex.yt.ytclient.request.ResumeOperation;
import ru.yandex.yt.ytclient.request.StartTransaction;
import ru.yandex.yt.ytclient.request.SuspendOperation;
import ru.yandex.yt.ytclient.request.TabletInfo;
import ru.yandex.yt.ytclient.request.TrimTable;
import ru.yandex.yt.ytclient.request.UnfreezeTable;
import ru.yandex.yt.ytclient.request.UnmountTable;
import ru.yandex.yt.ytclient.request.UpdateOperationParameters;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public interface ApiServiceClient extends TransactionalClient {
    CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction);

    /**
     * @deprected prefer to use {@link #startTransaction(StartTransaction)}
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
            LookupRowsRequest request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    <T> CompletableFuture<Void> lookupRows(
            MappedLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    <T> CompletableFuture<Void> versionedLookupRows(
            LookupRowsRequest request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

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

    default CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(query, null);
    }

    default CompletableFuture<UnversionedRowset> selectRows(String query, @Nullable Duration requestTimeout) {
        return selectRows(SelectRowsRequest.of(query).setTimeout(requestTimeout));
    }

    CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request);

    CompletableFuture<Long> buildSnapshot(BuildSnapshot req);

    CompletableFuture<Void> gcCollect(GcCollect req);

    default CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    CompletableFuture<Void> mountTable(MountTable req);

    /**
     * @deprected prefer to use {@link #mountTable(MountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> mountTable(MountTable.BuilderBase<?, MountTable> req) {
        return mountTable(req.build());
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see CompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    CompletableFuture<Void> unmountTable(UnmountTable req);

    /**
     * @deprected prefer to use {@link #unmountTable(UnmountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(UnmountTable.BuilderBase<?, UnmountTable> req) {
        return unmountTable(req.build());
    }

    default CompletableFuture<Void> remountTable(String path) {
        return remountTable(RemountTable.builder().setPath(path).build());
    }

    CompletableFuture<Void> remountTable(RemountTable req);

    /**
     * @deprected prefer to use {@link #remountTable(RemountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> remountTable(RemountTable.BuilderBase<?, RemountTable> req) {
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
     * @deprected prefer to use {@link #freezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> freezeTable(FreezeTable.BuilderBase<?, FreezeTable> req) {
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
     * @deprected prefer to use {@link #unfreezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(FreezeTable.BuilderBase<?, FreezeTable> req) {
        return unfreezeTable(req.build());
    }

    CompletableFuture<Void> unfreezeTable(UnfreezeTable req);

    /**
     * @deprected prefer to use {@link #unfreezeTable(UnfreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(UnfreezeTable.BuilderBase<?, UnfreezeTable> req) {
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
     * @deprected prefer to use {@link #reshardTable(ReshardTable)}
     */
    @Deprecated
    default CompletableFuture<Void> reshardTable(ReshardTable.BuilderBase<?, ReshardTable> req) {
        return reshardTable(req.build());
    }

    default CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        TrimTable req = new TrimTable(path, tableIndex, trimmedRowCount);
        return trimTable(req);
    }

    CompletableFuture<Void> trimTable(TrimTable req);

    CompletableFuture<Void> alterTable(AlterTable req);

    /**
     * @deprected prefer to use {@link #alterTable(AlterTable)}
     */
    @Deprecated
    default CompletableFuture<Void> alterTable(AlterTable.BuilderBase<?, AlterTable> req) {
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
     * @deprected prefer to use {@link #getOperation(GetOperation)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getOperation(GetOperation.BuilderBase<?, GetOperation> req) {
        return getOperation(req.build());
    }

    CompletableFuture<Void> abortOperation(AbortOperation req);

    CompletableFuture<Void> suspendOperation(SuspendOperation req);

    /**
     * @deprected prefer to use {@link #suspendOperation(SuspendOperation)}
     */
    @Deprecated
    default CompletableFuture<Void> suspendOperation(SuspendOperation.BuilderBase<?, SuspendOperation> req) {
        return suspendOperation(req.build());
    }

    CompletableFuture<Void> resumeOperation(ResumeOperation req);

    /**
     * @deprected prefer to use {@link #resumeOperation(ResumeOperation)}
     */
    @Deprecated
    default CompletableFuture<Void> resumeOperation(ResumeOperation.BuilderBase<?, ResumeOperation> req) {
        return resumeOperation(req.build());
    }

    CompletableFuture<YTreeNode> getJob(GetJob req);

    /**
     * @deprected prefer to use {@link #getJob(GetJob)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getJob(GetJob.BuilderBase<?, GetJob> req) {
        return getJob(req.build());
    }

    CompletableFuture<Void> abortJob(AbortJob req);

    /**
     * @deprected prefer to use {@link #abortJob(AbortJob)}
     */
    @Deprecated
    default CompletableFuture<Void> abortJob(AbortJob.BuilderBase<?, AbortJob> req) {
        return abortJob(req.build());
    }

    CompletableFuture<ListJobsResult> listJobs(ListJobs req);

    CompletableFuture<GetJobStderrResult> getJobStderr(GetJobStderr req);

    CompletableFuture<Void> updateOperationParameters(UpdateOperationParameters req);

    /**
     * @deprected prefer to use {@link #updateOperationParameters(UpdateOperationParameters)}
     */
    @Deprecated
    default CompletableFuture<Void> updateOperationParameters(
            UpdateOperationParameters.BuilderBase<?, UpdateOperationParameters> req) {
        return updateOperationParameters(req.build());
    }
}
