package tech.ytsaurus.client;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.nio.NioEventLoopGroup;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.request.AbortJob;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.AbortTransaction;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.AdvanceConsumer;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.AlterTableReplica;
import tech.ytsaurus.client.request.BuildSnapshot;
import tech.ytsaurus.client.request.CheckClusterLiveness;
import tech.ytsaurus.client.request.CheckPermission;
import tech.ytsaurus.client.request.CommitTransaction;
import tech.ytsaurus.client.request.CompleteOperation;
import tech.ytsaurus.client.request.ConcatenateNodes;
import tech.ytsaurus.client.request.CopyNode;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.CreateObject;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.GcCollect;
import tech.ytsaurus.client.request.GenerateTimestamps;
import tech.ytsaurus.client.request.GetFileFromCache;
import tech.ytsaurus.client.request.GetFileFromCacheResult;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.GetJob;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetJobStderrResult;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.GetTablePivotKeys;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.LinkNode;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.client.request.ListJobsResult;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.LockNodeResult;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.client.request.MapReduceOperation;
import tech.ytsaurus.client.request.MergeOperation;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.MoveNode;
import tech.ytsaurus.client.request.MultiTablePartition;
import tech.ytsaurus.client.request.PartitionTables;
import tech.ytsaurus.client.request.PingTransaction;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.request.PutFileToCache;
import tech.ytsaurus.client.request.PutFileToCacheResult;
import tech.ytsaurus.client.request.ReadFile;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.RegisterQueueConsumer;
import tech.ytsaurus.client.request.RemoteCopyOperation;
import tech.ytsaurus.client.request.RemountTable;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.ResumeOperation;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.request.SetNode;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.client.request.StartOperation;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TrimTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.rpcproxy.EAtomicity;
import tech.ytsaurus.rpcproxy.ETableReplicaMode;
import tech.ytsaurus.rpcproxy.TCheckPermissionResult;
import tech.ytsaurus.ysontree.YTreeNode;

public class MockYTsaurusClient implements BaseYTsaurusClient {
    private final Map<String, Deque<Callable<CompletableFuture<?>>>> mocks = new HashMap<>();
    private Map<String, Long> timesCalled = new HashMap<>();
    private final YTsaurusCluster cluster;
    private final ScheduledExecutorService executor = new NioEventLoopGroup(1);

    public MockYTsaurusClient(String clusterName) {
        this.cluster = new YTsaurusCluster(YTsaurusCluster.normalizeName(clusterName));
    }

    @Override
    public TransactionalClient getRootClient() {
        return this;
    }

    @Override
    public void close() {
    }

    public void mockMethod(String methodName, Callable<CompletableFuture<?>> callback) {
        synchronized (this) {
            if (!mocks.containsKey(methodName)) {
                mocks.put(methodName, new ArrayDeque<>());
            }

            mocks.get(methodName).add(callback);
        }
    }

    public void mockMethod(String methodName, Callable<CompletableFuture<?>> callback, long times) {
        for (long i = 0; i < times; ++i) {
            mockMethod(methodName, callback);
        }
    }

    public long getTimesCalled(String methodName) {
        synchronized (this) {
            return timesCalled.getOrDefault(methodName, 0L);
        }
    }

    public void flushTimesCalled(String methodName) {
        synchronized (this) {
            timesCalled.remove(methodName);
        }
    }

    public void flushTimesCalled() {
        synchronized (this) {
            timesCalled = new HashMap<>();
        }
    }

    public List<YTsaurusCluster> getClusters() {
        return List.of(cluster);
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("lookupRows");
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("lookupRows");
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return (CompletableFuture<VersionedRowset>) callMethod("versionedLookupRows");
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("selectRows");
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeRowSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("selectRows");
    }

    @Override
    public CompletableFuture<SelectRowsResult> selectRowsV2(
            SelectRowsRequest request
    ) {
        return (CompletableFuture<SelectRowsResult>) callMethod("selectRowsV2");
    }

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return (CompletableFuture<GUID>) callMethod("createNode");
    }

    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return (CompletableFuture<Void>) callMethod("removeNode");
    }

    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return (CompletableFuture<Void>) callMethod("setNode");
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return (CompletableFuture<YTreeNode>) callMethod("getNode");
    }

    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return (CompletableFuture<YTreeNode>) callMethod("listNode");
    }

    @Override
    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return (CompletableFuture<LockNodeResult>) callMethod("lockNode");
    }

    @Override
    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return (CompletableFuture<GUID>) callMethod("copyNode");
    }

    @Override
    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return (CompletableFuture<GUID>) callMethod("linkNode");
    }

    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return (CompletableFuture<GUID>) callMethod("modeNode");
    }

    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return (CompletableFuture<Boolean>) callMethod("existsNode");
    }

    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return (CompletableFuture<Void>) callMethod("concatenateNodes");
    }

    @Override
    public CompletableFuture<List<MultiTablePartition>> partitionTables(PartitionTables req) {
        return (CompletableFuture<List<MultiTablePartition>>) callMethod("partitionTables");
    }

    @Override
    public CompletableFuture<QueueRowset> pullConsumer(PullConsumer req) {
        return (CompletableFuture<QueueRowset>) callMethod("pullConsumer");
    }

    @Override
    public CompletableFuture<Void> advanceConsumer(AdvanceConsumer req) {
        return (CompletableFuture<Void>) callMethod("advanceConsumer");
    }

    @Override
    public CompletableFuture<Void> registerQueueConsumer(RegisterQueueConsumer req) {
        return (CompletableFuture<Void>) callMethod("registerQueueConsumer");
    }

    @Override
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        return null;
    }

    @Override
    public CompletableFuture<Void> pingTransaction(PingTransaction req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitTransaction(CommitTransaction req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(AbortTransaction req) {
        return null;
    }

    @Override
    public CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req) {
        return null;
    }

    @Override
    public CompletableFuture<GUID> createObject(CreateObject req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> checkClusterLiveness(CheckClusterLiveness req) {
        return null;
    }

    @Override
    public <T> CompletableFuture<Void> lookupRows(AbstractLookupRowsRequest<?, ?> request,
                                                  YTreeRowSerializer<T> serializer, ConsumerSource<T> consumer) {
        return null;
    }

    @Override
    public <T> CompletableFuture<Void> selectRows(
            SelectRowsRequest request, YTreeRowSerializer<T> serializer, ConsumerSource<T> consumer) {
        return null;
    }

    @Override
    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> request) {
        return null;
    }

    @Override
    public CompletableFuture<Long> buildSnapshot(BuildSnapshot req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> gcCollect(GcCollect req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> mountTable(MountTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> unmountTable(UnmountTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> remountTable(RemountTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> freezeTable(FreezeTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> unfreezeTable(UnfreezeTable req) {
        return null;
    }

    @Override
    public CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<TabletInfo>> getTabletInfos(GetTabletInfos req) {
        return (CompletableFuture<List<TabletInfo>>) callMethod("getTabletInfos");
    }

    @Override
    public CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> reshardTable(ReshardTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> trimTable(TrimTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> alterTable(AlterTable req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> alterTableReplica(
            GUID replicaId, boolean enabled, ETableReplicaMode mode, boolean preserveTimestamp, EAtomicity atomicity) {
        return null;
    }

    @Override
    public CompletableFuture<Void> alterTableReplica(AlterTableReplica req) {
        return null;
    }

    @Override
    public CompletableFuture<YTreeNode> getOperation(GetOperation req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> abortOperation(AbortOperation req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> completeOperation(CompleteOperation req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> suspendOperation(SuspendOperation req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> resumeOperation(ResumeOperation req) {
        return null;
    }

    @Override
    public CompletableFuture<YTreeNode> getJob(GetJob req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> abortJob(AbortJob req) {
        return null;
    }

    @Override
    public CompletableFuture<ListJobsResult> listJobs(ListJobs req) {
        return null;
    }

    @Override
    public CompletableFuture<GetJobStderrResult> getJobStderr(GetJobStderr req) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateOperationParameters(UpdateOperationParameters req) {
        return null;
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> reqr) {
        return (CompletableFuture<TableReader<T>>) callMethod("readTable");
    }

    @Override
    public <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req) {
        return (CompletableFuture<AsyncReader<T>>) callMethod("readTableV2");
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        return (CompletableFuture<TableWriter<T>>) callMethod("writeTable");
    }

    @Override
    public <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req) {
        return (CompletableFuture<AsyncWriter<T>>) callMethod("writeTableV2");
    }

    @Override
    public CompletableFuture<FileReader> readFile(ReadFile req) {
        return (CompletableFuture<FileReader>) callMethod("readFile");
    }

    @Override
    public CompletableFuture<FileWriter> writeFile(WriteFile req) {
        return (CompletableFuture<FileWriter>) callMethod("writeFile");
    }

    @Override
    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return (CompletableFuture<GUID>) callMethod("startOperation");
    }

    @Override
    public CompletableFuture<Operation> startMap(MapOperation req) {
        return (CompletableFuture<Operation>) callMethod("startMap");
    }

    @Override
    public CompletableFuture<Operation> startReduce(ReduceOperation req) {
        return (CompletableFuture<Operation>) callMethod("startReduce");
    }

    @Override
    public CompletableFuture<Operation> startSort(SortOperation req) {
        return (CompletableFuture<Operation>) callMethod("startSort");
    }

    @Override
    public CompletableFuture<Operation> startMapReduce(MapReduceOperation req) {
        return (CompletableFuture<Operation>) callMethod("startMapReduce");
    }

    @Override
    public CompletableFuture<Operation> startMerge(MergeOperation req) {
        return (CompletableFuture<Operation>) callMethod("startMerge");
    }

    @Override
    public CompletableFuture<Operation> startRemoteCopy(RemoteCopyOperation req) {
        return (CompletableFuture<Operation>) callMethod("startRemoteCopy");
    }

    @Override
    public Operation attachOperation(GUID operationId) {
        return null;
    }

    @Override
    public CompletableFuture<Operation> startVanilla(VanillaOperation req) {
        return (CompletableFuture<Operation>) callMethod("startVanilla");
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return (CompletableFuture<TCheckPermissionResult>) callMethod("checkPermission");
    }

    @Override
    public CompletableFuture<GetFileFromCacheResult> getFileFromCache(GetFileFromCache req) {
        return (CompletableFuture<GetFileFromCacheResult>) callMethod("getFileFromCache");
    }

    @Override
    public CompletableFuture<PutFileToCacheResult> putFileToCache(PutFileToCache req) {
        return (CompletableFuture<PutFileToCacheResult>) callMethod("putFileToCache");
    }

    private CompletableFuture<?> callMethod(String methodName) {
        synchronized (this) {
            timesCalled.put(methodName, Long.valueOf(timesCalled.getOrDefault(methodName, Long.valueOf(0)) + 1));

            if (!mocks.containsKey(methodName) || mocks.get(methodName).isEmpty()) {
                return CompletableFuture.failedFuture(new InternalError("Method " + methodName + " wasn't mocked"));
            }

            try {
                return mocks.get(methodName).pollFirst().call();
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        }
    }
}
