package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import io.netty.channel.nio.NioEventLoopGroup;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.operations.Operation;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.request.AbortJob;
import ru.yandex.yt.ytclient.proxy.request.AbortOperation;
import ru.yandex.yt.ytclient.proxy.request.AbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
import ru.yandex.yt.ytclient.proxy.request.AlterTableReplica;
import ru.yandex.yt.ytclient.proxy.request.BuildSnapshot;
import ru.yandex.yt.ytclient.proxy.request.CheckClusterLiveness;
import ru.yandex.yt.ytclient.proxy.request.CheckPermission;
import ru.yandex.yt.ytclient.proxy.request.CommitTransaction;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.CreateObject;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.GcCollect;
import ru.yandex.yt.ytclient.proxy.request.GenerateTimestamps;
import ru.yandex.yt.ytclient.proxy.request.GetFileFromCache;
import ru.yandex.yt.ytclient.proxy.request.GetFileFromCacheResult;
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetJob;
import ru.yandex.yt.ytclient.proxy.request.GetJobStderr;
import ru.yandex.yt.ytclient.proxy.request.GetJobStderrResult;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.GetOperation;
import ru.yandex.yt.ytclient.proxy.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.GetTabletInfos;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListJobs;
import ru.yandex.yt.ytclient.proxy.request.ListJobsResult;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MapOperation;
import ru.yandex.yt.ytclient.proxy.request.MapReduceOperation;
import ru.yandex.yt.ytclient.proxy.request.MergeOperation;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.PingTransaction;
import ru.yandex.yt.ytclient.proxy.request.PutFileToCache;
import ru.yandex.yt.ytclient.proxy.request.PutFileToCacheResult;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.ReduceOperation;
import ru.yandex.yt.ytclient.proxy.request.RemoteCopyOperation;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.ReshardTable;
import ru.yandex.yt.ytclient.proxy.request.ResumeOperation;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.SortOperation;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.SuspendOperation;
import ru.yandex.yt.ytclient.proxy.request.TabletInfo;
import ru.yandex.yt.ytclient.proxy.request.TrimTable;
import ru.yandex.yt.ytclient.proxy.request.UnfreezeTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters;
import ru.yandex.yt.ytclient.proxy.request.VanillaOperation;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public class MockYtClient implements BaseYtClient {
    private final Map<String, Deque<Callable<CompletableFuture<?>>>> mocks = new HashMap<>();
    private Map<String, Long> timesCalled = new HashMap<>();
    private final YtCluster cluster;
    private final ScheduledExecutorService executor = new NioEventLoopGroup(1);

    public MockYtClient(String clusterName) {
        this.cluster = new YtCluster(YtCluster.normalizeName(clusterName));
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

    public List<YtCluster> getClusters() {
        return List.of(cluster);
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("lookupRows");
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("lookupRows");
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request) {
        return (CompletableFuture<VersionedRowset>) callMethod("versionedLookupRows");
    }

    @Override
    public <T> CompletableFuture<List<T>> versionedLookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("versionedLookupRows");
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("selectRows");
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeObjectSerializer<T> serializer
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
    public <T> CompletableFuture<Void> lookupRows(AbstractLookupRowsRequest<?> request,
                                                  YTreeObjectSerializer<T> serializer, ConsumerSource<T> consumer) {
        return null;
    }

    @Override
    public <T> CompletableFuture<Void> versionedLookupRows(LookupRowsRequest request,
                                                           YTreeObjectSerializer<T> serializer,
                                                           ConsumerSource<T> consumer) {
        return null;
    }

    @Override
    public <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeObjectSerializer<T> serializer, ConsumerSource<T> consumer) {
        return null;
    }

    @Override
    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request) {
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
    public CompletableFuture<Void> alterTableReplica(GUID replicaId, boolean enabled, ETableReplicaMode mode, boolean preserveTimestamp, EAtomicity atomicity) {
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
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req, @Nullable TableAttachmentReader<T> reader) {
        return null;
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return (CompletableFuture<TableReader<T>>) callMethod("readTable");
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        return (CompletableFuture<TableWriter<T>>) callMethod("writeTable");
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
