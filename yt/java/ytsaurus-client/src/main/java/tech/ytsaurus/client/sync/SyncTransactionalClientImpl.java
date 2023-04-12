package tech.ytsaurus.client.sync;

import java.util.List;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.client.operations.SyncOperation;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.CheckPermission;
import tech.ytsaurus.client.request.ConcatenateNodes;
import tech.ytsaurus.client.request.CopyNode;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.request.GetFileFromCache;
import tech.ytsaurus.client.request.GetFileFromCacheResult;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.LinkNode;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.LockNodeResult;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.client.request.MapReduceOperation;
import tech.ytsaurus.client.request.MergeOperation;
import tech.ytsaurus.client.request.MoveNode;
import tech.ytsaurus.client.request.MultiTablePartition;
import tech.ytsaurus.client.request.PartitionTables;
import tech.ytsaurus.client.request.PutFileToCache;
import tech.ytsaurus.client.request.PutFileToCacheResult;
import tech.ytsaurus.client.request.ReadFile;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.RemoteCopyOperation;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.request.SetNode;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.client.request.StartOperation;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.rpcproxy.TCheckPermissionResult;
import tech.ytsaurus.ysontree.YTreeNode;

abstract class SyncTransactionalClientImpl implements SyncTransactionalClient {
    private final TransactionalClient client;

    protected SyncTransactionalClientImpl(TransactionalClient client) {
        this.client = client;
    }

    @Override
    public TCheckPermissionResult checkPermission(CheckPermission req) {
        return client.checkPermission(req).join();
    }

    @Override
    public LockNodeResult lockNode(LockNode req) {
        return client.lockNode(req).join();
    }

    @Override
    public void concatenateNodes(ConcatenateNodes req) {
        client.concatenateNodes(req).join();
    }

    @Override
    public GUID copyNode(CopyNode req) {
        return client.copyNode(req).join();
    }

    @Override
    public GUID createNode(CreateNode req) {
        return client.createNode(req).join();
    }

    @Override
    public Boolean existsNode(ExistsNode req) {
        return client.existsNode(req).join();
    }

    @Override
    public GetFileFromCacheResult getFileFromCache(GetFileFromCache req) {
        return client.getFileFromCache(req).join();
    }

    @Override
    public YTreeNode getNode(GetNode req) {
        return client.getNode(req).join();
    }

    @Override
    public GUID linkNode(LinkNode req) {
        return client.linkNode(req).join();
    }

    @Override
    public YTreeNode listNode(ListNode req) {
        return client.listNode(req).join();
    }

    @Override
    public UnversionedRowset lookupRows(AbstractLookupRowsRequest<?, ?> req) {
        return client.lookupRows(req).join();
    }

    @Override
    public <T> List<T> lookupRows(
            AbstractLookupRowsRequest<?, ?> req,
            YTreeRowSerializer<T> serializer
    ) {
        return client.lookupRows(req, serializer).join();
    }

    @Override
    public SyncOperation map(MapOperation req) {
        return new SyncOperation(client.map(req).join());
    }

    @Override
    public SyncOperation reduce(ReduceOperation req) {
        return new SyncOperation(client.reduce(req).join());
    }

    @Override
    public SyncOperation mapReduce(MapReduceOperation req) {
        return new SyncOperation(client.mapReduce(req).join());
    }

    @Override
    public SyncOperation merge(MergeOperation req) {
        return new SyncOperation(client.merge(req).join());
    }

    @Override
    public SyncOperation sort(SortOperation req) {
        return new SyncOperation(client.sort(req).join());
    }

    @Override
    public SyncOperation vanilla(VanillaOperation req) {
        return new SyncOperation(client.vanilla(req).join());
    }

    @Override
    public SyncOperation remoteCopy(RemoteCopyOperation req) {
        return new SyncOperation(client.remoteCopy(req).join());
    }

    @Override
    public SyncOperation startMap(MapOperation req) {
        return new SyncOperation(client.startMap(req).join());
    }

    @Override
    public SyncOperation startReduce(ReduceOperation req) {
        return new SyncOperation(client.startReduce(req).join());
    }

    @Override
    public SyncOperation startMapReduce(MapReduceOperation req) {
        return new SyncOperation(client.startMapReduce(req).join());
    }

    @Override
    public SyncOperation startMerge(MergeOperation req) {
        return new SyncOperation(client.startMerge(req).join());
    }

    @Override
    public SyncOperation startSort(SortOperation req) {
        return new SyncOperation(client.startSort(req).join());
    }

    @Override
    public SyncOperation startVanilla(VanillaOperation req) {
        return new SyncOperation(client.startVanilla(req).join());
    }

    @Override
    public SyncOperation startRemoteCopy(RemoteCopyOperation req) {
        return new SyncOperation(client.startRemoteCopy(req).join());
    }

    @Override
    public GUID startOperation(StartOperation req) {
        return client.startOperation(req).join();
    }

    @Override
    public GUID moveNode(MoveNode req) {
        return client.moveNode(req).join();
    }

    @Override
    public List<MultiTablePartition> partitionTables(PartitionTables req) {
        return client.partitionTables(req).join();
    }

    @Override
    public PutFileToCacheResult putFileToCache(PutFileToCache req) {
        return client.putFileToCache(req).join();
    }

    @Override
    public SyncFileReader readFile(ReadFile req) {
        return SyncFileReaderImpl.wrap(client.readFile(req).join());
    }

    @Override
    public <T> SyncTableReader<T> readTable(ReadTable<T> req) {
        return SyncTableReaderImpl.wrap(client.readTableV2(req).join());
    }

    @Override
    public SyncFileWriter writeFile(WriteFile req) {
        return SyncFileWriterImpl.wrap(client.writeFile(req).join());
    }

    @Override
    public <T> SyncTableWriter<T> writeTable(WriteTable<T> req) {
        return SyncTableWriterImpl.wrap(client.writeTableV2(req).join());
    }

    @Override
    public void removeNode(RemoveNode req) {
        client.removeNode(req).join();
    }

    @Override
    public UnversionedRowset selectRows(SelectRowsRequest req) {
        return client.selectRows(req).join();
    }

    @Override
    public <T> List<T> selectRows(
            SelectRowsRequest req,
            YTreeRowSerializer<T> serializer
    ) {
        return client.selectRows(req, serializer).join();
    }

    @Override
    public <T> void selectRows(
            SelectRowsRequest req,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        client.selectRows(req, serializer, consumer).join();
    }

    @Override
    public void setNode(SetNode req) {
        client.setNode(req).join();
    }

    @Override
    public VersionedRowset versionedLookupRows(AbstractLookupRowsRequest<?, ?> req) {
        return client.versionedLookupRows(req).join();
    }

    @Override
    public String toString() {
        return client.toString();
    }
}
