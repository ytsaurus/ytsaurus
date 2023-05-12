package tech.ytsaurus.client.sync;

import java.util.List;

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

public interface SyncTransactionalClient {
    TCheckPermissionResult checkPermission(CheckPermission req);

    LockNodeResult lockNode(LockNode req);

    void concatenateNodes(ConcatenateNodes req);

    GUID copyNode(CopyNode req);

    GUID createNode(CreateNode req);

    Boolean existsNode(ExistsNode req);

    GetFileFromCacheResult getFileFromCache(GetFileFromCache req);

    YTreeNode getNode(GetNode req);

    GUID linkNode(LinkNode req);

    YTreeNode listNode(ListNode req);

    UnversionedRowset lookupRows(AbstractLookupRowsRequest<?, ?> req);

    <T> List<T> lookupRows(
            AbstractLookupRowsRequest<?, ?> req,
            YTreeRowSerializer<T> serializer
    );

    SyncOperation map(MapOperation req);

    SyncOperation reduce(ReduceOperation req);

    SyncOperation mapReduce(MapReduceOperation req);

    SyncOperation merge(MergeOperation req);

    SyncOperation sort(SortOperation req);

    SyncOperation vanilla(VanillaOperation req);

    SyncOperation remoteCopy(RemoteCopyOperation req);

    SyncOperation startMap(MapOperation req);

    SyncOperation startReduce(ReduceOperation req);

    SyncOperation startMapReduce(MapReduceOperation req);

    SyncOperation startMerge(MergeOperation req);

    SyncOperation startSort(SortOperation req);

    SyncOperation startVanilla(VanillaOperation req);

    SyncOperation startRemoteCopy(RemoteCopyOperation req);

    GUID startOperation(StartOperation req);

    GUID moveNode(MoveNode req);

    List<MultiTablePartition> partitionTables(PartitionTables req);

    PutFileToCacheResult putFileToCache(PutFileToCache req);

    SyncFileReader readFile(ReadFile req);

    <T> SyncTableReader<T> readTable(ReadTable<T> req);

    SyncFileWriter writeFile(WriteFile req);

    <T> SyncTableWriter<T> writeTable(WriteTable<T> req);

    void removeNode(RemoveNode req);

    UnversionedRowset selectRows(SelectRowsRequest req);

    <T> List<T> selectRows(
            SelectRowsRequest req,
            YTreeRowSerializer<T> serializer
    );

    <T> void selectRows(
            SelectRowsRequest req,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    void setNode(SetNode req);

    VersionedRowset versionedLookupRows(AbstractLookupRowsRequest<?, ?> req);
}
