package tech.ytsaurus.client.sync;

import java.util.List;

import tech.ytsaurus.client.operations.MapReduceSpec;
import tech.ytsaurus.client.operations.MapSpec;
import tech.ytsaurus.client.operations.MergeSpec;
import tech.ytsaurus.client.operations.ReduceSpec;
import tech.ytsaurus.client.operations.RemoteCopySpec;
import tech.ytsaurus.client.operations.SortSpec;
import tech.ytsaurus.client.operations.VanillaSpec;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AdvanceConsumer;
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

    /**
     * Run map operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #map(MapSpec)}.
     */
    SyncOperation map(MapOperation req);

    /**
     * Run reduce operation. Wait for its completion and check status.
     * <p>
     * {@link #map(MapOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation map(MapSpec spec) {
        return map(MapOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run reduce operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #reduce(ReduceSpec)}.
     */
    SyncOperation reduce(ReduceOperation req);

    /**
     * Run reduce operation. Wait for its completion and check status.
     * <p>
     * {@link #reduce(ReduceOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation reduce(ReduceSpec spec) {
        return reduce(ReduceOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run map-reduce operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #mapReduce(MapReduceSpec)}.
     */
    SyncOperation mapReduce(MapReduceOperation req);

    /**
     * Run map-reduce operation. Wait for its completion and check status.
     * <p>
     * {@link #mapReduce(MapReduceOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation mapReduce(MapReduceSpec spec) {
        return mapReduce(MapReduceOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run merge operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #merge(MergeSpec)}.
     */
    SyncOperation merge(MergeOperation req);

    /**
     * Run merge operation. Wait for its completion and check status.
     * <p>
     * {@link #merge(MergeOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation merge(MergeSpec spec) {
        return merge(MergeOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run sort operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #sort(SortSpec)}.
     */
    SyncOperation sort(SortOperation req);

    /**
     * Run sort operation. Wait for its completion and check status.
     * <p>
     * {@link #sort(SortOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation sort(SortSpec spec) {
        return sort(SortOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run vanilla operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #vanilla(VanillaSpec)}.
     */
    SyncOperation vanilla(VanillaOperation req);

    /**
     * Run vanilla operation. Wait for its completion and check status.
     * <p>
     * {@link #vanilla(VanillaOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation vanilla(VanillaSpec spec) {
        return vanilla(VanillaOperation.builder()
                .setSpec(spec)
                .build());
    }

    /**
     * Run remote-copy operation. Wait for its completion and check status.
     * <p>
     * This method provides more possibilities for fine-tuning compared to
     * {@link #remoteCopy(RemoteCopySpec)}.
     */
    SyncOperation remoteCopy(RemoteCopyOperation req);

    /**
     * Run remote-copy operation. Wait for its completion and check status.
     * <p>
     * {@link #remoteCopy(RemoteCopyOperation)} provides more possibilities for fine-tuning.
     */
    default SyncOperation remoteCopy(RemoteCopySpec spec) {
        return remoteCopy(RemoteCopyOperation.builder()
                .setSpec(spec)
                .build());
    }

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

    void advanceConsumer(AdvanceConsumer req);

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
