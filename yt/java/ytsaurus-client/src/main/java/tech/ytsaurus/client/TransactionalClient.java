package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.operations.Operation;
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
import tech.ytsaurus.client.request.ReadSerializationContext;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.ReadTableDirect;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.RemoteCopyOperation;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.SetNode;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.client.request.StartOperation;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.rpcproxy.TCheckPermissionResult;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Interface of transactional YT client.
 * <p>
 * <b>WARNING</b> Callbacks that <b>can block</b> (e.g. they use {@link CompletableFuture#join})
 * <b>MUST NEVER BE USED</b> with non-Async thenApply, whenComplete etc methods
 * called on futures returned by this client.
 *
 * @see ApiServiceClient
 */
public interface TransactionalClient extends ImmutableTransactionalClient {
    TransactionalClient getRootClient();

    CompletableFuture<GUID> createNode(CreateNode req);

    /**
     * @deprecated prefer to use {@link #createNode(CreateNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> createNode(CreateNode.BuilderBase<?> req) {
        return createNode(req.build());
    }

    CompletableFuture<Void> removeNode(RemoveNode req);

    /**
     * @deprecated prefer to use {@link #removeNode(RemoveNode)}
     */
    @Deprecated
    default CompletableFuture<Void> removeNode(RemoveNode.BuilderBase<?> req) {
        return removeNode(req.build());
    }

    CompletableFuture<Void> setNode(SetNode req);

    /**
     * @deprecated prefer to use {@link #setNode(SetNode)}
     */
    @Deprecated
    default CompletableFuture<Void> setNode(SetNode.BuilderBase<?> req) {
        return setNode(req.build());
    }

    CompletableFuture<YTreeNode> getNode(GetNode req);

    /**
     * @deprecated prefer to use {@link #getNode(GetNode)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getNode(GetNode.BuilderBase<?> getNode) {
        return getNode(getNode.build());
    }

    CompletableFuture<YTreeNode> listNode(ListNode req);

    /**
     * @deprecated prefer to use {@link #listNode(ListNode)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> listNode(ListNode.BuilderBase<?> listNode) {
        return listNode(listNode.build());
    }

    CompletableFuture<LockNodeResult> lockNode(LockNode req);

    /**
     * @deprecated prefer to use {@link #lockNode(LockNode)}
     */
    @Deprecated
    default CompletableFuture<LockNodeResult> lockNode(LockNode.BuilderBase<?> req) {
        return lockNode(req.build());
    }

    CompletableFuture<GUID> copyNode(CopyNode req);

    /**
     * @deprecated prefer to use {@link #copyNode(CopyNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> copyNode(CopyNode.BuilderBase<?> req) {
        return copyNode(req.build());
    }

    CompletableFuture<GUID> linkNode(LinkNode req);

    /**
     * @deprecated prefer to use {@link #linkNode(LinkNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> linkNode(LinkNode.BuilderBase<?> req) {
        return linkNode(req.build());
    }

    CompletableFuture<GUID> moveNode(MoveNode req);

    /**
     * @deprecated prefer to use {@link #moveNode(MoveNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> moveNode(MoveNode.BuilderBase<?> req) {
        return moveNode(req.build());
    }

    CompletableFuture<Boolean> existsNode(ExistsNode req);

    /**
     * @deprecated prefer to use {@link #existsNode(ExistsNode)}
     */
    @Deprecated
    default CompletableFuture<Boolean> existsNode(ExistsNode.BuilderBase<?> req) {
        return existsNode(req.build());
    }

    CompletableFuture<Void> concatenateNodes(ConcatenateNodes req);

    /**
     * @deprecated prefer to use {@link #concatenateNodes(ConcatenateNodes)}
     */
    @Deprecated
    default CompletableFuture<Void> concatenateNodes(ConcatenateNodes.BuilderBase<?> req) {
        return concatenateNodes(req.build());
    }

    CompletableFuture<List<MultiTablePartition>> partitionTables(PartitionTables req);

    CompletableFuture<Void> advanceConsumer(AdvanceConsumer req);

    /**
     * @deprecated prefer to use {@link #readTable(ReadTable)}
     */
    @Deprecated
    default <T> CompletableFuture<TableReader<T>> readTable(ReadTable.BuilderBase<T, ?> req) {
        return readTable(req.build());
    }

    /**
     * @deprecated prefer to use {@link #readTable(ReadTable)}
     */
    @Deprecated
    default <T> CompletableFuture<TableReader<T>> readTable(
            ReadTable.BuilderBase<T, ?> req,
            @Nullable TableAttachmentReader<T> reader) {
        if (reader != null) {
            req.setSerializationContext(new ReadSerializationContext<T>(reader));
        }
        return readTable(req.build());
    }

    <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req);

    <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req);

    default CompletableFuture<TableReader<byte[]>> readTableDirect(ReadTableDirect req) {
        return readTable(req);
    }

    <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req);

    /**
     * @deprecated prefer to use {@link #writeTable(WriteTable)}
     */
    @Deprecated
    default <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable.BuilderBase<T, ?> req) {
        return writeTable(req.build());
    }

    <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req);

    CompletableFuture<FileReader> readFile(ReadFile req);

    /**
     * @deprecated prefer to use {@link #readFile(ReadFile)}
     */
    @Deprecated
    default CompletableFuture<FileReader> readFile(ReadFile.BuilderBase<?> req) {
        return readFile(req.build());
    }

    CompletableFuture<FileWriter> writeFile(WriteFile req);

    /**
     * @deprecated prefer to use {@link #writeTable(WriteTable)}
     */
    @Deprecated
    default CompletableFuture<FileWriter> writeFile(WriteFile.BuilderBase<?> req) {
        return writeFile(req.build());
    }

    CompletableFuture<GUID> startOperation(StartOperation req);

    /**
     * @deprecated prefer to use {@link #startOperation(StartOperation)}
     */
    @Deprecated
    default CompletableFuture<GUID> startOperation(StartOperation.BuilderBase<?> req) {
        return startOperation(req.build());
    }

    CompletableFuture<Operation> startMap(MapOperation req);

    default CompletableFuture<Operation> map(MapOperation req) {
        return startMap(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startReduce(ReduceOperation req);

    default CompletableFuture<Operation> reduce(ReduceOperation req) {
        return startReduce(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startSort(SortOperation req);

    default CompletableFuture<Operation> sort(SortOperation req) {
        return startSort(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startMapReduce(MapReduceOperation req);

    default CompletableFuture<Operation> mapReduce(MapReduceOperation req) {
        return startMapReduce(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startMerge(MergeOperation req);

    default CompletableFuture<Operation> merge(MergeOperation req) {
        return startMerge(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startRemoteCopy(RemoteCopyOperation req);

    Operation attachOperation(GUID operationId);

    default CompletableFuture<Operation> remoteCopy(RemoteCopyOperation req) {
        return startRemoteCopy(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<Operation> startVanilla(VanillaOperation req);

    default CompletableFuture<Operation> vanilla(VanillaOperation req) {
        return startVanilla(req).thenCompose(op -> op.watch().thenApply(unused -> op));
    }

    CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req);

    CompletableFuture<GetFileFromCacheResult> getFileFromCache(GetFileFromCache req);

    CompletableFuture<PutFileToCacheResult> putFileToCache(PutFileToCache req);

    default CompletableFuture<YTreeNode> getNode(YPath path) {
        return getNode(GetNode.builder().setPath(path).build());
    }

    default CompletableFuture<YTreeNode> getNode(String path) {
        return getNode(path, null);
    }

    default CompletableFuture<YTreeNode> getNode(String path, @Nullable Duration requestTimeout) {
        return getNode(GetNode.builder().setPath(YPath.simple(path)).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<YTreeNode> listNode(YPath path) {
        return listNode(ListNode.builder().setPath(path).build());
    }

    default CompletableFuture<YTreeNode> listNode(String path) {
        return listNode(path, null);
    }

    default CompletableFuture<YTreeNode> listNode(String path, @Nullable Duration requestTimeout) {
        return listNode(ListNode.builder().setPath(YPath.simple(path)).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<Void> setNode(String path, byte[] data) {
        return setNode(path, data, null);
    }

    default CompletableFuture<Void> setNode(String path, byte[] data, @Nullable Duration requestTimeout) {
        return setNode(SetNode.builder().setPath(YPath.simple(path)).setValue(data).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return setNode(path, data.toBinary());
    }

    default CompletableFuture<Void> setNode(String path, YTreeNode data, @Nullable Duration requestTimeout) {
        return setNode(path, data.toBinary(), requestTimeout);
    }

    default CompletableFuture<Boolean> existsNode(String path) {
        return existsNode(path, null);
    }

    default CompletableFuture<Boolean> existsNode(String path, @Nullable Duration requestTimeout) {
        return existsNode(ExistsNode.builder().setPath(YPath.simple(path)).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<GUID> createNode(String path, CypressNodeType type) {
        return createNode(new CreateNode(YPath.simple(path), type));
    }

    default CompletableFuture<GUID> createNode(String path, CypressNodeType type, @Nullable Duration requestTimeout) {
        return createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(type)
                .setTimeout(requestTimeout)
                .build());
    }

    default CompletableFuture<GUID> createNode(String path, CypressNodeType type, Map<String, YTreeNode> attributes) {
        return createNode(path, type, attributes, null);
    }

    default CompletableFuture<GUID> createNode(
            String path,
            CypressNodeType type,
            Map<String, YTreeNode> attributes,
            @Nullable Duration requestTimeout
    ) {
        return createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(type)
                .setAttributes(attributes)
                .setTimeout(requestTimeout)
                .build());
    }

    default CompletableFuture<Void> removeNode(String path) {
        return removeNode(new RemoveNode(YPath.simple(path)));
    }

    default CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode) {
        return lockNode(path, mode, null);
    }

    default CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode, @Nullable Duration requestTimeout) {
        return lockNode(LockNode.builder()
                .setPath(YPath.simple(path))
                .setMode(mode)
                .setTimeout(requestTimeout)
                .build());
    }

    default CompletableFuture<GUID> copyNode(String src, String dst) {
        return copyNode(src, dst, null);
    }

    default CompletableFuture<GUID> copyNode(String src, String dst, @Nullable Duration requestTimeout) {
        return copyNode(CopyNode.builder().setSource(src).setDestination(dst).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<GUID> moveNode(String from, String to) {
        return moveNode(from, to, null);
    }

    default CompletableFuture<GUID> moveNode(String from, String to, @Nullable Duration requestTimeout) {
        return moveNode(MoveNode.builder().setSource(from).setDestination(to).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<GUID> linkNode(String src, String dst) {
        return linkNode(new LinkNode(src, dst));
    }

    default CompletableFuture<Void> concatenateNodes(String[] from, String to) {
        return concatenateNodes(from, to, null);
    }

    default CompletableFuture<Void> concatenateNodes(String[] from, String to, @Nullable Duration requestTimeout) {
        return concatenateNodes(ConcatenateNodes.builder()
                .setSourcePaths(Arrays.stream(from).map(YPath::simple).collect(Collectors.toList()))
                .setDestinationPath(YPath.simple(to))
                .setTimeout(requestTimeout)
                .build());
    }
}
