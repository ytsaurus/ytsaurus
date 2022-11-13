package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;
import ru.yandex.yt.ytclient.operations.Operation;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.request.CheckPermission;
import ru.yandex.yt.ytclient.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.request.CopyNode;
import ru.yandex.yt.ytclient.request.CreateNode;
import ru.yandex.yt.ytclient.request.ExistsNode;
import ru.yandex.yt.ytclient.request.GetFileFromCache;
import ru.yandex.yt.ytclient.request.GetFileFromCacheResult;
import ru.yandex.yt.ytclient.request.GetNode;
import ru.yandex.yt.ytclient.request.LinkNode;
import ru.yandex.yt.ytclient.request.ListNode;
import ru.yandex.yt.ytclient.request.LockMode;
import ru.yandex.yt.ytclient.request.LockNode;
import ru.yandex.yt.ytclient.request.LockNodeResult;
import ru.yandex.yt.ytclient.request.MapOperation;
import ru.yandex.yt.ytclient.request.MapReduceOperation;
import ru.yandex.yt.ytclient.request.MergeOperation;
import ru.yandex.yt.ytclient.request.MoveNode;
import ru.yandex.yt.ytclient.request.MultiTablePartition;
import ru.yandex.yt.ytclient.request.PartitionTables;
import ru.yandex.yt.ytclient.request.PutFileToCache;
import ru.yandex.yt.ytclient.request.PutFileToCacheResult;
import ru.yandex.yt.ytclient.request.ReadFile;
import ru.yandex.yt.ytclient.request.ReadTable;
import ru.yandex.yt.ytclient.request.ReadTableDirect;
import ru.yandex.yt.ytclient.request.ReduceOperation;
import ru.yandex.yt.ytclient.request.RemoteCopyOperation;
import ru.yandex.yt.ytclient.request.RemoveNode;
import ru.yandex.yt.ytclient.request.SetNode;
import ru.yandex.yt.ytclient.request.SortOperation;
import ru.yandex.yt.ytclient.request.StartOperation;
import ru.yandex.yt.ytclient.request.VanillaOperation;
import ru.yandex.yt.ytclient.request.WriteFile;
import ru.yandex.yt.ytclient.request.WriteTable;

/**
 * Interface of transactional YT client.
 * <p>
 *     <b>WARNING</b> Callbacks that <b>can block</b> (e.g. they use {@link CompletableFuture#join})
 *     <b>MUST NEVER BE USED</b> with non-Async thenApply, whenComplete, etc methods
 *     called on futures returned by this client.
 *
 * @see YtClient
 */
public interface TransactionalClient extends ImmutableTransactionalClient {
    TransactionalClient getRootClient();

    CompletableFuture<GUID> createNode(CreateNode req);

    /**
     * @deprected prefer to use {@link #createNode(CreateNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> createNode(CreateNode.BuilderBase<?> req) {
        return createNode(req.build());
    }

    CompletableFuture<Void> removeNode(RemoveNode req);

    /**
     * @deprected prefer to use {@link #removeNode(RemoveNode)}
     */
    @Deprecated
    default CompletableFuture<Void> removeNode(RemoveNode.BuilderBase<?> req) {
        return removeNode(req.build());
    }

    CompletableFuture<Void> setNode(SetNode req);

    /**
     * @deprected prefer to use {@link #setNode(SetNode)}
     */
    @Deprecated
    default CompletableFuture<Void> setNode(SetNode.BuilderBase<?> req) {
        return setNode(req.build());
    }

    CompletableFuture<YTreeNode> getNode(GetNode req);

    /**
     * @deprected prefer to use {@link #getNode(GetNode)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getNode(GetNode.BuilderBase<?> getNode) {
        return getNode(getNode.build());
    }

    CompletableFuture<YTreeNode> listNode(ListNode req);

    /**
     * @deprected prefer to use {@link #listNode(ListNode)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> listNode(ListNode.BuilderBase<?> listNode) {
        return listNode(listNode.build());
    }

    CompletableFuture<LockNodeResult> lockNode(LockNode req);

    /**
     * @deprected prefer to use {@link #lockNode(LockNode)}
     */
    @Deprecated
    default CompletableFuture<LockNodeResult> lockNode(LockNode.BuilderBase<?> req) {
        return lockNode(req.build());
    }

    CompletableFuture<GUID> copyNode(CopyNode req);

    /**
     * @deprected prefer to use {@link #copyNode(CopyNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> copyNode(CopyNode.BuilderBase<?> req) {
        return copyNode(req.build());
    }

    CompletableFuture<GUID> linkNode(LinkNode req);

    /**
     * @deprected prefer to use {@link #linkNode(LinkNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> linkNode(LinkNode.BuilderBase<?> req) {
        return linkNode(req.build());
    }

    CompletableFuture<GUID> moveNode(MoveNode req);

    /**
     * @deprected prefer to use {@link #moveNode(MoveNode)}
     */
    @Deprecated
    default CompletableFuture<GUID> moveNode(MoveNode.BuilderBase<?> req) {
        return moveNode(req.build());
    }

    CompletableFuture<Boolean> existsNode(ExistsNode req);

    /**
     * @deprected prefer to use {@link #existsNode(ExistsNode)}
     */
    @Deprecated
    default CompletableFuture<Boolean> existsNode(ExistsNode.BuilderBase<?> req) {
        return existsNode(req.build());
    }

    CompletableFuture<Void> concatenateNodes(ConcatenateNodes req);

    /**
     * @deprected prefer to use {@link #concatenateNodes(ConcatenateNodes)}
     */
    @Deprecated
    default CompletableFuture<Void> concatenateNodes(ConcatenateNodes.BuilderBase<?> req) {
        return concatenateNodes(req.build());
    }

    CompletableFuture<List<MultiTablePartition>> partitionTables(PartitionTables req);

    /**
     * @deprected prefer to use {@link #readTable(ReadTable)}
     */
    @Deprecated
    default <T> CompletableFuture<TableReader<T>> readTable(ReadTable.BuilderBase<T, ?> req) {
        return readTable(req.build());
    }

    /**
     * @deprected prefer to use {@link #readTable(ReadTable, TableAttachmentReader)}
     */
    @Deprecated
    default <T> CompletableFuture<TableReader<T>> readTable(
            ReadTable.BuilderBase<T, ?> req,
            @Nullable TableAttachmentReader<T> reader) {
        return readTable(req.build(), reader);
    }

    default <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        Optional<WireRowDeserializer<T>> deserializer = req.getSerializationContext().getDeserializer();
        if (deserializer.isPresent()) {
            return readTable(req, new TableAttachmentWireProtocolReader<>(deserializer.get()));
        } else {
            return readTable(req, null);
        }
    }

    <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req,
                                                    @Nullable TableAttachmentReader<T> reader);

    default <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req) {
        Optional<WireRowDeserializer<T>> deserializer = req.getSerializationContext().getDeserializer();
        if (deserializer.isPresent()) {
            return readTableV2(req, new TableAttachmentWireProtocolReader<>(deserializer.get()));
        } else {
            return readTableV2(req, null);
        }
    }

    <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req,
                                                      @Nullable TableAttachmentReader<T> reader);

    default CompletableFuture<TableReader<byte[]>> readTableDirect(ReadTableDirect req) {
        return readTable(req, TableAttachmentReader.BYPASS);
    }

    <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req);

    /**
     * @deprected prefer to use {@link #writeTable(WriteTable)}
     */
    @Deprecated
    default <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable.BuilderBase<T, ?> req) {
        return writeTable(req.build());
    }

    <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req);

    CompletableFuture<FileReader> readFile(ReadFile req);

    /**
     * @deprected prefer to use {@link #readFile(ReadFile)}
     */
    @Deprecated
    default CompletableFuture<FileReader> readFile(ReadFile.BuilderBase<?> req) {
        return readFile(req.build());
    }

    CompletableFuture<FileWriter> writeFile(WriteFile req);

    /**
     * @deprected prefer to use {@link #writeTable(WriteTable)}
     */
    @Deprecated
    default CompletableFuture<FileWriter> writeFile(WriteFile.BuilderBase<?> req) {
        return writeFile(req.build());
    }

    CompletableFuture<GUID> startOperation(StartOperation req);

    /**
     * @deprected prefer to use {@link #startOperation(StartOperation)}
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

    default CompletableFuture<GUID> createNode(String path, ObjectType type) {
        return createNode(new CreateNode(YPath.simple(path), type));
    }

    default CompletableFuture<GUID> createNode(String path, ObjectType type, @Nullable Duration requestTimeout) {
        return createNode(CreateNode.builder()
                .setPath(YPath.simple(path))
                .setType(type)
                .setTimeout(requestTimeout)
                .build());
    }

    default CompletableFuture<GUID> createNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        return createNode(path, type, attributes, null);
    }

    default CompletableFuture<GUID> createNode(
            String path,
            ObjectType type,
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
