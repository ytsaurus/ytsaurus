package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.ytclient.proxy.request.CheckPermission;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;

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
    CompletableFuture<GUID> createNode(CreateNode req);

    CompletableFuture<Void> removeNode(RemoveNode req);

    CompletableFuture<Void> setNode(SetNode req);

    CompletableFuture<YTreeNode> getNode(GetNode req);

    CompletableFuture<YTreeNode> listNode(ListNode req);

    CompletableFuture<LockNodeResult> lockNode(LockNode req);

    CompletableFuture<GUID> copyNode(CopyNode req);

    CompletableFuture<GUID> linkNode(LinkNode req);

    CompletableFuture<GUID> moveNode(MoveNode req);

    CompletableFuture<Boolean> existsNode(ExistsNode req);

    CompletableFuture<Void> concatenateNodes(ConcatenateNodes req);

    <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req);

    <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req);

    CompletableFuture<FileReader> readFile(ReadFile req);

    CompletableFuture<FileWriter> writeFile(WriteFile req);

    CompletableFuture<GUID> startOperation(StartOperation req);

    CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req);

    default CompletableFuture<YTreeNode> getNode(String path) {
        return getNode(path, null);
    }

    default CompletableFuture<YTreeNode> getNode(String path, @Nullable Duration requestTimeout) {
        return getNode(new GetNode(path).setTimeout(requestTimeout));
    }

    default CompletableFuture<YTreeNode> listNode(String path) {
        return listNode(path, null);
    }

    default CompletableFuture<YTreeNode> listNode(String path, @Nullable Duration requestTimeout) {
        return listNode(new ListNode(path).setTimeout(requestTimeout));
    }

    default CompletableFuture<Void> setNode(String path, byte[] data) {
        return setNode(path, data, null);
    }

    default CompletableFuture<Void> setNode(String path, byte[] data, @Nullable Duration requestTimeout) {
        return setNode(new SetNode(path, data).setTimeout(requestTimeout));
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
        return existsNode(new ExistsNode(path).setTimeout(requestTimeout));
    }

    default CompletableFuture<GUID> createNode(String path, ObjectType type) {
        return createNode(new CreateNode(path, type));
    }

    default CompletableFuture<GUID> createNode(String path, ObjectType type, @Nullable Duration requestTimeout) {
        return createNode(new CreateNode(path, type).setTimeout(requestTimeout));
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
        return createNode(new CreateNode(path, type, attributes).setTimeout(requestTimeout));
    }

    default CompletableFuture<Void> removeNode(String path) {
        return removeNode(new RemoveNode(path));
    }

    default CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode) {
        return lockNode(path, mode, null);
    }

    default CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode, @Nullable Duration requestTimeout) {
        return lockNode(new LockNode(path, mode).setTimeout(requestTimeout));
    }

    default CompletableFuture<GUID> copyNode(String src, String dst) {
        return copyNode(src, dst, null);
    }

    default CompletableFuture<GUID> copyNode(String src, String dst, @Nullable Duration requestTimeout) {
        return copyNode(new CopyNode(src, dst).setTimeout(requestTimeout));
    }

    default CompletableFuture<GUID> moveNode(String from, String to) {
        return moveNode(from, to, null);
    }

    default CompletableFuture<GUID> moveNode(String from, String to, @Nullable Duration requestTimeout) {
        return moveNode(new MoveNode(from, to).setTimeout(requestTimeout));
    }

    default CompletableFuture<GUID> linkNode(String src, String dst) {
        return linkNode(new LinkNode(src, dst));
    }

    default CompletableFuture<Void> concatenateNodes(String[] from, String to) {
        return concatenateNodes(from, to, null);
    }

    default CompletableFuture<Void> concatenateNodes(String[] from, String to, @Nullable Duration requestTimeout) {
        return concatenateNodes(new ConcatenateNodes(from, to).setTimeout(requestTimeout));
    }
}
