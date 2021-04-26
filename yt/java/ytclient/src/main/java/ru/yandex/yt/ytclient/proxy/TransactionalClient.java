package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
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
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

/**
 * Interface of transactional YT client.
 * <p>
 *     <b>WARNING</b> Callbacks that <b>can block</b> (e.g. they use {@link CompletableFuture#join})
 *     <b>MUST NEVER BE USED</b> with non-Async thenApply, whenComplete, etc methods
 *     called on futures returned by this client.
 *
 * @see YtClient
 */
public abstract class TransactionalClient {
    public abstract CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request);

    public abstract <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    );

    public abstract CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request);

    public abstract <T> CompletableFuture<List<T>> versionedLookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    );

    public abstract CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request);

    public abstract <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeObjectSerializer<T> serializer
    );

    public abstract CompletableFuture<GUID> createNode(CreateNode req);

    public abstract CompletableFuture<Void> removeNode(RemoveNode req);

    public abstract CompletableFuture<Void> setNode(SetNode req);

    public abstract CompletableFuture<YTreeNode> getNode(GetNode req);

    public abstract CompletableFuture<YTreeNode> listNode(ListNode req);

    public abstract CompletableFuture<LockNodeResult> lockNode(LockNode req);

    public abstract CompletableFuture<GUID> copyNode(CopyNode req);

    public abstract CompletableFuture<GUID> linkNode(LinkNode req);

    public abstract CompletableFuture<GUID> moveNode(MoveNode req);

    public abstract CompletableFuture<Boolean> existsNode(ExistsNode req);

    public abstract CompletableFuture<Void> concatenateNodes(ConcatenateNodes req);

    public abstract <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req);

    public abstract <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req);

    public abstract CompletableFuture<FileReader> readFile(ReadFile req);

    public abstract CompletableFuture<FileWriter> writeFile(WriteFile req);

    public abstract CompletableFuture<GUID> startOperation(StartOperation req);

    public abstract CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req);

    //
    // Convenience methods
    //
    // NB. Some of the methods are not final since users have already overriden them :(
    //

    public final CompletableFuture<YTreeNode> getNode(String path) {
        return getNode(path, null);
    }

    public final CompletableFuture<YTreeNode> getNode(String path, @Nullable Duration requestTimeout) {
        return getNode(new GetNode(path).setTimeout(requestTimeout));
    }

    public final CompletableFuture<YTreeNode> listNode(String path) {
        return listNode(path, null);
    }

    public final CompletableFuture<YTreeNode> listNode(String path, @Nullable Duration requestTimeout) {
        return listNode(new ListNode(path).setTimeout(requestTimeout));
    }

    public final CompletableFuture<Void> setNode(String path, byte[] data) {
        return setNode(path, data, null);
    }

    public final CompletableFuture<Void> setNode(String path, byte[] data, @Nullable Duration requestTimeout) {
        return setNode(new SetNode(path, data).setTimeout(requestTimeout));
    }

    public final CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return setNode(path, data.toBinary());
    }

    public final CompletableFuture<Void> setNode(String path, YTreeNode data, @Nullable Duration requestTimeout) {
        return setNode(path, data.toBinary(), requestTimeout);
    }

    public CompletableFuture<Boolean> existsNode(String path) {
        return existsNode(path, null);
    }

    public final CompletableFuture<Boolean> existsNode(String path, @Nullable Duration requestTimeout) {
        return existsNode(new ExistsNode(path).setTimeout(requestTimeout));
    }

    public final CompletableFuture<GUID> createNode(String path, ObjectType type) {
        return createNode(new CreateNode(path, type));
    }

    public final CompletableFuture<GUID> createNode(String path, ObjectType type, @Nullable Duration requestTimeout) {
        return createNode(new CreateNode(path, type).setTimeout(requestTimeout));
    }

    public final CompletableFuture<GUID> createNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        return createNode(path, type, attributes, null);
    }

    public final CompletableFuture<GUID> createNode(
            String path,
            ObjectType type,
            Map<String, YTreeNode> attributes,
            @Nullable Duration requestTimeout
    ) {
        return createNode(new CreateNode(path, type, attributes).setTimeout(requestTimeout));
    }

    public final CompletableFuture<Void> removeNode(String path) {
        return removeNode(new RemoveNode(path));
    }

    public CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode) {
        return lockNode(path, mode, null);
    }

    public CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode, @Nullable Duration requestTimeout) {
        return lockNode(new LockNode(path, mode).setTimeout(requestTimeout));
    }

    public final CompletableFuture<GUID> copyNode(String src, String dst) {
        return copyNode(src, dst, null);
    }

    public final CompletableFuture<GUID> copyNode(String src, String dst, @Nullable Duration requestTimeout) {
        return copyNode(new CopyNode(src, dst).setTimeout(requestTimeout));
    }

    public final CompletableFuture<GUID> moveNode(String from, String to) {
        return moveNode(from, to, null);
    }

    public final CompletableFuture<GUID> moveNode(String from, String to, @Nullable Duration requestTimeout) {
        return moveNode(new MoveNode(from, to).setTimeout(requestTimeout));
    }

    public final CompletableFuture<GUID> linkNode(String src, String dst) {
        return linkNode(new LinkNode(src, dst));
    }

    public final CompletableFuture<Void> concatenateNodes(String[] from, String to) {
        return concatenateNodes(from, to, null);
    }

    public final CompletableFuture<Void> concatenateNodes(String[] from, String to, @Nullable Duration requestTimeout) {
        return concatenateNodes(new ConcatenateNodes(from, to).setTimeout(requestTimeout));
    }
}
