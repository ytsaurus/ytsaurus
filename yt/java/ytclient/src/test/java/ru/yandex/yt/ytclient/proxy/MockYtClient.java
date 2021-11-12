package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.nio.NioEventLoopGroup;

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
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public class MockYtClient implements TransactionalClient, BaseYtClient {
    private Map<String, Deque<Callable<CompletableFuture<?>>>> mocks = new HashMap<>();
    private Map<String, Long> timesCalled = new HashMap<>();
    private YtCluster cluster;
    private ScheduledExecutorService executor = new NioEventLoopGroup(1);

    public MockYtClient(String clusterName) {
        this.cluster = new YtCluster(YtCluster.normalizeName(clusterName));
    }

    public void mockMethod(String methodName, Callable<CompletableFuture<?>> callback) {
        synchronized (this) {
            if (!mocks.containsKey(methodName)) {
                mocks.put(methodName, new ArrayDeque<>());
            }

            mocks.get(methodName).add(callback);
        }
    }

    public long getTimesCalled(String methodName) {
        synchronized (this) {
            return timesCalled.getOrDefault(methodName, Long.valueOf(0));
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

    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("lookupRows");
    }

    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("lookupRows");
    }

    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request) {
        return (CompletableFuture<VersionedRowset>) callMethod("versionedLookupRows");
    }

    public <T> CompletableFuture<List<T>> versionedLookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("versionedLookupRows");
    }

    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return (CompletableFuture<UnversionedRowset>) callMethod("selectRows");
    }

    public <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeObjectSerializer<T> serializer
    ) {
        return (CompletableFuture<List<T>>) callMethod("selectRows");
    }

    public CompletableFuture<GUID> createNode(CreateNode req) {
        return (CompletableFuture<GUID>) callMethod("createNode");
    }

    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return (CompletableFuture<Void>) callMethod("removeNode");
    }

    public CompletableFuture<Void> setNode(SetNode req) {
        return (CompletableFuture<Void>) callMethod("setNode");
    }

    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return (CompletableFuture<YTreeNode>) callMethod("getNode");
    }

    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return (CompletableFuture<YTreeNode>) callMethod("listNode");
    }

    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return (CompletableFuture<LockNodeResult>) callMethod("lockNode");
    }

    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return (CompletableFuture<GUID>) callMethod("copyNode");
    }

    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return (CompletableFuture<GUID>) callMethod("linkNode");
    }

    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return (CompletableFuture<GUID>) callMethod("modeNode");
    }

    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return (CompletableFuture<Boolean>) callMethod("existsNode");
    }

    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return (CompletableFuture<Void>) callMethod("concatenateNodes");
    }

    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return (CompletableFuture<TableReader<T>>) callMethod("readTable");
    }

    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        return (CompletableFuture<TableWriter<T>>) callMethod("writeTable");
    }

    public CompletableFuture<FileReader> readFile(ReadFile req) {
        return (CompletableFuture<FileReader>) callMethod("readFile");
    }

    public CompletableFuture<FileWriter> writeFile(WriteFile req) {
        return (CompletableFuture<FileWriter>) callMethod("writeFile");
    }

    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return (CompletableFuture<GUID>) callMethod("startOperation");
    }

    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return (CompletableFuture<TCheckPermissionResult>) callMethod("checkPermission");
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
