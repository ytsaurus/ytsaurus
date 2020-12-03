package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspMountTable;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Client that provides compound commands over YT (e.g. mount table and wait all tablets are mounted).
 */
public abstract class CompoundClient extends ApiServiceClient {
    public CompoundClient(RpcOptions options) {
        super(options);
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted) {
        return mountTable(path, cellId, freeze, waitMounted, null);
    }

    /**
     * @param requestTimeout applies only to request itself and does NOT apply to waiting for tablets to be mounted
     */
    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted, @Nullable Duration requestTimeout) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        RpcClientRequestBuilder<TReqMountTable.Builder, RpcClientResponse<TRspMountTable>> builder = service.mountTable();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);
        builder.body().setFreeze(freeze);
        if (cellId != null) {
            builder.body().setCellId(RpcUtil.toProto(cellId));
        }

        RpcUtil.apply(invoke(builder),
                response -> {
                    ScheduledExecutorService executor = response.sender().executor();
                    if (waitMounted) {
                        executor.schedule(() -> runTabletsStateChecker(path, result, executor, "mounted"), 1, TimeUnit.SECONDS);
                    } else {
                        result.complete(null);
                    }
                    return null;
                }).exceptionally(result::completeExceptionally);

        return result;
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze) {
        return mountTable(path, cellId, freeze, null);
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, @Nullable Duration requestTimeout) {
        return mountTable(path, cellId, freeze, false, requestTimeout);
    }

    public CompletableFuture<Void> mountTable(String path) {
        return mountTable(path, null);
    }

    public CompletableFuture<Void> mountTable(String path, @Nullable Duration requestTimeout) {
        return mountTable(path, null, false, requestTimeout);
    }

    public CompletableFuture<Void> mountTable(String path, boolean freeze) {
        return mountTable(path, freeze, null);
    }

    public CompletableFuture<Void> mountTable(String path, boolean freeze, @Nullable Duration requestTimeout) {
        return mountTable(path, null, freeze, requestTimeout);
    }

    public CompletableFuture<Void> unmountTable(String path, boolean force) {
        return unmountTable(path, force, null, false);
    }

    public CompletableFuture<Void> unmountTable(String path, boolean force, @Nullable Duration requestTimeout,
                                                boolean waitUnmounted) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        RpcClientRequestBuilder<TReqUnmountTable.Builder, RpcClientResponse<TRspUnmountTable>> builder =
                service.unmountTable();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);
        builder.body().setForce(force);
        RpcUtil.apply(invoke(builder), response -> {
            if (waitUnmounted) {
                ScheduledExecutorService executor = response.sender().executor();
                executor.schedule(() -> runTabletsStateChecker(path, result, executor, "unmounted"), 1, TimeUnit.SECONDS);
            } else {
                result.complete(null);
            }
            return null;
        }).exceptionally(result::completeExceptionally);
        return result;
    }

    public CompletableFuture<Void> unmountTable(String path) {
        return unmountTable(path, null);
    }

    public CompletableFuture<Void> unmountTable(String path, boolean force, boolean waitUnmounted) {
        return unmountTable(path, force, null, waitUnmounted);
    }

    public CompletableFuture<Void> unmountTable(String path, @Nullable Duration requestTimeout) {
        return unmountTable(path, false, requestTimeout, false);
    }

    public CompletableFuture<Void> remountTable(RemountTable req) {
        RpcClientRequestBuilder<TReqRemountTable.Builder, RpcClientResponse<TRspRemountTable>> builder =
                service.remountTable();
        if (req.getTimeout().isPresent()) {
            builder.setTimeout(req.getTimeout().get());
        }
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> remountTable(String path) {
        return remountTable(path, null);
    }

    public CompletableFuture<Void> remountTable(String path, @Nullable Duration requestTimeout) {
        return remountTable(new RemountTable(path).setTimeout(requestTimeout));
    }

    public CompletableFuture<Void> freezeTable(FreezeTable req) {
        RpcClientRequestBuilder<TReqFreezeTable.Builder, RpcClientResponse<TRspFreezeTable>> builder =
                service.freezeTable();
        if (req.getTimeout().isPresent()) {
            builder.setTimeout(req.getTimeout().get());
        }
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> freezeTable(String path) {
        return freezeTable(path, null);
    }

    public CompletableFuture<Void> freezeTable(String path, @Nullable Duration requestTimeout) {
        return freezeTable(new FreezeTable(path).setTimeout(requestTimeout));
    }

    public CompletableFuture<Void> unfreezeTable(FreezeTable req) {
        RpcClientRequestBuilder<TReqUnfreezeTable.Builder, RpcClientResponse<TRspUnfreezeTable>> builder =
                service.unfreezeTable();
        if (req.getTimeout().isPresent()) {
            builder.setTimeout(req.getTimeout().get());
        }
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> unfreezeTable(String path) {
        return unfreezeTable(path, null);
    }

    public CompletableFuture<Void> unfreezeTable(String path, @Nullable Duration requestTimeout) {
        return unfreezeTable(new FreezeTable(path).setTimeout(requestTimeout));
    }

    private void runTabletsStateChecker(String tablePath, CompletableFuture<Void> futureToComplete, ScheduledExecutorService executorService, String state) {
        getNode(tablePath + "/@tablets").thenAccept(tablets -> {
            List<YTreeNode> tabletPaths = tablets.asList();
            Stream<Boolean> tabletsMounted = tabletPaths.stream()
                    .map(node -> node.asMap().getOrThrow("state").stringValue().equals(state));
            if (tabletsMounted.allMatch(tableState -> tableState)) {
                futureToComplete.complete(null);
            } else {
                executorService.schedule(() -> runTabletsStateChecker(tablePath, futureToComplete, executorService, state), 1, TimeUnit.SECONDS);
            }
        }).exceptionally(e -> {
            futureToComplete.completeExceptionally(e);
            return null;
        });
    }
}
