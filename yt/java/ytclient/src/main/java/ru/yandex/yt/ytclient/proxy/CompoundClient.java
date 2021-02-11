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
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

/**
 * Client that provides compound commands over YT (e.g. mount table and wait all tablets are mounted).
 */
public abstract class CompoundClient extends ApiServiceClient {
    private final ScheduledExecutorService executorService;

    public CompoundClient(ScheduledExecutorService executorService, RpcOptions options) {
        super(options);
        this.executorService = executorService;
    }

    public CompletableFuture<Void> mountTableAndWaitTablets(MountTable req) {
        return mountTable(req).thenCompose(res -> waitTabletState(req.getPath(), "mounted"));
    }

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted) {
        return mountTable(path, cellId, freeze, waitMounted, null);
    }

    /**
     * @param requestTimeout applies only to request itself and does NOT apply to waiting for tablets to be mounted
     */
    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted, @Nullable Duration requestTimeout) {
        MountTable req = new MountTable(path);
        if (cellId != null) {
            req.setCellId(cellId);
        }
        req.setFreeze(freeze);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        if (waitMounted) {
            return mountTableAndWaitTablets(req);
        } else {
            return mountTable(req);
        }
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

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)} or {@link #unmountTable(String)}.
     */
    @Deprecated
    public CompletableFuture<Void> unmountTable(String path, boolean force) {
        return unmountTable(path, force, null, false);
    }

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see ApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     */
    public CompletableFuture<Void> unmountTableAndWaitTablets(UnmountTable req) {
        String path = req.getPath();
        return unmountTable(req).thenCompose(rsp -> waitTabletState(path, "unmounted"));
    }

    /**
     * Unmount table and wait until all tablets become unmounted.
     *
     * @see UnmountTable
     * @see ApiServiceClient#unmountTable(UnmountTable)
     */
    public CompletableFuture<Void> unmountTableAndWaitTablets(String path) {
        return unmountTableAndWaitTablets(new UnmountTable(path));
    }

    /**
     * Unmount table.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)}
     * or {@link #unmountTableAndWaitTablets(UnmountTable)} .
     */
    @Deprecated
    public CompletableFuture<Void> unmountTable(String path, boolean force, @Nullable Duration requestTimeout,
                                                boolean waitUnmounted) {
        UnmountTable req = new UnmountTable(path);
        req.setForce(force);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        if (waitUnmounted) {
            return unmountTableAndWaitTablets(req);
        } else {
            return unmountTable(req);
        }
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * @see ApiServiceClient#unmountTable(UnmountTable)
     * @see UnmountTable
     * @see #unmountTableAndWaitTablets(UnmountTable)
     * @see #unmountTableAndWaitTablets(String)
     */
    public CompletableFuture<Void> unmountTable(String path) {
        return unmountTable(path, null);
    }

    /**
     * Unmount table.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated Prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)}
     * or {@link #unmountTableAndWaitTablets(UnmountTable)} .
     */
    @Deprecated
    public CompletableFuture<Void> unmountTable(String path, boolean force, boolean waitUnmounted) {
        return unmountTable(path, force, null, waitUnmounted);
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * <b>Dangerous:</b> using force flag is dangerous, check {@link UnmountTable}
     *
     * @deprecated prefer to use {@link ApiServiceClient#unmountTable(UnmountTable)} or {@link #unmountTable(String)}.
     */
    @Deprecated
    public CompletableFuture<Void> unmountTable(String path, @Nullable Duration requestTimeout) {
        return unmountTable(path, false, requestTimeout, false);
    }

    private void runTabletsStateChecker(String tablePath, CompletableFuture<Void> futureToComplete, String state) {
        getNode(tablePath + "/@tablets").thenAccept(tablets -> {
            List<YTreeNode> tabletPaths = tablets.asList();
            Stream<Boolean> tabletsMounted = tabletPaths.stream()
                    .map(node -> node.asMap().getOrThrow("state").stringValue().equals(state));
            if (tabletsMounted.allMatch(tableState -> tableState)) {
                futureToComplete.complete(null);
            } else {
                executorService.schedule(() -> runTabletsStateChecker(tablePath, futureToComplete, state), 1, TimeUnit.SECONDS);
            }
        }).exceptionally(e -> {
            futureToComplete.completeExceptionally(e);
            return null;
        });
    }

    private CompletableFuture<Void> waitTabletState(String tablePath, String targetState) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        runTabletsStateChecker(tablePath, result, targetState);
        return result;
    }
}
