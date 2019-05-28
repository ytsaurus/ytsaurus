package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.ytclient.misc.YtTimestamp;
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
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public class ApiServiceTransaction implements AutoCloseable, TransactionalClient {
    private final ApiServiceClient client;
    private final GUID id;
    private final YtTimestamp startTimestamp;
    private final boolean ping;
    private final boolean sticky;
    private final TransactionalOptions transactionalOptions;
    private final Duration pingPeriod;
    private final ScheduledExecutorService executor;

    enum State {
        ACTIVE,
        COMMITTING,
        COMMITTED,
        ABORTED
    }

    @Override
    public String toString() {
        return "Transaction(" + client + ")@" + id;
    }

    private final Object stateLock = new Object();
    private State state;

    public ApiServiceClient getClient() {
        return client;
    }

    public GUID getId() {
        return id;
    }

    public YtTimestamp getStartTimestamp() {
        return startTimestamp;
    }

    public boolean isPing() {
        return ping;
    }

    public boolean isSticky() {
        return sticky;
    }


    ApiServiceTransaction(
            ApiServiceClient client,
            GUID id,
            YtTimestamp startTimestamp,
            boolean ping,
            boolean pingAncestors,
            boolean sticky,
            Duration pingPeriod,
            ScheduledExecutorService executor)
    {
        this.client = client;
        this.id = Objects.requireNonNull(id);
        this.startTimestamp = Objects.requireNonNull(startTimestamp);
        this.ping = ping;
        this.sticky = sticky;
        this.transactionalOptions = new TransactionalOptions(id, ping, pingAncestors, sticky);
        this.state = State.ACTIVE;
        this.pingPeriod = pingPeriod;
        this.executor = executor;

        if (ping && ! pingPeriod.isZero() && ! pingPeriod.isNegative()) {
            runPeriodicPings();
        }
    }

    ApiServiceTransaction(
            RpcClient rpcClient,
            RpcOptions rpcOptions,
            GUID id,
            YtTimestamp startTimestamp,
            boolean ping,
            boolean pingAncestors,
            boolean sticky,
            Duration pingPeriod,
            ScheduledExecutorService executor)
    {
        this(
                new ApiServiceClient(Objects.requireNonNull(rpcClient), rpcOptions),
                id,
                startTimestamp,
                ping,
                pingAncestors,
                sticky,
                pingPeriod,
                executor
        );
    }

    private State getState() {
        synchronized (stateLock) {
            return state;
        }
    }

    private boolean isPingableState() {
        State state = getState();
        return state == State.ACTIVE /*|| state == State.COMMITTING*/;
    }

    private void runPeriodicPings() {
        if (!isPingableState()) {
            return;
        }

        ping().thenAccept((unused) -> {
            if (!isPingableState()) {
                return;
            }

            executor.schedule(() -> {
                runPeriodicPings();
                return null;
            }, pingPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }).exceptionally((ex) -> {
            // TODO check timeout here?
            return null;
        });
    }

    public CompletableFuture<Void> ping() {
        return client.pingTransaction(id, sticky);
    }

    private void setCommitted() {
        synchronized (stateLock) {
            if (state != State.COMMITTING) {
                throw new IllegalStateException(String.format("Transaction '%s' is already being committed", id));
            }

            state = State.COMMITTED;
        }
    }

    private void setAborted() {
        synchronized (stateLock) {
            state = State.ABORTED;
        }
    }

    public CompletableFuture<Void> commit() {
        synchronized (stateLock) {
            switch (state) {
                case COMMITTED:
                    throw new IllegalStateException(String.format("Transaction '%s' is already committed", id));
                case COMMITTING:
                    throw new IllegalStateException(String.format("Transaction '%s' is already being committed", id));
                case ABORTED:
                    throw new IllegalStateException(String.format("Transaction '%s' is already aborted", id));
                default:
                    state = State.COMMITTING;
                    break;
            }
        }

        return client.commitTransaction(id, sticky).whenComplete((result, error) -> {
            if (error == null) {
                setCommitted();
            } else {
                setAborted();
            }
        });
    }

    public CompletableFuture<Void> abort() {
        State state = getState();
        if (state != State.ACTIVE) {
            throw new IllegalStateException(String.format("Transaction '%s' is closed", id));
        }

        synchronized (stateLock) {
            this.state = State.ABORTED;
        }

        // dont wait for answer
        return client.abortTransaction(id, sticky);
    }

    @Override
    public void close() {
        CompletableFuture<Void> future;
        try {
            future = abort();
        } catch (IllegalStateException ignored) {
            // транзакция уже закрыта
            return;
        }
        try {
            future.join();
        } catch (CancellationException | CompletionException ignored) {
            // игнорируем ошибки abort'а
        } finally {
            synchronized (stateLock) {
                this.state = State.ABORTED;
            }
        }
    }

    public CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request) {
        return client.lookupRows(request.setTimestamp(startTimestamp));
    }

    public CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request) {
        return client.versionedLookupRows(request.setTimestamp(startTimestamp));
    }

    public CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(SelectRowsRequest.of(query));
    }

    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return client.selectRows(request.setTimestamp(startTimestamp));
    }

    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest request) {
        return client.modifyRows(id, request);
    }

    /* nodes */

    public CompletableFuture<GUID> createNode(CreateNode req) {
        return client.createNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<GUID> createNode(String path, ObjectType type) {
        return createNode(new CreateNode(path, type));
    }

    public CompletableFuture<GUID> createNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        return createNode(new CreateNode(path, type, attributes));
    }

    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return client.existsNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<Boolean> existsNode(String path) {
        return existsNode(new ExistsNode(path));
    }

    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return client.getNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<YTreeNode> getNode(String path) {
        return getNode(new GetNode(path));
    }

    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return client.listNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<YTreeNode> listNode(String path) {
        return listNode(new ListNode(path));
    }

    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return client.removeNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<Void> removeNode(String path) {
        return removeNode(new RemoveNode(path));
    }

    public CompletableFuture<Void> setNode(SetNode req) {
        return client.setNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<Void> setNode(String path, byte[] data) {
        return setNode(new SetNode(path, data));
    }

    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return setNode(path, data.toBinary());
    }

    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return client.lockNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode) {
        return lockNode(new LockNode(path, mode));
    }

    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return client.copyNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<GUID> copyNode(String src, String dst) {
        return copyNode(new CopyNode(src, dst));
    }

    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return client.moveNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<GUID> moveNode(String from, String to) {
        return moveNode(new MoveNode(from, to));
    }

    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return client.linkNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<GUID> linkNode(String src, String dst) {
        return linkNode(new LinkNode(src, dst));
    }

    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return client.concatenateNodes(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<Void> concatenateNodes(String [] from, String to) {
        return concatenateNodes(new ConcatenateNodes(from, to));
    }

    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return client.startOperation(req.setTransactionOptions(transactionalOptions));
    }

    CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return client.checkPermission(req.setTransactionalOptions(transactionalOptions));
    }
}
