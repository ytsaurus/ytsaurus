package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.YtTimestamp;
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
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public class ApiServiceTransaction extends TransactionalClient implements AutoCloseable {
    private final ApiServiceClient client;
    private final GUID id;
    private final YtTimestamp startTimestamp;
    private final boolean ping;
    private final boolean sticky;
    private final TransactionalOptions transactionalOptions;
    private final Duration pingPeriod;
    private final ScheduledExecutorService executor;
    private final CompletableFuture<Void> transactionCompleteFuture = new CompletableFuture<>();

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

    CompletableFuture<Void> getTransactionCompleteFuture() {
        return transactionCompleteFuture;
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

            transactionCompleteFuture.complete(null);
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
        return client.abortTransaction(id, sticky).whenComplete((result, error) -> {
            transactionCompleteFuture.complete(null);
        });
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

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request) {
        return client.lookupRows(request.setTimestamp(startTimestamp));
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(AbstractLookupRowsRequest<?> request, YTreeObjectSerializer<T> serializer) {
        return client.lookupRows(request.setTimestamp(startTimestamp), serializer);
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request) {
        return client.versionedLookupRows(request.setTimestamp(startTimestamp));
    }

    @Override
    public <T> CompletableFuture<List<T>> versionedLookupRows(AbstractLookupRowsRequest<?> request,
                                                              YTreeObjectSerializer<T> serializer) {
        return client.versionedLookupRows(request.setTimestamp(startTimestamp), serializer);
    }

    public CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(SelectRowsRequest.of(query));
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return client.selectRows(request.setTimestamp(startTimestamp));
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(SelectRowsRequest request, YTreeObjectSerializer<T> serializer) {
        return client.selectRows(request.setTimestamp(startTimestamp), serializer);
    }

    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest<?> request) {
        // TODO: hide id to request
        return client.modifyRows(id, request);
    }

    /* nodes */

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return client.createNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return client.existsNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return client.getNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return client.listNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return client.removeNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return client.setNode(req.setTransactionalOptions(transactionalOptions));
    }

    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return client.lockNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return client.copyNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return client.moveNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return client.linkNode(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return client.concatenateNodes(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return client.readTable(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        return client.writeTable(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<FileReader> readFile(ReadFile req) {
        return client.readFile(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<FileWriter> writeFile(WriteFile req) {
        return client.writeFile(req.setTransactionalOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return client.startOperation(req.setTransactionOptions(transactionalOptions));
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return client.checkPermission(req.setTransactionalOptions(transactionalOptions));
    }

    /**
     * Return address of a proxy that is used for this transaction.
     */
    @Nullable
    String getRpcProxyAddress() {
        return client.getRpcProxyAddress();
    }
}
