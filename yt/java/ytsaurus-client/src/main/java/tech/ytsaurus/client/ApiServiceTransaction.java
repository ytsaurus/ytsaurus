package tech.ytsaurus.client;

import java.time.Duration;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.Spec;
import tech.ytsaurus.client.operations.SpecPreparationContext;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
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
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.rpcproxy.EOperationType;
import tech.ytsaurus.rpcproxy.TCheckPermissionResult;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

public class ApiServiceTransaction implements TransactionalClient, AutoCloseable, Abortable {
    enum State {
        ACTIVE,
        COMMITTING,
        COMMITTED,
        CLOSED,
    }

    private static final Logger logger = LoggerFactory.getLogger(ApiServiceTransaction.class);

    private final ApiServiceClientImpl client;
    private final GUID id;
    private final YtTimestamp startTimestamp;
    private final boolean ping;
    private final boolean sticky;
    private final TransactionalOptions transactionalOptions;
    private final Duration pingPeriod;
    private final Duration failedPingRetryPeriod;
    private final ScheduledExecutorService executor;
    private final CompletableFuture<Void> transactionCompleteFuture = new CompletableFuture<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);
    private final AtomicReference<Boolean> forcedPingStop = new AtomicReference<>(false);
    private final AbstractQueue<CompletableFuture<Void>> modifyRowsResults = new ConcurrentLinkedQueue<>();
    private final Consumer<Exception> onPingFailed;

    @SuppressWarnings("checkstyle:ParameterNumber")
    ApiServiceTransaction(
            ApiServiceClientImpl client,
            GUID id,
            YtTimestamp startTimestamp,
            boolean ping,
            boolean pingAncestors,
            boolean sticky,
            Duration pingPeriod,
            ScheduledExecutorService executor
    ) {
        this(
                client,
                id,
                startTimestamp,
                ping,
                pingAncestors,
                sticky,
                pingPeriod,
                null,
                executor,
                null
        );
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    ApiServiceTransaction(
            ApiServiceClientImpl client,
            GUID id,
            YtTimestamp startTimestamp,
            boolean ping,
            boolean pingAncestors,
            boolean sticky,
            Duration pingPeriod,
            Duration failedPingRetryPeriod,
            ScheduledExecutorService executor,
            Consumer<Exception> onPingFailed
    ) {
        this.client = client;
        this.id = Objects.requireNonNull(id);
        this.startTimestamp = Objects.requireNonNull(startTimestamp);
        this.ping = ping;
        this.sticky = sticky;
        this.transactionalOptions = new TransactionalOptions(id, sticky);
        this.pingPeriod = pingPeriod;
        this.failedPingRetryPeriod = isValidPingPeriod(failedPingRetryPeriod) ? failedPingRetryPeriod : pingPeriod;
        this.executor = executor;
        this.onPingFailed = onPingFailed;

        if (isValidPingPeriod(pingPeriod)) {
            executor.schedule(this::runPeriodicPings, pingPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public TransactionalClient getRootClient() {
        return client;
    }

    @Override
    public String toString() {
        return "Transaction(" + client + ")@" + id;
    }

    public ApiServiceClient getClient() {
        return client;
    }

    public GUID getId() {
        return id;
    }

    public TransactionalOptions getTransactionalOptions() {
        return transactionalOptions;
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

    CompletableFuture<Void> getTransactionCompleteFuture() {
        return transactionCompleteFuture;
    }

    boolean isActive() {
        return state.get() == State.ACTIVE;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean isPingableState() {
        State currentState = state.get();
        return (currentState == State.ACTIVE || currentState == State.COMMITTING) && !forcedPingStop.get();
    }

    public void stopPing() {
        forcedPingStop.set(true);
    }

    private void runPeriodicPings() {
        if (!isPingableState()) {
            return;
        }

        ping().whenComplete((unused, ex) -> {
            long nextPingDelayMs;

            if (ex == null) {
                nextPingDelayMs = pingPeriod.toMillis();
            } else {
                nextPingDelayMs = failedPingRetryPeriod.toMillis();

                if (onPingFailed != null) {
                    onPingFailed.accept(ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
                }
                if (ex instanceof YTsaurusError) {
                    if (((YTsaurusError) ex).matches(YTsaurusErrorCode.NoSuchTransaction.getCode())) {
                        return;
                    }
                }
            }

            if (!isPingableState()) {
                return;
            }
            executor.schedule(this::runPeriodicPings, nextPingDelayMs, TimeUnit.MILLISECONDS);
        });
    }

    public CompletableFuture<Void> ping() {
        return client.pingTransaction(id);
    }

    private void throwWrongState(State expectedOldState, State newState) {
        // Yep, this state is outdated but Java8 doesn't have compareAndExchange,
        // so we do our best here. In any case it's a bug.
        State currentState = state.get();
        throw new IllegalStateException(
                String.format(
                        "Failed to set transaction into '%s' state; " +
                                "expected state: '%s'; current state (maybe outdated): '%s'",
                        newState,
                        expectedOldState,
                        currentState
                ));
    }

    private void updateState(State expectedOldState, State newState) {
        boolean set = state.compareAndSet(expectedOldState, newState);
        if (!set) {
            throwWrongState(expectedOldState, newState);
        }
    }

    public CompletableFuture<Void> commit() {
        updateState(State.ACTIVE, State.COMMITTING);

        List<CompletableFuture<Void>> allModifyRowsResults = new ArrayList<>();
        CompletableFuture<Void> current;
        while ((current = modifyRowsResults.poll()) != null) {
            allModifyRowsResults.add(current);
        }

        CompletableFuture<Void> allModifiesCompleted = CompletableFuture.allOf(
                allModifyRowsResults.toArray(new CompletableFuture[0])
        );

        return allModifiesCompleted.whenComplete((unused, error) -> {
            if (error != null) {
                logger.warn("Cannot commit transaction since modify rows failed:", error);
                abortImpl(false);
            }
        }).thenCompose(unused -> client.commitTransaction(id)
                .whenComplete((result, error) -> {
                    if (error == null) {
                        updateState(State.COMMITTING, State.COMMITTED);
                    } else {
                        state.set(State.CLOSED);
                    }
                    transactionCompleteFuture.complete(null);
                }));
    }

    public CompletableFuture<Void> abort() {
        return abortImpl(true);
    }

    private CompletableFuture<Void> abortImpl(boolean complainWrongState) {
        State oldState = state.getAndSet(State.CLOSED);
        if (oldState == State.ACTIVE || oldState == State.COMMITTING && !complainWrongState) {
            // Don't wait for answer.
            return client.abortTransaction(id)
                    .whenComplete((result, error) -> transactionCompleteFuture.complete(null));
        } else if (complainWrongState) {
            throwWrongState(State.ACTIVE, State.CLOSED);
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Aborts transaction unless it was committed or aborted before.
     */
    @Override
    public void close() {
        // NB. We intentionally ignore all errors of abort.
        abortImpl(false);
    }

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return client.lookupRows(request.toBuilder().setTimestamp(startTimestamp).build());
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return client.lookupRows(request.toBuilder().setTimestamp(startTimestamp).build(), serializer);
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return client.versionedLookupRows(request.toBuilder().setTimestamp(startTimestamp).build());
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(SelectRowsRequest.of(query));
    }

    @Override
    public CompletableFuture<SelectRowsResult> selectRowsV2(SelectRowsRequest request) {
        return client.selectRowsV2(request.toBuilder().setTimestamp(startTimestamp).build());
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return client.selectRows(request.toBuilder().setTimestamp(startTimestamp).build());
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer) {
        return client.selectRows(request.toBuilder().setTimestamp(startTimestamp).build(), serializer);
    }

    @Override
    public <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer,
                                                  ConsumerSource<T> consumer) {
        return client.selectRows(request.toBuilder().setTimestamp(startTimestamp).build(), serializer, consumer);
    }

    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest<?, ?> request) {
        // TODO: hide id to request
        CompletableFuture<Void> result = client.modifyRows(id, request);
        modifyRowsResults.add(result);
        return result;
    }

    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest.Builder<?, ?> request) {
        // TODO: hide id to request
        CompletableFuture<Void> result = client.modifyRows(id, request);
        modifyRowsResults.add(result);
        return result;
    }

    public CompletableFuture<Void> advanceConsumer(AdvanceConsumer req) {
        return client.advanceConsumer(req.toBuilder().setTransactionId(id).build());
    }

    /* nodes */

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return client.createNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return client.existsNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return client.getNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return client.listNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return client.removeNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return client.setNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return client.lockNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return client.copyNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return client.moveNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return client.linkNode(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return client.concatenateNodes(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<List<MultiTablePartition>> partitionTables(PartitionTables req) {
        return client.partitionTables(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return client.readTable(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req) {
        return client.readTableV2(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        return client.writeTable(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req) {
        return client.writeTableV2(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<FileReader> readFile(ReadFile req) {
        return client.readFile(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<FileWriter> writeFile(WriteFile req) {
        return client.writeFile(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return client.startOperation(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    private CompletableFuture<YTreeNode> prepareSpec(Spec spec) {
        return CompletableFuture.supplyAsync(
                () -> {
                    YTreeBuilder builder = YTree.builder();
                    spec.prepare(builder, this, new SpecPreparationContext(client.getConfig()));
                    return client.patchSpec(builder.build().mapNode());
                },
                client.getPrepareSpecExecutor());
    }

    @Override
    public CompletableFuture<Operation> startMap(MapOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(preparedSpec, transactionalReq, EOperationType.OT_MAP)
        );
    }

    @Override
    public CompletableFuture<Operation> startReduce(ReduceOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(preparedSpec, transactionalReq, EOperationType.OT_REDUCE)
        );
    }

    @Override
    public CompletableFuture<Operation> startSort(SortOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(preparedSpec, transactionalReq, EOperationType.OT_SORT)
        );
    }

    @Override
    public CompletableFuture<Operation> startMapReduce(MapReduceOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(
                        preparedSpec, transactionalReq, EOperationType.OT_MAP_REDUCE)
        );
    }

    @Override
    public CompletableFuture<Operation> startMerge(MergeOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(preparedSpec, transactionalReq, EOperationType.OT_MERGE)
        );
    }

    @Override
    public CompletableFuture<Operation> startRemoteCopy(RemoteCopyOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(
                        preparedSpec, transactionalReq, EOperationType.OT_REMOTE_COPY)
        );
    }

    @Override
    public CompletableFuture<Operation> startVanilla(VanillaOperation req) {
        var transactionalReq = req.toBuilder().setTransactionalOptions(transactionalOptions).build();
        return prepareSpec(transactionalReq.getSpec()).thenCompose(
                preparedSpec -> client.startPreparedOperation(preparedSpec, transactionalReq, EOperationType.OT_VANILLA)
        );
    }

    @Override
    public Operation attachOperation(GUID operationId) {
        return client.attachOperation(operationId);
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return client.checkPermission(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<GetFileFromCacheResult> getFileFromCache(GetFileFromCache req) {
        return client.getFileFromCache(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    @Override
    public CompletableFuture<PutFileToCacheResult> putFileToCache(PutFileToCache req) {
        return client.putFileToCache(req.toBuilder().setTransactionalOptions(transactionalOptions).build());
    }

    /**
     * Return address of a proxy that is used for this transaction.
     */
    @Nullable
    String getRpcProxyAddress() {
        return client.getRpcProxyAddress();
    }

    private static boolean isValidPingPeriod(Duration pingPeriod) {
        return pingPeriod != null && !pingPeriod.isZero() && !pingPeriod.isNegative();
    }
}
