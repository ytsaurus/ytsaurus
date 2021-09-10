package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.ConsumerSourceRet;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.proxy.request.AbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
import ru.yandex.yt.ytclient.proxy.request.AlterTableReplica;
import ru.yandex.yt.ytclient.proxy.request.Atomicity;
import ru.yandex.yt.ytclient.proxy.request.BuildSnapshot;
import ru.yandex.yt.ytclient.proxy.request.CheckPermission;
import ru.yandex.yt.ytclient.proxy.request.CommitTransaction;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.CreateObject;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.GcCollect;
import ru.yandex.yt.ytclient.proxy.request.GenerateTimestamps;
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.GetOperation;
import ru.yandex.yt.ytclient.proxy.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.GetTabletInfos;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.PingTransaction;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.ReadTableDirect;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.ReshardTable;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.TableReplicaMode;
import ru.yandex.yt.ytclient.proxy.request.TabletInfo;
import ru.yandex.yt.ytclient.proxy.request.TabletInfoReplica;
import ru.yandex.yt.ytclient.proxy.request.TrimTable;
import ru.yandex.yt.ytclient.proxy.request.UnfreezeTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

/**
 * Клиент для высокоуровневой работы с ApiService
 */
public class ApiServiceClient extends TransactionalClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    @Nonnull private final Executor heavyExecutor;
    @Nullable private final RpcClient rpcClient;
    @Nonnull final RpcOptions rpcOptions;

    private ApiServiceClient(
            @Nullable RpcClient client,
            @Nonnull RpcOptions options,
            @Nonnull Executor heavyExecutor
    ) {
        OutageController outageController = options.getTestingOptions().getOutageController();
        if (client != null && outageController != null) {
            this.rpcClient = new OutageRpcClient(client, outageController);
        } else {
            this.rpcClient = client;
        }
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
        this.rpcOptions = options;
    }

    public ApiServiceClient(@Nullable RpcClient client, RpcOptions options) {
        this(client, options, ForkJoinPool.commonPool());
    }

    public ApiServiceClient(RpcOptions options) {
        this(null, options);
    }

    public ApiServiceClient(RpcClient client) {
        this(client, new RpcOptions());
    }

    /**
     * Start new master or tablet transaction.
     * @see StartTransaction
     */
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        RpcClientRequestBuilder<TReqStartTransaction.Builder, TRspStartTransaction> builder =
                ApiServiceMethodTable.START_TRANSACTION.createRequestBuilder(rpcOptions);
        return RpcUtil.apply(sendRequest(startTransaction, builder), response -> {
            GUID id = RpcUtil.fromProto(response.body().getId());
            YtTimestamp startTimestamp = YtTimestamp.valueOf(response.body().getStartTimestamp());
            RpcClient sender = response.sender();
            ApiServiceTransaction result;
            if (rpcClient != null && rpcClient.equals(sender)) {
                result = new ApiServiceTransaction(
                        this,
                        id,
                        startTimestamp,
                        startTransaction.getPing(),
                        startTransaction.getPingAncestors(),
                        startTransaction.getSticky(),
                        startTransaction.getPingPeriod().orElse(null),
                        sender.executor());
            } else {
                result = new ApiServiceTransaction(
                        sender,
                        rpcOptions,
                        id,
                        startTimestamp,
                        startTransaction.getPing(),
                        startTransaction.getPingAncestors(),
                        startTransaction.getSticky(),
                        startTransaction.getPingPeriod().orElse(null),
                        sender.executor());
            }

            sender.ref();
            result.getTransactionCompleteFuture().whenComplete((ignored, ex) -> sender.unref());
            logger.debug("New transaction {} has started by {}", id, builder);
            return result;
        });
    }

    public CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        return startTransaction(options.toStartTransaction());
    }

    /**
     * Ping existing transaction
     * @see PingTransaction
     */
    public CompletableFuture<Void> pingTransaction(PingTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.PING_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> pingTransaction(GUID id) {
        return pingTransaction(new PingTransaction(id));
    }

    /**
     * Commit existing transaction
     * @see CommitTransaction
     */
    public CompletableFuture<Void> commitTransaction(CommitTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.COMMIT_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> commitTransaction(GUID id) {
        return commitTransaction(new CommitTransaction(id));
    }

    /**
     * Abort existing transaction
     * @see AbortTransaction
     */
    public CompletableFuture<Void> abortTransaction(AbortTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> abortTransaction(GUID id) {
        return abortTransaction(new AbortTransaction(id));
    }

    /* nodes */

    /**
     * Get cypress node
     * @see GetNode
     */
    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_NODE.createRequestBuilder(rpcOptions)),
                response -> parseByteString(response.body().getValue()));
    }

    /**
     * List cypress node
     * @see ListNode
     */
    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LIST_NODE.createRequestBuilder(rpcOptions)),
                response -> parseByteString(response.body().getValue()));
    }

    /**
     * Set cypress node
     * @see SetNode
     */
    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.SET_NODE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Check if cypress node exists
     * @see ExistsNode
     */
    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.EXISTS_NODE.createRequestBuilder(rpcOptions)),
                response -> response.body().getExists());
    }

    /**
     * Get table pivot keys.
     * @see GetTablePivotKeys
     */
    public CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_TABLE_PIVOT_KEYS.createRequestBuilder(rpcOptions)),
                response -> YTreeBinarySerializer.deserialize(
                        new ByteArrayInputStream(response.body().getValue().toByteArray())
                ).asList());
    }

    /**
     * Create new master object.
     * @see CreateObject
     */
    public CompletableFuture<GUID> createObject(CreateObject req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CREATE_OBJECT.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getObjectId()));
    }

    /**
     * Create cypress node
     * @see CreateNode
     */
    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CREATE_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Remove cypress node
     * @see RemoveNode
     */
    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.REMOVE_NODE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Lock cypress node
     * @see LockNode
     */
    @Override
    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LOCK_NODE.createRequestBuilder(rpcOptions)),
                response -> new LockNodeResult(
                        RpcUtil.fromProto(response.body().getNodeId()),
                        RpcUtil.fromProto(response.body().getLockId())));
    }

    /**
     * Copy cypress node
     * @see CopyNode
     */
    @Override
    public CompletableFuture<GUID> copyNode(CopyNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.COPY_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Move cypress node
     * @see MoveNode
     */
    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.MOVE_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Link cypress node
     * @see LinkNode
     */
    @Override
    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LINK_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Concatenate nodes
     * @see ConcatenateNodes
     */
    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CONCATENATE_NODES.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    // TODO: TReqAttachTransaction

    /* */

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?> request) {
        return lookupRowsImpl(request, response ->
                ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                        response.attachments()));
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer
    ) {
        return lookupRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return lookupRowsImpl(request, response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer);
            return null;
        });
    }

    private <T> CompletableFuture<T> lookupRowsImpl(
            AbstractLookupRowsRequest<?> request,
            Function<RpcClientResponse<TRspLookupRows>, T> responseReader
    ) {
        return handleHeavyResponse(
                sendRequest(
                        request.asLookupRowsWritable(),
                        ApiServiceMethodTable.LOOKUP_ROWS.createRequestBuilder(rpcOptions)
                ),
                response -> {
                    logger.trace("LookupRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
                    return responseReader.apply(response);
                });
    }

    @Deprecated
    public CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        return lookupRows(request.setTimestamp(timestamp));
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?> request) {
        return versionedLookupRowsImpl(request, response -> ApiServiceUtil
                .deserializeVersionedRowset(response.body().getRowsetDescriptor(), response.attachments()));
    }

    @Override
    public <T> CompletableFuture<List<T>> versionedLookupRows(AbstractLookupRowsRequest<?> request,
                                                              YTreeObjectSerializer<T> serializer) {
        return versionedLookupRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeVersionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> versionedLookupRows(
            LookupRowsRequest request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return versionedLookupRowsImpl(request, response -> {
            ApiServiceUtil.deserializeVersionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer);
            return null;
        });
    }

    private <T> CompletableFuture<T> versionedLookupRowsImpl(
            AbstractLookupRowsRequest<?> request,
            Function<RpcClientResponse<TRspVersionedLookupRows>, T> responseReader
    ) {
        return handleHeavyResponse(
                sendRequest(
                        request.asVersionedLookupRowsWritable(),
                        ApiServiceMethodTable.VERSIONED_LOOKUP_ROWS.createRequestBuilder(rpcOptions)
                ),
                response -> {
                    logger.trace("VersionedLookupRows incoming rowset descriptor: {}",
                            response.body().getRowsetDescriptor());
                    return responseReader.apply(response);
                });
    }

    @Deprecated
    public CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        return versionedLookupRows(request.setTimestamp(timestamp));
    }

    public CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(query, null);
    }

    public CompletableFuture<UnversionedRowset> selectRows(String query, @Nullable Duration requestTimeout) {
        return selectRows(SelectRowsRequest.of(query).setTimeout(requestTimeout));
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return selectRowsImpl(request, response ->
                ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                        response.attachments()));
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(SelectRowsRequest request, YTreeObjectSerializer<T> serializer) {
        return selectRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeObjectSerializer<T> serializer,
                                                  ConsumerSource<T> consumer) {
        return selectRowsImpl(request, response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer);
            return null;
        });
    }

    private <T> CompletableFuture<T> selectRowsImpl(SelectRowsRequest request,
                                                    Function<RpcClientResponse<TRspSelectRows>, T> responseReader) {
        return handleHeavyResponse(
                sendRequest(request, ApiServiceMethodTable.SELECT_ROWS.createRequestBuilder(rpcOptions)),
                response -> {
                    logger.trace("SelectRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
                    return responseReader.apply(response);
                });
    }

    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request) {
        return RpcUtil.apply(
                sendRequest(
                        new ModifyRowsWrapper(transactionId, request),
                        ApiServiceMethodTable.MODIFY_ROWS.createRequestBuilder(rpcOptions)
                ),
                response -> null);
    }

    // TODO: TReqBatchModifyRows

    public CompletableFuture<Long> buildSnapshot(BuildSnapshot req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.BUILD_SNAPSHOT.createRequestBuilder(rpcOptions)),
                response -> response.body().getSnapshotId());
    }

    public CompletableFuture<Void> gcCollect(GcCollect req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GC_COLLECT.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    public CompletableFuture<Void> mountTable(MountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.MOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see CompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    public CompletableFuture<Void> unmountTable(UnmountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.UNMOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> remountTable(String path) {
        return remountTable(new RemountTable(path));
    }

    public CompletableFuture<Void> remountTable(RemountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.REMOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> freezeTable(String path) {
        return freezeTable(path, null);
    }

    public CompletableFuture<Void> freezeTable(String path, @Nullable Duration requestTimeout) {
        return freezeTable(new FreezeTable(path).setTimeout(requestTimeout));
    }

    public CompletableFuture<Void> freezeTable(FreezeTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.FREEZE_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> unfreezeTable(String path) {
        return unfreezeTable(path, null);
    }

    public CompletableFuture<Void> unfreezeTable(String path, @Nullable Duration requestTimeout) {
        return unfreezeTable(new UnfreezeTable(path).setTimeout(requestTimeout));
    }

    public CompletableFuture<Void> unfreezeTable(FreezeTable req) {
        UnfreezeTable unfreezeReq = new UnfreezeTable(req.getPath());
        if (req.getTimeout().isPresent()) {
            unfreezeReq.setTimeout(req.getTimeout().get());
        }
        return unfreezeTable(unfreezeReq);
    }

    public CompletableFuture<Void> unfreezeTable(UnfreezeTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.UNFREEZE_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp) {
        return RpcUtil.apply(
                sendRequest(
                        new GetInSyncReplicasWrapper(timestamp, request),
                        ApiServiceMethodTable.GET_IN_SYNC_REPLICAS.createRequestBuilder(rpcOptions)
                ),
                response -> response.body().getReplicaIdsList()
                        .stream().map(RpcUtil::fromProto).collect(Collectors.toList()));
    }

    @Deprecated
    public CompletableFuture<List<GUID>> getInSyncReplicas(
            String path,
            YtTimestamp timestamp,
            TableSchema schema,
            Iterable<? extends List<?>> keys
    ) {
        return getInSyncReplicas(new GetInSyncReplicas(path, schema, keys), timestamp);
    }

    public CompletableFuture<List<TabletInfo>> getTabletInfos(GetTabletInfos req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_TABLET_INFOS.createRequestBuilder(rpcOptions)),
                response ->
                        response.body().getTabletsList()
                                .stream()
                                .map(x -> {
                                    List<TabletInfoReplica> replicas = x.getReplicasList().stream()
                                            .map(o -> new TabletInfoReplica(RpcUtil.fromProto(o.getReplicaId()),
                                                    o.getLastReplicationTimestamp()))
                                            .collect(Collectors.toList());
                                    return new TabletInfo(x.getTotalRowCount(), x.getTrimmedRowCount(),
                                            x.getLastWriteTimestamp(), replicas);
                                })
                                .collect(Collectors.toList()));
    }

    public CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices) {
        GetTabletInfos req = new GetTabletInfos(path);
        req.setTabletIndexes(tabletIndices);
        return getTabletInfos(req);
    }

    public CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GENERATE_TIMESTAMPS.createRequestBuilder(rpcOptions)),
                response -> YtTimestamp.valueOf(response.body().getTimestamp()));
    }

    public CompletableFuture<YtTimestamp> generateTimestamps(int count) {
        GenerateTimestamps req = new GenerateTimestamps(count);
        return generateTimestamps(req);
    }

    public CompletableFuture<YtTimestamp> generateTimestamps() {
        return generateTimestamps(1);
    }

    /* tables */
    public CompletableFuture<Void> reshardTable(ReshardTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.RESHARD_TABLE.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    public CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        TrimTable req = new TrimTable(path, tableIndex, trimmedRowCount);
        return trimTable(req);
    }

    public CompletableFuture<Void> trimTable(TrimTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.TRIM_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> alterTable(AlterTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ALTER_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    public CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity
    ) {
        TableReplicaMode convertedMode;
        switch (mode) {
            case TRM_ASYNC:
                convertedMode = TableReplicaMode.Async;
                break;
            case TRM_SYNC:
                convertedMode = TableReplicaMode.Sync;
                break;
            default:
                throw new IllegalArgumentException();
        }
        Atomicity convertedAtomicity;
        switch (atomicity) {
            case A_FULL:
                convertedAtomicity = Atomicity.Full;
                break;
            case A_NONE:
                convertedAtomicity = Atomicity.None;
                break;
            default:
                throw new IllegalArgumentException();
        }

        return alterTableReplica(
                new AlterTableReplica(replicaId)
                        .setEnabled(enabled)
                        .setMode(convertedMode)
                        .setPreserveTimestamps(preserveTimestamp)
                        .setAtomicity(convertedAtomicity)
        );
    }

    public CompletableFuture<Void> alterTableReplica(AlterTableReplica req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ALTER_TABLE_REPLICA.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<GUID> startOperation(StartOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.START_OPERATION.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getOperationId()));
    }

    public CompletableFuture<YTreeNode> getOperation(GetOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_OPERATION.createRequestBuilder(rpcOptions)),
                response -> parseByteString(response.body().getMeta())
        );
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CHECK_PERMISSION.createRequestBuilder(rpcOptions)),
                response -> response.body().getResult());
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return readTable(req, new TableAttachmentWireProtocolReader<>(req.getDeserializer()));
    }

    public CompletableFuture<TableReader<byte[]>> readTableDirect(ReadTableDirect req) {
        return readTable(req, TableAttachmentReader.BYPASS);
    }

    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req,
                                                           TableAttachmentReader<T> reader) {
        RpcClientRequestBuilder<TReqReadTable.Builder, TRspReadTable>
                builder = ApiServiceMethodTable.READ_TABLE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        TableReaderImpl<T> tableReader = new TableReaderImpl<>(reader);
        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableReader);
        CompletableFuture<TableReader<T>> result = streamControlFuture.thenCompose(
                control -> tableReader.waitMetadata());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        RpcClientRequestBuilder<TReqWriteTable.Builder, TRspWriteTable>
                builder = ApiServiceMethodTable.WRITE_TABLE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        TableWriterImpl<T> tableWriter = new TableWriterImpl<>(
                req.getWindowSize(),
                req.getPacketSize(),
                req.getSerializer());

        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
        CompletableFuture<TableWriter<T>> result = streamControlFuture
                .thenCompose(control -> tableWriter.startUpload());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public CompletableFuture<FileReader> readFile(ReadFile req) {
        RpcClientRequestBuilder<TReqReadFile.Builder, TRspReadFile>
                builder = ApiServiceMethodTable.READ_FILE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        FileReaderImpl fileReader = new FileReaderImpl();
        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, fileReader);
        CompletableFuture<FileReader> result = streamControlFuture.thenCompose(
                control -> fileReader.waitMetadata());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public CompletableFuture<FileWriter> writeFile(WriteFile req) {
        RpcClientRequestBuilder<TReqWriteFile.Builder, TRspWriteFile>
                builder = ApiServiceMethodTable.WRITE_FILE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        FileWriterImpl fileWriter = new FileWriterImpl(
                req.getWindowSize(),
                req.getPacketSize());
        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, fileWriter);
        CompletableFuture<FileWriter> result = streamControlFuture.thenCompose(control -> fileWriter.startUpload());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    /* */

    private <T, Response> CompletableFuture<T> handleHeavyResponse(CompletableFuture<Response> future,
                                                                   Function<Response, T> fn) {
        return RpcUtil.applyAsync(future, fn, heavyExecutor);
    }

    protected <RequestType extends MessageLite.Builder, ResponseType extends MessageLite>
    CompletableFuture<RpcClientResponse<ResponseType>>
    invoke(RpcClientRequestBuilder<RequestType, ResponseType> builder) {
        return builder.invoke(rpcClient);
    }

    protected <RequestType extends MessageLite.Builder, ResponseType extends MessageLite>
    CompletableFuture<RpcClientStreamControl>
    startStream(RpcClientRequestBuilder<RequestType, ResponseType> builder, RpcStreamConsumer consumer) {
        RpcClientStreamControl control = rpcClient.startStream(
                rpcClient,
                builder.getRpcRequest(),
                consumer,
                builder.getOptions()
        );

        return CompletableFuture.completedFuture(control);
    }

    private <RequestMsgBuilder extends MessageLite.Builder, ResponseMsg extends MessageLite,
            RequestType extends HighLevelRequest<RequestMsgBuilder>>
    CompletableFuture<RpcClientResponse<ResponseMsg>>
    sendRequest(RequestType req, RpcClientRequestBuilder<RequestMsgBuilder, ResponseMsg> builder) {
        logger.debug("Starting request {}; {}", builder, req.getArgumentsLogString());
        req.writeHeaderTo(builder.header());
        req.writeTo(builder);
        return invoke(builder);
    }

    @Override
    public String toString() {
        return rpcClient != null ? rpcClient.toString() : super.toString();
    }

    @Nullable
    String getRpcProxyAddress() {
        if (rpcClient == null) {
            return null;
        }
        return rpcClient.getAddressString();
    }

    private static YTreeNode parseByteString(ByteString byteString) {
        return YTreeBinarySerializer.deserialize(byteString.newInput());
    }
}

@NonNullApi
@NonNullFields
class ModifyRowsWrapper implements HighLevelRequest<TReqModifyRows.Builder> {
    private final GUID transactionId;
    private final AbstractModifyRowsRequest<?> request;

    ModifyRowsWrapper(GUID transactionId, AbstractModifyRowsRequest<?> request) {
        this.transactionId = transactionId;
        this.request = request;
    }

    @Override
    public String getArgumentsLogString() {
        return "TransactionId: " + transactionId + "; ";
    }

    @Override
    public void writeHeaderTo(TRequestHeader.Builder header) {
        request.writeHeaderTo(header);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqModifyRows.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
        builder.body().setPath(request.getPath());
        if (request.getRequireSyncReplica().isPresent()) {
            builder.body().setRequireSyncReplica(request.getRequireSyncReplica().get());
        }
        builder.body().addAllRowModificationTypes(request.getRowModificationTypes());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));
        request.serializeRowsetTo(builder.attachments());
    }
}

@NonNullApi
@NonNullFields
class GetInSyncReplicasWrapper implements HighLevelRequest<TReqGetInSyncReplicas.Builder> {
    private final YtTimestamp timestamp;
    private final GetInSyncReplicas request;

    GetInSyncReplicasWrapper(YtTimestamp timestamp, GetInSyncReplicas request) {
        this.timestamp = timestamp;
        this.request = request;
    }

    @Override
    public String getArgumentsLogString() {
        return "Path: " + request.getPath() +
                "; Timestamp: " + timestamp + "; ";
    }

    @Override
    public void writeHeaderTo(TRequestHeader.Builder header) {
        request.writeHeaderTo(header);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, ?> builder) {
        builder.body().setPath(request.getPath());
        builder.body().setTimestamp(timestamp.getValue());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));

        request.serializeRowsetTo(builder.attachments());
    }
}
