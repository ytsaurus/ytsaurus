package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqAddMember;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.rpcproxy.TReqAlterTableReplica;
import ru.yandex.yt.rpcproxy.TReqCheckPermission;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqGetTabletInfos;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqRemoveMember;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.rpcproxy.TReqReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TRspAddMember;
import ru.yandex.yt.rpcproxy.TRspAlterTable;
import ru.yandex.yt.rpcproxy.TRspAlterTableReplica;
import ru.yandex.yt.rpcproxy.TRspCheckPermission;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TRspGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TRspGetTabletInfos;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspMountTable;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspRemoveMember;
import ru.yandex.yt.rpcproxy.TRspReshardTable;
import ru.yandex.yt.rpcproxy.TRspReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspStartOperation;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspTrimTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.ConsumerSourceRet;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.proxy.request.AbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
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
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
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
import ru.yandex.yt.ytclient.proxy.request.TabletInfo;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceClient;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

/**
 * Клиент для высокоуровневой работы с ApiService
 */
public class ApiServiceClient extends TransactionalClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    private final ApiService service;
    @Nonnull private final Executor heavyExecutor;
    @Nullable private final RpcClient rpcClient;
    @Nonnull private final RpcOptions rpcOptions;

    private ApiServiceClient(
            @Nullable RpcClient client,
            @Nonnull RpcOptions options,
            @Nonnull ApiService service,
            @Nonnull Executor heavyExecutor)
    {
        this.service = Objects.requireNonNull(service);
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
        this.rpcClient = client;
        this.rpcOptions = options;
    }

    private ApiServiceClient(@Nullable RpcClient client, RpcOptions options, ApiService service) {
        this(client, options, service, ForkJoinPool.commonPool());
    }

    public ApiServiceClient(RpcClient client, RpcOptions options) {
        this(client, options, RpcServiceClient.create(ApiService.class, options));
    }

    public ApiServiceClient(RpcOptions options) {
        this(null, options, RpcServiceClient.create(ApiService.class, options));
    }

    public ApiServiceClient(RpcClient client) {
        this(client, new RpcOptions());
    }

    public ApiService getService() {
        return service;
    }

    /**
     * Start new master or tablet transaction.
     * @see StartTransaction
     */
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> builder =
                service.startTransaction();
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
                sendRequest(req, service.pingTransaction()),
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
                sendRequest(req, service.commitTransaction()),
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
                sendRequest(req, service.abortTransaction()),
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
                sendRequest(req, service.getNode()),
                response -> parseByteString(response.body().getValue()));
    }

    /**
     * List cypress node
     * @see ListNode
     */
    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.listNode()),
                response -> parseByteString(response.body().getValue()));
    }

    /**
     * Set cypress node
     * @see SetNode
     */
    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.setNode()),
                response -> null);
    }

    /**
     * Check if cypress node exists
     * @see ExistsNode
     */
    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.existsNode()),
                response -> response.body().getExists());
    }

    /**
     * Get table pivot keys.
     * @see GetTablePivotKeys
     */
    public CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req) {
        return RpcUtil.apply(
                sendRequest(req, service.getTablePivotKeys()),
                response -> YTreeBinarySerializer.deserialize(
                        new ByteArrayInputStream(response.body().getValue().toByteArray())
                ).asList());
    }

    public CompletableFuture<List<YTreeNode>> getTablePivotKeys(String path, @Nullable Duration requestTimeout) {
        GetTablePivotKeys req = new GetTablePivotKeys(path);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        return getTablePivotKeys(req);
    }

    /**
     * Create new master object.
     * @see CreateObject
     */
    public CompletableFuture<GUID> createObject(CreateObject req) {
        return RpcUtil.apply(
                sendRequest(req, service.createObject()),
                response -> RpcUtil.fromProto(response.body().getObjectId()));
    }

    public CompletableFuture<GUID> createObject(ObjectType type, Map<String, YTreeNode> attributes) {
        return createObject(type, attributes, null);
    }

    public CompletableFuture<GUID> createObject(ObjectType type, Map<String, YTreeNode> attributes, @Nullable Duration requestTimeout) {
        CreateObject req = new CreateObject(type);
        req.setAttributes(attributes);
        if (requestTimeout != null) {
            req.setTimeout(requestTimeout);
        }
        return createObject(req);
    }

    /**
     * Create cypress node
     * @see CreateNode
     */
    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.createNode()),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }


    /**
     * Remove cypress node
     * @see RemoveNode
     */
    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.removeNode()),
                response -> null);
    }

    /**
     * Lock cypress node
     * @see LockNode
     */
    @Override
    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.lockNode()),
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
                sendRequest(req, service.copyNode()),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Move cypress node
     * @see MoveNode
     */
    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.moveNode()),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Link cypress node
     * @see LinkNode
     */
    @Override
    public CompletableFuture<GUID> linkNode(LinkNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.linkNode()),
                response -> RpcUtil.fromProto(response.body().getNodeId()));
    }

    /**
     * Concatenate nodes
     * @see ConcatenateNodes
     */
    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return RpcUtil.apply(
                sendRequest(req, service.concatenateNodes()),
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
    public <T> CompletableFuture<List<T>> lookupRows(AbstractLookupRowsRequest<?> request, YTreeObjectSerializer<T> serializer) {
        return lookupRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> lookupRows(AbstractLookupRowsRequest<?> request, YTreeObjectSerializer<T> serializer,
                                                  ConsumerSource<T> consumer) {
        return lookupRowsImpl(request, response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer);
            return null;
        });
    }

    private <T> CompletableFuture<T> lookupRowsImpl(
            AbstractLookupRowsRequest<?> request,
            Function<RpcClientResponse<TRspLookupRows>, T> responseReader)
    {
        return handleHeavyResponse(
                sendRequest(request.asLookupRowsWritable(), service.lookupRows()),
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

    public <T> CompletableFuture<Void> versionedLookupRows(LookupRowsRequest request,
                                                           YTreeObjectSerializer<T> serializer, ConsumerSource<T> consumer) {
        return versionedLookupRowsImpl(request, response -> {
            ApiServiceUtil.deserializeVersionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer);
            return null;
        });
    }

    private <T> CompletableFuture<T> versionedLookupRowsImpl(
            AbstractLookupRowsRequest<?> request,
            Function<RpcClientResponse<TRspVersionedLookupRows>, T> responseReader)
    {
        return handleHeavyResponse(
                sendRequest(request.asVersionedLookupRowsWritable(), service.versionedLookupRows()),
                response -> {
                    logger.trace("VersionedLookupRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
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
                sendRequest(request, service.selectRows()),
                response -> {
                    logger.trace("SelectRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
                    return responseReader.apply(response);
                });
    }

    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request) {
        return RpcUtil.apply(
                sendRequest(new ModifyRowsWrapper(transactionId, request), service.modifyRows()),
                response -> null);
    }

    // TODO: TReqBatchModifyRows

    public CompletableFuture<Long> buildSnapshot(BuildSnapshot req) {
        return RpcUtil.apply(
                sendRequest(req, service.buildSnapshot()),
                response -> response.body().getSnapshotId());
    }

    public CompletableFuture<Void> gcCollect(GcCollect req) {
        return RpcUtil.apply(
                sendRequest(req, service.gcCollect()),
                response -> null);
    }

    public CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    public CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp) {
        RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, RpcClientResponse<TRspGetInSyncReplicas>> builder =
                service.getInSyncReplicas();

        request.writeHeaderTo(builder.header());
        builder.body().setPath(request.getPath());
        builder.body().setTimestamp(timestamp.getValue());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));

        request.serializeRowsetTo(builder.attachments());

        return RpcUtil.apply(invoke(builder),
                response ->
                        response.body().getReplicaIdsList().stream().map(RpcUtil::fromProto)
                                .collect(Collectors.toList()));
    }

    @Deprecated
    public CompletableFuture<List<GUID>> getInSyncReplicas(
            String path,
            YtTimestamp timestamp,
            TableSchema schema,
            Iterable<? extends List<?>> keys)
    {
        return getInSyncReplicas(new GetInSyncReplicas(path, schema, keys), timestamp);
    }

    public CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices) {
        return getTabletInfos(path, tabletIndices, null);
    }

    public CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGetTabletInfos.Builder, RpcClientResponse<TRspGetTabletInfos>> builder =
                service.getTabletInfos();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }

        builder.body().setPath(path);
        builder.body().addAllTabletIndexes(tabletIndices);

        return RpcUtil.apply(invoke(builder),
                response ->
                        response.body()
                                .getTabletsList()
                                .stream()
                                .map(x -> new TabletInfo(x.getTotalRowCount(), x.getTrimmedRowCount()))
                                .collect(Collectors.toList()));
    }

    public CompletableFuture<YtTimestamp> generateTimestamps(int count) {
        return generateTimestamps(count, null);
    }

    public CompletableFuture<YtTimestamp> generateTimestamps(int count, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGenerateTimestamps.Builder, RpcClientResponse<TRspGenerateTimestamps>> builder =
                service.generateTimestamps();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setCount(count);
        return RpcUtil.apply(invoke(builder),
                response -> YtTimestamp.valueOf(response.body().getTimestamp()));
    }

    public CompletableFuture<YtTimestamp> generateTimestamps() {
        return generateTimestamps(null);
    }

    public CompletableFuture<YtTimestamp> generateTimestamps(@Nullable Duration requestTimeout) {
        return generateTimestamps(1, requestTimeout);
    }

    /* tables */
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

    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted) {
        return mountTable(path, cellId, freeze, waitMounted, null);
    }

    /**
     * @param requestTimeout applies only to request itself and does NOT apply to waiting for tablets to be mounted
     */
    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted, @Nullable Duration requestTimeout) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        RpcClientRequestBuilder<TReqMountTable.Builder, RpcClientResponse<TRspMountTable>> builder =
                service.mountTable();

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

    public CompletableFuture<Void> reshardTable(ReshardTable req) {
        RpcClientRequestBuilder<TReqReshardTable.Builder, RpcClientResponse<TRspReshardTable>> builder =
                service.reshardTable();

        if (req.getTimeout().isPresent()) {
            builder.setTimeout(req.getTimeout().get());
        }
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<List<GUID>> reshardTableAutomatic(ReshardTable req) {
        RpcClientRequestBuilder<TReqReshardTableAutomatic.Builder, RpcClientResponse<TRspReshardTableAutomatic>>
                builder = service.reshardTableAutomatic();

        if (req.getTimeout().isPresent()) {
            builder.setTimeout(req.getTimeout().get());
        }
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder), response ->
                response.body().getTabletActionsList().stream().map(RpcUtil::fromProto).collect(Collectors.toList())
        );
    }

    public CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        return trimTable(path, tableIndex, trimmedRowCount, null);
    }

    public CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqTrimTable.Builder, RpcClientResponse<TRspTrimTable>> builder =
                service.trimTable();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);
        builder.body().setTabletIndex(tableIndex);
        builder.body().setTrimmedRowCount(trimmedRowCount);
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> alterTable(AlterTable req) {
        RpcClientRequestBuilder<TReqAlterTable.Builder, RpcClientResponse<TRspAlterTable>> builder =
                service.alterTable();

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity) {
        return alterTableReplica(replicaId, enabled, mode, preserveTimestamp, atomicity, null);
    }

    public CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity,
            @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqAlterTableReplica.Builder, RpcClientResponse<TRspAlterTableReplica>>
                builder = service.alterTableReplica();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setReplicaId(RpcUtil.toProto(replicaId))
                .setEnabled(enabled)
                .setPreserveTimestamps(preserveTimestamp)
                .setAtomicity(atomicity)
                .setMode(mode);

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    /* */

    @Override
    public CompletableFuture<GUID> startOperation(StartOperation req) {
        RpcClientRequestBuilder<TReqStartOperation.Builder, RpcClientResponse<TRspStartOperation>>
                builder = service.startOperation();

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder), response -> RpcUtil.fromProto(response.body().getOperationId()));
    }

    /* Jobs */

    public CompletableFuture<Void> addMember(String group, String member, MutatingOptions mo) {
        return addMember(group, member, mo, null);
    }

    public CompletableFuture<Void> addMember(String group, String member, MutatingOptions mo, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqAddMember.Builder, RpcClientResponse<TRspAddMember>>
                builder = service.addMember();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setGroup(group)
                .setMember(member)
                .setMutatingOptions(mo.writeTo(TMutatingOptions.newBuilder()));

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> removeMember(String group, String member, MutatingOptions mo) {
        return removeMember(group, member, mo, null);
    }

    public CompletableFuture<Void> removeMember(String group, String member, MutatingOptions mo, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqRemoveMember.Builder, RpcClientResponse<TRspRemoveMember>>
                builder = service.removeMember();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setGroup(group)
                .setMember(member)
                .setMutatingOptions(mo.writeTo(TMutatingOptions.newBuilder()));

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        RpcClientRequestBuilder<TReqCheckPermission.Builder, RpcClientResponse<TRspCheckPermission>>
                builder = service.checkPermission();

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder), response -> response.body().getResult());
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
        RpcClientRequestBuilder<TReqReadTable.Builder, RpcClientResponse<TRspReadTable>>
                builder = service.readTable();

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
        RpcClientRequestBuilder<TReqWriteTable.Builder, RpcClientResponse<TRspWriteTable>>
                builder = service.writeTable();

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        TableWriterImpl<T> tableWriter = new TableWriterImpl<>(
                req.getWindowSize(),
                req.getPacketSize(),
                req.getSerializer());
        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
        CompletableFuture<TableWriter<T>> result = streamControlFuture.thenCompose(control -> tableWriter.startUpload());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public CompletableFuture<FileReader> readFile(ReadFile req) {
        RpcClientRequestBuilder<TReqReadFile.Builder, RpcClientResponse<TRspReadFile>>
                builder = service.readFile();

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
        RpcClientRequestBuilder<TReqWriteFile.Builder, RpcClientResponse<TRspWriteFile>>
                builder = service.writeFile();

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

    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<ResponseType> invoke(
            RpcClientRequestBuilder<RequestType, ResponseType> builder) {
        return builder.invoke(rpcClient);
    }

    protected <RequestType extends MessageLite.Builder, ResponseType> CompletableFuture<RpcClientStreamControl>
    startStream(RpcClientRequestBuilder<RequestType, ResponseType> builder, RpcStreamConsumer consumer)
    {
        return CompletableFuture.completedFuture(builder.startStream(rpcClient, consumer));
    }

    private <RequestMsgBuilder extends MessageLite.Builder, ResponseMsg,
            RequestType extends HighLevelRequest<RequestMsgBuilder>>
    CompletableFuture<ResponseMsg>
    sendRequest(RequestType req, RpcClientRequestBuilder<RequestMsgBuilder, ResponseMsg> builder)
    {
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

    static private YTreeNode parseByteString(ByteString byteString) {
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
