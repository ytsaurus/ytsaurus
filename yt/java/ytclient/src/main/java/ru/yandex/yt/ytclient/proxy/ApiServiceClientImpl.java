package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeNodeUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.EOperationType;
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
import ru.yandex.yt.ytclient.YtClientConfiguration;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.ConsumerSourceRet;
import ru.yandex.yt.ytclient.operations.Operation;
import ru.yandex.yt.ytclient.operations.OperationImpl;
import ru.yandex.yt.ytclient.operations.Spec;
import ru.yandex.yt.ytclient.operations.SpecPreparationContext;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.request.AbortJob;
import ru.yandex.yt.ytclient.proxy.request.AbortOperation;
import ru.yandex.yt.ytclient.proxy.request.AbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
import ru.yandex.yt.ytclient.proxy.request.AlterTableReplica;
import ru.yandex.yt.ytclient.proxy.request.Atomicity;
import ru.yandex.yt.ytclient.proxy.request.BaseOperation;
import ru.yandex.yt.ytclient.proxy.request.BuildSnapshot;
import ru.yandex.yt.ytclient.proxy.request.CheckClusterLiveness;
import ru.yandex.yt.ytclient.proxy.request.CheckPermission;
import ru.yandex.yt.ytclient.proxy.request.CommitTransaction;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.CreateObject;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.GcCollect;
import ru.yandex.yt.ytclient.proxy.request.GenerateTimestamps;
import ru.yandex.yt.ytclient.proxy.request.GetFileFromCache;
import ru.yandex.yt.ytclient.proxy.request.GetFileFromCacheResult;
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetJob;
import ru.yandex.yt.ytclient.proxy.request.GetJobStderr;
import ru.yandex.yt.ytclient.proxy.request.GetJobStderrResult;
import ru.yandex.yt.ytclient.proxy.request.GetOperation;
import ru.yandex.yt.ytclient.proxy.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.GetTabletInfos;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListJobs;
import ru.yandex.yt.ytclient.proxy.request.ListJobsResult;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MapOperation;
import ru.yandex.yt.ytclient.proxy.request.MapReduceOperation;
import ru.yandex.yt.ytclient.proxy.request.MergeOperation;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.MutateNode;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.PingTransaction;
import ru.yandex.yt.ytclient.proxy.request.PutFileToCache;
import ru.yandex.yt.ytclient.proxy.request.PutFileToCacheResult;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.ReduceOperation;
import ru.yandex.yt.ytclient.proxy.request.RemoteCopyOperation;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.proxy.request.ReshardTable;
import ru.yandex.yt.ytclient.proxy.request.ResumeOperation;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.SortOperation;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.SuspendOperation;
import ru.yandex.yt.ytclient.proxy.request.TableReplicaMode;
import ru.yandex.yt.ytclient.proxy.request.TableReq;
import ru.yandex.yt.ytclient.proxy.request.TabletInfo;
import ru.yandex.yt.ytclient.proxy.request.TabletInfoReplica;
import ru.yandex.yt.ytclient.proxy.request.TrimTable;
import ru.yandex.yt.ytclient.proxy.request.UnfreezeTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters;
import ru.yandex.yt.ytclient.proxy.request.VanillaOperation;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.request.ExistsNode;
import ru.yandex.yt.ytclient.request.GetNode;
import ru.yandex.yt.ytclient.request.ListNode;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequestsTestingController;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;


/**
 * Клиент для высокоуровневой работы с ApiService
 */
@NonNullFields
public class ApiServiceClientImpl implements ApiServiceClient, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClientImpl.class);
    private static final List<String> JOB_TYPES = Arrays.asList("mapper", "reducer", "reduce_combiner");

    private final ScheduledExecutorService executorService;
    private final Executor heavyExecutor;
    private final ExecutorService prepareSpecExecutor = Executors.newSingleThreadExecutor();
    @Nullable private final RpcClient rpcClient;
    final YtClientConfiguration configuration;
    final RpcOptions rpcOptions;

    public ApiServiceClientImpl(
            @Nullable RpcClient client,
            @Nonnull YtClientConfiguration configuration,
            @Nonnull Executor heavyExecutor,
            @Nonnull ScheduledExecutorService executorService
    ) {
        OutageController outageController = configuration.getRpcOptions().getTestingOptions().getOutageController();
        if (client != null && outageController != null) {
            this.rpcClient = new OutageRpcClient(client, outageController);
        } else {
            this.rpcClient = client;
        }
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
        this.configuration = configuration;
        this.rpcOptions = configuration.getRpcOptions();
        this.executorService = executorService;
    }

    public ApiServiceClientImpl(
            @Nullable RpcClient client,
            @Nonnull RpcOptions options,
            @Nonnull Executor heavyExecutor,
            @Nonnull ScheduledExecutorService executorService
    ) {
        this(client, YtClientConfiguration.builder().setRpcOptions(options).build(), heavyExecutor, executorService);
    }

    @Override
    public void close() {
        prepareSpecExecutor.shutdown();
    }

    @Override
    public TransactionalClient getRootClient() {
        return this;
    }

    /**
     * Start new master or tablet transaction.
     * @see StartTransaction
     */
    @Override
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        RpcClientRequestBuilder<TReqStartTransaction.Builder, TRspStartTransaction> builder =
                ApiServiceMethodTable.START_TRANSACTION.createRequestBuilder(rpcOptions);
        return RpcUtil.apply(sendRequest(startTransaction, builder), response -> {
            GUID id = RpcUtil.fromProto(response.body().getId());
            YtTimestamp startTimestamp = YtTimestamp.valueOf(response.body().getStartTimestamp());
            RpcClient sender = response.sender();
            ApiServiceTransaction result;

            if (startTransaction.getSticky() && (rpcClient == null || !rpcClient.equals(sender))) {
                logger.trace("Create sticky transaction with new client to proxy {}", sender.getAddressString());
                result = new ApiServiceTransaction(
                        new ApiServiceClientImpl(
                                Objects.requireNonNull(sender), configuration, heavyExecutor, executorService),
                        id,
                        startTimestamp,
                        startTransaction.getPing(),
                        startTransaction.getPingAncestors(),
                        startTransaction.getSticky(),
                        startTransaction.getPingPeriod().orElse(null),
                        startTransaction.getFailedPingRetryPeriod().orElse(null),
                        sender.executor(),
                        startTransaction.getOnPingFailed().orElse(null));
            } else {
                if (startTransaction.getSticky()) {
                    logger.trace("Create sticky transaction with client {} to proxy {}", this,
                            sender.getAddressString());
                } else {
                    logger.trace("Create non-sticky transaction with client {}", this);
                }

                result = new ApiServiceTransaction(
                        this,
                        id,
                        startTimestamp,
                        startTransaction.getPing(),
                        startTransaction.getPingAncestors(),
                        startTransaction.getSticky(),
                        startTransaction.getPingPeriod().orElse(null),
                        startTransaction.getFailedPingRetryPeriod().orElse(null),
                        sender.executor(),
                        startTransaction.getOnPingFailed().orElse(null));
            }

            sender.ref();
            result.getTransactionCompleteFuture().whenComplete((ignored, ex) -> sender.unref());

            RpcRequestsTestingController rpcRequestsTestingController =
                    rpcOptions.getTestingOptions().getRpcRequestsTestingController();
            if (rpcRequestsTestingController != null) {
                rpcRequestsTestingController.addStartedTransaction(id);
            }

            logger.debug("New transaction {} has started by {}", id, builder);
            return result;
        });
    }

    /**
     * Ping existing transaction
     * @see PingTransaction
     */
    @Override
    public CompletableFuture<Void> pingTransaction(PingTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.PING_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Commit existing transaction
     * @see CommitTransaction
     */
    @Override
    public CompletableFuture<Void> commitTransaction(CommitTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.COMMIT_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Abort existing transaction
     * @see AbortTransaction
     */
    @Override
    public CompletableFuture<Void> abortTransaction(AbortTransaction req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_TRANSACTION.createRequestBuilder(rpcOptions)),
                response -> null);
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
    @Override
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
     * Check cluster's liveness
     * @see CheckClusterLiveness
     */
    public CompletableFuture<Void> checkClusterLiveness(CheckClusterLiveness req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CHECK_CLUSTER_LIVENESS.createRequestBuilder(rpcOptions)),
                response -> null);
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

    @Override
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

    @Override
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

    @Override
    public CompletableFuture<SelectRowsResult> selectRowsV2(SelectRowsRequest request) {
        return sendRequest(request, ApiServiceMethodTable.SELECT_ROWS.createRequestBuilder(rpcOptions))
                .thenApply(
                        response -> new SelectRowsResult(response, heavyExecutor)
                );
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

    @Override
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


    @Override
    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request) {
        return RpcUtil.apply(
                sendRequest(
                        new ModifyRowsWrapper(transactionId, request),
                        ApiServiceMethodTable.MODIFY_ROWS.createRequestBuilder(rpcOptions)
                ),
                response -> null);
    }

    // TODO: TReqBatchModifyRows

    @Override
    public CompletableFuture<Long> buildSnapshot(BuildSnapshot req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.BUILD_SNAPSHOT.createRequestBuilder(rpcOptions)),
                response -> response.body().getSnapshotId());
    }

    @Override
    public CompletableFuture<Void> gcCollect(GcCollect req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GC_COLLECT.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> mountTable(MountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.MOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> unmountTable(UnmountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.UNMOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> remountTable(RemountTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.REMOUNT_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> freezeTable(FreezeTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.FREEZE_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> unfreezeTable(UnfreezeTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.UNFREEZE_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp) {
        return RpcUtil.apply(
                sendRequest(
                        new GetInSyncReplicasWrapper(timestamp, request),
                        ApiServiceMethodTable.GET_IN_SYNC_REPLICAS.createRequestBuilder(rpcOptions)
                ),
                response -> response.body().getReplicaIdsList()
                        .stream().map(RpcUtil::fromProto).collect(Collectors.toList()));
    }

    @Override
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

    @Override
    public CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GENERATE_TIMESTAMPS.createRequestBuilder(rpcOptions)),
                response -> YtTimestamp.valueOf(response.body().getTimestamp()));
    }

    /* tables */
    @Override
    public CompletableFuture<Void> reshardTable(ReshardTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.RESHARD_TABLE.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> trimTable(TrimTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.TRIM_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<Void> alterTable(AlterTable req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ALTER_TABLE.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
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

    @Override
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

    private YTreeMapNode patchSpec(YTreeMapNode spec) {
        YTreeMapNode resultingSpec = spec;

        for (String op : JOB_TYPES) {
            if (resultingSpec.containsKey(op) && configuration.getJobSpecPatch().isPresent()) {
                YTreeNode patch = YTree.builder()
                        .beginMap()
                        .key(op).value(configuration.getJobSpecPatch().get())
                        .endMap()
                        .build();
                YTreeBuilder b = YTree.builder();
                YTreeNodeUtils.merge(resultingSpec, patch, b, true);
                resultingSpec = b.build().mapNode();
            }
        }

        if (resultingSpec.containsKey("tasks") && configuration.getJobSpecPatch().isPresent()) {
            for (Map.Entry<String, YTreeNode> entry
                    : resultingSpec.getOrThrow("tasks").asMap().entrySet()
            ) {
                YTreeNode patch = YTree.builder()
                        .beginMap()
                        .key("tasks")
                        .beginMap()
                        .key(entry.getKey()).value(configuration.getJobSpecPatch().get())
                        .endMap()
                        .endMap()
                        .build();

                YTreeBuilder b = YTree.builder();
                YTreeNodeUtils.merge(resultingSpec, patch, b, true);
                resultingSpec = b.build().mapNode();
            }
        }

        if (configuration.getSpecPatch().isPresent()) {
            YTreeBuilder b = YTree.builder();
            YTreeNodeUtils.merge(resultingSpec, configuration.getSpecPatch().get(), b, true);
            resultingSpec = b.build().mapNode();
        }
        return resultingSpec;
    }

    private CompletableFuture<YTreeNode> prepareSpec(Spec spec) {
        return CompletableFuture.supplyAsync(
                () -> {
                    YTreeBuilder builder = YTree.builder();
                    spec.prepare(builder, this, new SpecPreparationContext(configuration));
                    return patchSpec(builder.build().mapNode());
                },
                prepareSpecExecutor);
    }

    private <T extends Spec> CompletableFuture<Operation> startOperationImpl(
            BaseOperation<T> req,
            EOperationType type
    ) {
        return prepareSpec(req.getSpec()).thenCompose(
                preparedSpec -> startOperation(
                        new StartOperation(type, preparedSpec)
                                .setTransactionOptions(req.getTransactionalOptions().orElse(null))
                                .setMutatingOptions(req.getMutatingOptions())
                ).thenApply(operationId ->
                        new OperationImpl(operationId, this, executorService, configuration.getOperationPingPeriod()))
        );
    }

    @Override
    public CompletableFuture<Operation> startMap(MapOperation req) {
        return startOperationImpl(req, EOperationType.OT_MAP);
    }

    @Override
    public CompletableFuture<Operation> startReduce(ReduceOperation req) {
        return startOperationImpl(req, EOperationType.OT_REDUCE);
    }

    @Override
    public CompletableFuture<Operation> startSort(SortOperation req) {
        return startOperationImpl(req, EOperationType.OT_SORT);
    }

    @Override
    public CompletableFuture<Operation> startMapReduce(MapReduceOperation req) {
        return startOperationImpl(req, EOperationType.OT_MAP_REDUCE);
    }

    @Override
    public CompletableFuture<Operation> startMerge(MergeOperation req) {
        return startOperationImpl(req, EOperationType.OT_MERGE);
    }

    @Override
    public CompletableFuture<Operation> startRemoteCopy(RemoteCopyOperation req) {
        return startOperationImpl(req, EOperationType.OT_REMOTE_COPY);
    }

    @Override
    public CompletableFuture<Operation> startVanilla(VanillaOperation req) {
        return startOperationImpl(req, EOperationType.OT_VANILLA);
    }

    @Override
    public CompletableFuture<YTreeNode> getOperation(GetOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_OPERATION.createRequestBuilder(rpcOptions)),
                response -> parseByteString(response.body().getMeta())
        );
    }

    @Override
    public Operation attachOperation(GUID operationId) {
        return new OperationImpl(operationId, this, executorService, configuration.getOperationPingPeriod());
    }

    @Override
    public CompletableFuture<Void> abortOperation(AbortOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_OPERATION.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> suspendOperation(SuspendOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.SUSPEND_OPERATION.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> resumeOperation(ResumeOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.RESUME_OPERATION.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<YTreeNode> getJob(GetJob req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_JOB.createRequestBuilder(rpcOptions)),
                response -> parseByteString(response.body().getInfo())
        );
    }

    @Override
    public CompletableFuture<ListJobsResult> listJobs(ListJobs req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LIST_JOBS.createRequestBuilder(rpcOptions)),
                response -> new ListJobsResult(response.body())
        );
    }

    @Override
    public CompletableFuture<GetJobStderrResult> getJobStderr(GetJobStderr req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_JOB_STDERR.createRequestBuilder(rpcOptions)),
                response -> new GetJobStderrResult(response.attachments()));
    }

    @Override
    public CompletableFuture<Void> abortJob(AbortJob req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_JOB.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> updateOperationParameters(UpdateOperationParameters req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.UPDATE_OPERATION_PARAMETERS.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<TCheckPermissionResult> checkPermission(CheckPermission req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CHECK_PERMISSION.createRequestBuilder(rpcOptions)),
                response -> response.body().getResult());
    }

    @Override
    public CompletableFuture<GetFileFromCacheResult> getFileFromCache(GetFileFromCache req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_FILE_FROM_CACHE.createRequestBuilder(rpcOptions)),
                response -> {
                    if (!response.body().getResult().getPath().isEmpty()) {
                        return new GetFileFromCacheResult(YPath.simple(response.body().getResult().getPath()));
                    }
                    return new GetFileFromCacheResult(null);
                });
    }

    @Override
    public CompletableFuture<PutFileToCacheResult> putFileToCache(PutFileToCache req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.PUT_FILE_TO_CACHE.createRequestBuilder(rpcOptions)),
                response -> new PutFileToCacheResult(YPath.simple(response.body().getResult().getPath())));
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req,
                                                           @Nullable TableAttachmentReader<T> reader) {
        RpcClientRequestBuilder<TReqReadTable.Builder, TRspReadTable>
                builder = ApiServiceMethodTable.READ_TABLE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        TableReaderImpl<T> tableReader;
        if (reader != null) {
            tableReader = new TableReaderImpl<>(reader);
        } else {
            Objects.requireNonNull(req.getObjectClazz());
            tableReader = new TableReaderImpl<>(req.getObjectClazz());
        }
        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableReader);
        CompletableFuture<TableReader<T>> result = streamControlFuture.thenCompose(
                control -> tableReader.waitMetadata());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            throw new IllegalStateException("Cannot write table with retries in ApiServiceClient");
        }

        RpcClientRequestBuilder<TReqWriteTable.Builder, TRspWriteTable>
                builder = ApiServiceMethodTable.WRITE_TABLE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        TableWriterImpl<T> tableWriter = new TableWriterImpl<>(req);

        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
        CompletableFuture<TableWriter<T>> result = streamControlFuture
                .thenCompose(control -> tableWriter.startUpload());
        RpcUtil.relayCancel(result, streamControlFuture);
        return result;
    }

    @Override
    public <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            throw new IllegalStateException("Cannot write table with retries in ApiServiceClient");
        }

        RpcClientRequestBuilder<TReqWriteTable.Builder, TRspWriteTable>
                builder = ApiServiceMethodTable.WRITE_TABLE.createRequestBuilder(rpcOptions);

        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        AsyncTableWriterImpl<T> tableWriter = new AsyncTableWriterImpl<>(req);

        CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
        CompletableFuture<AsyncWriter<T>> result = streamControlFuture
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
        /**
         * If several mutating requests was done with the same RequestType instance,
         * it must have different mutation ids.
         * So we reset mutationId for every request.
         */
        if (req instanceof MutateNode) {
            ((MutateNode<?>) req).setMutatingOptions(new MutatingOptions().setMutationId(GUID.create()));
        } else if (req instanceof TableReq) {
            ((TableReq<?>) req).setMutatingOptions(new MutatingOptions().setMutationId(GUID.create()));
        }

        if (req instanceof RequestBase) {
            ((RequestBase<?>) req).setUserAgent(configuration.getVersion());
        }

        logger.debug("Starting request {}; {}; User-Agent: {}",
                builder,
                req.getArgumentsLogString(),
                configuration.getVersion());
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
        request.serializeRowsetTo(builder);
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
