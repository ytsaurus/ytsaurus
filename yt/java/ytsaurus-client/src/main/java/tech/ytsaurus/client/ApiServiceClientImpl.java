package tech.ytsaurus.client;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.OperationImpl;
import tech.ytsaurus.client.operations.Spec;
import tech.ytsaurus.client.operations.SpecPreparationContext;
import tech.ytsaurus.client.request.AbortJob;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.AbortQuery;
import tech.ytsaurus.client.request.AbortTransaction;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.AdvanceConsumer;
import tech.ytsaurus.client.request.AlterQuery;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.AlterTableReplica;
import tech.ytsaurus.client.request.Atomicity;
import tech.ytsaurus.client.request.BaseOperation;
import tech.ytsaurus.client.request.BuildSnapshot;
import tech.ytsaurus.client.request.CheckClusterLiveness;
import tech.ytsaurus.client.request.CheckPermission;
import tech.ytsaurus.client.request.CommitTransaction;
import tech.ytsaurus.client.request.CompleteOperation;
import tech.ytsaurus.client.request.ConcatenateNodes;
import tech.ytsaurus.client.request.CopyNode;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.CreateObject;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.GcCollect;
import tech.ytsaurus.client.request.GenerateTimestamps;
import tech.ytsaurus.client.request.GetFileFromCache;
import tech.ytsaurus.client.request.GetFileFromCacheResult;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.GetJob;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetJobStderrResult;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.GetQuery;
import tech.ytsaurus.client.request.GetQueryResult;
import tech.ytsaurus.client.request.GetTablePivotKeys;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.HighLevelRequest;
import tech.ytsaurus.client.request.LinkNode;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.client.request.ListJobsResult;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.ListQueries;
import tech.ytsaurus.client.request.ListQueriesResult;
import tech.ytsaurus.client.request.LockNode;
import tech.ytsaurus.client.request.LockNodeResult;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.client.request.MapReduceOperation;
import tech.ytsaurus.client.request.MergeOperation;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.MoveNode;
import tech.ytsaurus.client.request.MultiTablePartition;
import tech.ytsaurus.client.request.MutateNode;
import tech.ytsaurus.client.request.MutatingOptions;
import tech.ytsaurus.client.request.PartitionTables;
import tech.ytsaurus.client.request.PingTransaction;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.request.PutFileToCache;
import tech.ytsaurus.client.request.PutFileToCacheResult;
import tech.ytsaurus.client.request.Query;
import tech.ytsaurus.client.request.QueryResult;
import tech.ytsaurus.client.request.ReadFile;
import tech.ytsaurus.client.request.ReadQueryResult;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.RegisterQueueConsumer;
import tech.ytsaurus.client.request.RemoteCopyOperation;
import tech.ytsaurus.client.request.RemountTable;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.request.RequestBase;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.ResumeOperation;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.request.SetNode;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.client.request.StartOperation;
import tech.ytsaurus.client.request.StartQuery;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.client.request.TableReplicaMode;
import tech.ytsaurus.client.request.TableReq;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TabletInfoReplica;
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.client.request.TrimTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.client.request.VanillaOperation;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.ConsumerSourceRet;
import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestsTestingController;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.cypress.RichYPathParser;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.EAtomicity;
import tech.ytsaurus.rpcproxy.EOperationType;
import tech.ytsaurus.rpcproxy.ETableReplicaMode;
import tech.ytsaurus.rpcproxy.TCheckPermissionResult;
import tech.ytsaurus.rpcproxy.TReqGetInSyncReplicas;
import tech.ytsaurus.rpcproxy.TReqModifyRows;
import tech.ytsaurus.rpcproxy.TReqReadFile;
import tech.ytsaurus.rpcproxy.TReqStartTransaction;
import tech.ytsaurus.rpcproxy.TReqWriteFile;
import tech.ytsaurus.rpcproxy.TRspLookupRows;
import tech.ytsaurus.rpcproxy.TRspReadFile;
import tech.ytsaurus.rpcproxy.TRspSelectRows;
import tech.ytsaurus.rpcproxy.TRspStartTransaction;
import tech.ytsaurus.rpcproxy.TRspVersionedLookupRows;
import tech.ytsaurus.rpcproxy.TRspWriteFile;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

/**
 * Implementation of ApiServiceClient
 *
 * @see ApiServiceClient
 */
@NonNullFields
public class ApiServiceClientImpl implements ApiServiceClient, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClientImpl.class);
    private static final List<String> JOB_TYPES = Arrays.asList("mapper", "reducer", "reduce_combiner");

    private final ScheduledExecutorService executorService;
    private final Executor heavyExecutor;
    private final ExecutorService prepareSpecExecutor = Executors.newSingleThreadExecutor();
    @Nullable
    private final RpcClient rpcClient;
    private final YTsaurusClientConfig config;
    protected final RpcOptions rpcOptions;
    protected final SerializationResolver serializationResolver;

    public ApiServiceClientImpl(
            @Nullable RpcClient client,
            @Nonnull YTsaurusClientConfig config,
            @Nonnull Executor heavyExecutor,
            @Nonnull ScheduledExecutorService executorService,
            SerializationResolver serializationResolver
    ) {
        OutageController outageController = config.getRpcOptions().getTestingOptions().getOutageController();
        if (client != null && outageController != null) {
            this.rpcClient = new OutageRpcClient(client, outageController);
        } else {
            this.rpcClient = client;
        }
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
        this.config = config;
        this.rpcOptions = config.getRpcOptions();
        this.executorService = executorService;
        this.serializationResolver = serializationResolver;
    }

    public ApiServiceClientImpl(
            @Nullable RpcClient client,
            @Nonnull RpcOptions options,
            @Nonnull Executor heavyExecutor,
            @Nonnull ScheduledExecutorService executorService,
            SerializationResolver serializationResolver
    ) {
        this(
                client,
                YTsaurusClientConfig.builder().withPorto().setRpcOptions(options).build(),
                heavyExecutor,
                executorService,
                serializationResolver
        );
    }

    @Override
    public void close() {
        prepareSpecExecutor.shutdown();
    }

    @Override
    public TransactionalClient getRootClient() {
        return this;
    }

    YTsaurusClientConfig getConfig() {
        return config;
    }

    ExecutorService getPrepareSpecExecutor() {
        return prepareSpecExecutor;
    }

    /**
     * Start new master or tablet transaction.
     *
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
                                Objects.requireNonNull(sender), config, heavyExecutor,
                                executorService, serializationResolver),
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
     *
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
     *
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
     *
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
     *
     * @see GetNode
     */
    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.parseByteString(response.body().getValue()));
    }

    /**
     * List cypress node
     *
     * @see ListNode
     */
    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LIST_NODE.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.parseByteString(response.body().getValue()));
    }

    /**
     * Set cypress node
     *
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
     *
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
     *
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
     *
     * @see CreateObject
     */
    public CompletableFuture<GUID> createObject(CreateObject req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CREATE_OBJECT.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getObjectId()));
    }

    /**
     * Check cluster's liveness
     *
     * @see CheckClusterLiveness
     */
    public CompletableFuture<Void> checkClusterLiveness(CheckClusterLiveness req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CHECK_CLUSTER_LIVENESS.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    /**
     * Create cypress node
     *
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
     *
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
     *
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
     *
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
     *
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
     *
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
     *
     * @see ConcatenateNodes
     */
    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.CONCATENATE_NODES.createRequestBuilder(rpcOptions)),
                response -> null);
    }

    @Override
    public CompletableFuture<List<MultiTablePartition>> partitionTables(PartitionTables req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.PARTITION_TABLES.createRequestBuilder(rpcOptions)),
                response -> response.body().getPartitionsList().stream()
                        .map(p -> {
                            List<YPath> tableRanges = p.getTableRangesList().stream()
                                    .map(RichYPathParser::parse)
                                    .collect(Collectors.toList());
                            var statistics = new MultiTablePartition.AggregateStatistics(
                                    p.getAggregateStatistics().getChunkCount(),
                                    p.getAggregateStatistics().getDataWeight(),
                                    p.getAggregateStatistics().getRowCount());
                            return new MultiTablePartition(tableRanges, statistics);
                        })
                        .collect(Collectors.toList()));
    }

    // TODO: TReqAttachTransaction

    /* */

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return lookupRowsImpl(request, response ->
                ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                        response.attachments()));
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return lookupRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result, serializationResolver);
            return result.get();
        });
    }

    @Override
    public <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return lookupRowsImpl(request, response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer, serializationResolver);
            return null;
        });
    }

    private <T> CompletableFuture<T> lookupRowsImpl(
            AbstractLookupRowsRequest<?, ?> request,
            Function<RpcClientResponse<TRspLookupRows>, T> responseReader
    ) {
        request.convertValues(serializationResolver);
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
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return versionedLookupRowsImpl(request, response -> ApiServiceUtil
                .deserializeVersionedRowset(response.body().getRowsetDescriptor(), response.attachments()));
    }

    private <T> CompletableFuture<T> versionedLookupRowsImpl(
            AbstractLookupRowsRequest<?, ?> request,
            Function<RpcClientResponse<TRspVersionedLookupRows>, T> responseReader
    ) {
        request.convertValues(serializationResolver);
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
                        response -> new SelectRowsResult(response, heavyExecutor, serializationResolver)
                );
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return selectRowsImpl(request, response ->
                ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                        response.attachments()));
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer) {
        return selectRowsImpl(request, response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result, serializationResolver);
            return result.get();
        });
    }

    @Override
    public <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer,
                                                  ConsumerSource<T> consumer) {
        return selectRowsImpl(request, response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer, serializationResolver);
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
    public CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> request) {
        request.convertValues(serializationResolver);
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
        request.convertValues(serializationResolver);
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
                                            .map(o -> new TabletInfoReplica(
                                                    RpcUtil.fromProto(o.getReplicaId()),
                                                    o.getLastReplicationTimestamp(),
                                                    ETableReplicaMode.forNumber(o.getMode()))
                                            )
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
        req.convertValues(serializationResolver);
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
                AlterTableReplica.builder()
                        .setReplicaId(replicaId)
                        .setEnabled(enabled)
                        .setMode(convertedMode)
                        .setPreserveTimestamps(preserveTimestamp)
                        .setAtomicity(convertedAtomicity)
                        .build()
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

    YTreeMapNode patchSpec(YTreeMapNode spec) {
        YTreeMapNode resultingSpec = spec;

        for (String op : JOB_TYPES) {
            if (resultingSpec.containsKey(op) && config.getJobSpecPatch().isPresent()) {
                YTreeNode patch = YTree.builder()
                        .beginMap()
                        .key(op).value(config.getJobSpecPatch().get())
                        .endMap()
                        .build();
                YTreeBuilder b = YTree.builder();
                YTreeNodeUtils.merge(resultingSpec, patch, b, true);
                resultingSpec = b.build().mapNode();
            }
        }

        if (resultingSpec.containsKey("tasks") && config.getJobSpecPatch().isPresent()) {
            for (Map.Entry<String, YTreeNode> entry
                    : resultingSpec.getOrThrow("tasks").asMap().entrySet()
            ) {
                YTreeNode patch = YTree.builder()
                        .beginMap()
                        .key("tasks")
                        .beginMap()
                        .key(entry.getKey()).value(config.getJobSpecPatch().get())
                        .endMap()
                        .endMap()
                        .build();

                YTreeBuilder b = YTree.builder();
                YTreeNodeUtils.merge(resultingSpec, patch, b, true);
                resultingSpec = b.build().mapNode();
            }
        }

        if (config.getSpecPatch().isPresent()) {
            YTreeBuilder b = YTree.builder();
            YTreeNodeUtils.merge(resultingSpec, config.getSpecPatch().get(), b, true);
            resultingSpec = b.build().mapNode();
        }
        return resultingSpec;
    }

    private CompletableFuture<YTreeNode> prepareSpec(Spec spec, @Nullable TransactionalOptions transactionalOptions) {
        return CompletableFuture.supplyAsync(
                () -> {
                    YTreeBuilder builder = YTree.builder();
                    spec.prepare(builder, this, new SpecPreparationContext(config, transactionalOptions));
                    return patchSpec(builder.build().mapNode());
                },
                prepareSpecExecutor);
    }

    <T extends Spec> CompletableFuture<Operation> startPreparedOperation(
            YTreeNode preparedSpec,
            BaseOperation<T> req,
            EOperationType type
    ) {
        return startOperation(
                StartOperation.builder()
                        .setType(type)
                        .setSpec(preparedSpec)
                        .setTransactionalOptions(req.getTransactionalOptions().orElse(null))
                        .setMutatingOptions(req.getMutatingOptions())
                        .build()
        ).thenApply(operationId ->
                new OperationImpl(operationId, this, executorService, config.getOperationPingPeriod()));
    }

    private <T extends Spec> CompletableFuture<Operation> startOperationImpl(
            BaseOperation<T> req,
            EOperationType type
    ) {
        return prepareSpec(req.getSpec(), req.getTransactionalOptions().orElse(null)).thenCompose(
                preparedSpec -> startPreparedOperation(preparedSpec, req, type)
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
                response -> RpcUtil.parseByteString(response.body().getMeta())
        );
    }

    @Override
    public Operation attachOperation(GUID operationId) {
        return new OperationImpl(operationId, this, executorService, config.getOperationPingPeriod());
    }

    @Override
    public CompletableFuture<Void> abortOperation(AbortOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_OPERATION.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> completeOperation(CompleteOperation req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.COMPLETE_OPERATION.createRequestBuilder(rpcOptions)),
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
                response -> RpcUtil.parseByteString(response.body().getInfo())
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
    public CompletableFuture<QueueRowset> pullConsumer(PullConsumer req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.PULL_CONSUMER.createRequestBuilder(rpcOptions)),
                response -> new QueueRowset(
                        ApiServiceUtil.deserializeUnversionedRowset(
                                response.body().getRowsetDescriptor(),
                                response.attachments()
                        ),
                        response.body().getStartOffset()
                )
        );
    }

    @Override
    public CompletableFuture<Void> advanceConsumer(AdvanceConsumer req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ADVANCE_CONSUMER.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<Void> registerQueueConsumer(RegisterQueueConsumer req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.REGISTER_QUEUE_CONSUMER.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<GUID> startQuery(StartQuery req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.START_QUERY.createRequestBuilder(rpcOptions)),
                response -> RpcUtil.fromProto(response.body().getQueryId())
        );
    }

    @Override
    public CompletableFuture<Void> abortQuery(AbortQuery req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ABORT_QUERY.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public CompletableFuture<QueryResult> getQueryResult(GetQueryResult req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_QUERY_RESULT.createRequestBuilder(rpcOptions)),
                response -> new QueryResult(response.body())
        );
    }

    @Override
    public CompletableFuture<UnversionedRowset> readQueryResult(ReadQueryResult req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.READ_QUERY_RESULT.createRequestBuilder(rpcOptions)),
                response -> ApiServiceUtil.deserializeUnversionedRowset(
                        response.body().getRowsetDescriptor(), response.attachments()
                )
        );
    }

    @Override
    public CompletableFuture<Query> getQuery(GetQuery req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.GET_QUERY.createRequestBuilder(rpcOptions)),
                response -> new Query(response.body().getQuery())
        );
    }

    @Override
    public CompletableFuture<ListQueriesResult> listQueries(ListQueries req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.LIST_QUERIES.createRequestBuilder(rpcOptions)),
                response -> new ListQueriesResult(response.body())
        );
    }

    @Override
    public CompletableFuture<Void> alterQuery(AlterQuery req) {
        return RpcUtil.apply(
                sendRequest(req, ApiServiceMethodTable.ALTER_QUERY.createRequestBuilder(rpcOptions)),
                response -> null
        );
    }

    @Override
    public <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        Optional<TableAttachmentReader<T>> attachmentReader = req.getSerializationContext().getAttachmentReader();
        if (attachmentReader.isEmpty()) {
            Optional<YTreeSerializer<T>> serializer = req.getSerializationContext().getYtreeSerializer();
            if (serializer.isPresent()) {
                attachmentReader = Optional.of(new TableAttachmentWireProtocolReader<>(
                        serializationResolver.createWireRowDeserializer(serializer.get())));
            }
        }

        TableReaderImpl<T> tableReader;
        if (attachmentReader.isPresent()) {
            tableReader = new TableReaderImpl<>(attachmentReader.get());
        } else {
            if (req.getSerializationContext().getObjectClass().isEmpty()) {
                throw new IllegalArgumentException("No object clazz");
            }
            tableReader = new TableReaderImpl<>(req, req.getSerializationContext().getObjectClass().get());
        }

        return setTableSchemaInSerializer(req)
                .thenCompose(transactionAndLockResult -> {
                    var readReq = getConfiguredReadReq(req, transactionAndLockResult);
                    if (transactionAndLockResult != null) {
                        tableReader.setTransaction(transactionAndLockResult.getKey());
                    }

                    var builder = ApiServiceMethodTable.READ_TABLE.createRequestBuilder(rpcOptions);
                    readReq.writeHeaderTo(builder.header());
                    readReq.writeTo(builder.body());
                    CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableReader);
                    CompletableFuture<TableReader<T>> result = streamControlFuture.thenCompose(
                            control -> tableReader.waitMetadata(serializationResolver));
                    RpcUtil.relayCancel(result, streamControlFuture);
                    return result;
                });
    }

    @Override
    public <T> CompletableFuture<AsyncReader<T>> readTableV2(ReadTable<T> req) {
        Optional<TableAttachmentReader<T>> attachmentReader = req.getSerializationContext().getAttachmentReader();
        if (attachmentReader.isEmpty()) {
            Optional<YTreeSerializer<T>> serializer = req.getSerializationContext().getYtreeSerializer();
            if (serializer.isPresent()) {
                attachmentReader = Optional.of(new TableAttachmentWireProtocolReader<>(
                        serializationResolver.createWireRowDeserializer(serializer.get())));
            }
        }

        AsyncTableReaderImpl<T> tableReader;
        if (attachmentReader.isPresent()) {
            tableReader = new AsyncTableReaderImpl<>(attachmentReader.get());
        } else {
            if (req.getSerializationContext().getObjectClass().isEmpty()) {
                throw new IllegalArgumentException("No object clazz");
            }
            tableReader = new AsyncTableReaderImpl<>(req,
                    req.getSerializationContext().getObjectClass().get());
        }

        return setTableSchemaInSerializer(req)
                .thenCompose(transactionAndLockResult -> {
                    var readReq = getConfiguredReadReq(req, transactionAndLockResult);
                    if (transactionAndLockResult != null) {
                        tableReader.setTransaction(transactionAndLockResult.getKey());
                    }

                    var builder = ApiServiceMethodTable.READ_TABLE.createRequestBuilder(rpcOptions);
                    readReq.writeHeaderTo(builder.header());
                    readReq.writeTo(builder.body());
                    CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableReader);
                    CompletableFuture<AsyncReader<T>> result = streamControlFuture.thenCompose(
                            control -> tableReader.waitMetadata(serializationResolver));
                    RpcUtil.relayCancel(result, streamControlFuture);
                    return result;
                });
    }

    @Override
    public <T> CompletableFuture<TableWriter<T>> writeTable(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            throw new IllegalStateException("Cannot write table with retries in ApiServiceClient");
        }

        return setTableSchemaInSerializer(req)
                .thenCompose(transactionAndLockResult -> {
                    var writeReq = getWriteReqByTransactionAndLockResult(req, transactionAndLockResult);
                    var tableWriter = new TableWriterImpl<>(writeReq, serializationResolver);
                    if (transactionAndLockResult != null) {
                        tableWriter.setTransaction(transactionAndLockResult.getKey());
                    }

                    var builder = ApiServiceMethodTable.WRITE_TABLE.createRequestBuilder(rpcOptions);
                    writeReq.writeHeaderTo(builder.header());
                    writeReq.writeTo(builder.body());
                    CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
                    CompletableFuture<TableWriter<T>> result = streamControlFuture
                            .thenCompose(control -> tableWriter.startUpload());
                    RpcUtil.relayCancel(result, streamControlFuture);
                    return result;
                });
    }

    @Override
    public <T> CompletableFuture<AsyncWriter<T>> writeTableV2(WriteTable<T> req) {
        if (req.getNeedRetries()) {
            throw new IllegalStateException("Cannot write table with retries in ApiServiceClient");
        }

        return setTableSchemaInSerializer(req)
                .thenCompose(transactionAndLockResult -> {
                    var writeReq = getWriteReqByTransactionAndLockResult(req, transactionAndLockResult);
                    var tableWriter = new AsyncTableWriterImpl<>(writeReq, serializationResolver);
                    if (transactionAndLockResult != null) {
                        tableWriter.setTransaction(transactionAndLockResult.getKey());
                    }

                    var builder = ApiServiceMethodTable.WRITE_TABLE.createRequestBuilder(rpcOptions);
                    writeReq.writeHeaderTo(builder.header());
                    writeReq.writeTo(builder.body());
                    CompletableFuture<RpcClientStreamControl> streamControlFuture = startStream(builder, tableWriter);
                    CompletableFuture<AsyncWriter<T>> result = streamControlFuture
                            .thenCompose(control -> tableWriter.startUpload());
                    RpcUtil.relayCancel(result, streamControlFuture);
                    return result;
                });
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

    private <T> CompletableFuture<Map.Entry<ApiServiceTransaction, LockNodeResult>> setTableSchemaInSerializer(
            WriteTable<T> req
    ) {
        var skiffSerializer = req.getSerializationContext().getSkiffSerializer();
        if (skiffSerializer.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (req.getTableSchema().isPresent()) {
            skiffSerializer.get()
                    .setTableSchema(req.getTableSchema().get());
            return CompletableFuture.completedFuture(null);
        }
        return startTransactionAndSetSchema(req.getTransactionId().orElse(null),
                req.getYPath(),
                skiffSerializer.get(),
                req.getYPath().getAppend().orElse(false) ? LockMode.Shared : LockMode.Exclusive);
    }

    private <T> CompletableFuture<Map.Entry<ApiServiceTransaction, LockNodeResult>> setTableSchemaInSerializer(
            ReadTable<T> req
    ) {
        var skiffSerializer = req.getSerializationContext().getSkiffSerializer();
        if (skiffSerializer.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (req.getTableSchema().isPresent()) {
            skiffSerializer.get()
                    .setTableSchema(req.getTableSchema().get());
            return CompletableFuture.completedFuture(null);
        }
        return startTransactionAndSetSchema(req.getTransactionId().orElse(null),
                req.getYPath(),
                skiffSerializer.get(),
                LockMode.Snapshot);
    }

    private <T> CompletableFuture<Map.Entry<ApiServiceTransaction, LockNodeResult>> startTransactionAndSetSchema(
            GUID transactionId,
            YPath path,
            EntitySkiffSerializer<T> serializer,
            LockMode lockMode) {
        return startTransaction(StartTransaction.master().toBuilder()
                .setParentId(transactionId)
                .build()
        )
                .thenCompose(transaction -> transaction.lockNode(new LockNode(path, lockMode))
                        .thenCompose(lockNodeResult -> transaction.getNode(GetNode.builder()
                                                .setPath(YPath.objectRoot(lockNodeResult.nodeId))
                                                .setAttributes(List.of("schema"))
                                                .build()
                                        )
                                        .thenApply(node ->
                                                TableSchema.fromYTree(node.getAttributeOrThrow("schema"))
                                        )
                                        .thenAccept(serializer::setTableSchema)
                                        .thenApply(unused -> lockNodeResult)
                        )
                        .handle((lockNodeResult, ex) -> {
                            if (ex == null) {
                                return CompletableFuture.completedFuture(lockNodeResult);
                            }
                            return transaction.abort()
                                    .thenAccept(voidRes -> {
                                        throw new RuntimeException(ex);
                                    })
                                    .thenApply(unused -> lockNodeResult);
                        })
                        .thenCompose(future -> future)
                        .thenApply(lockNodeResult -> new AbstractMap.SimpleEntry<>(transaction, lockNodeResult))
                );
    }

    private static <T> ReadTable<T> getConfiguredReadReq(
            ReadTable<T> req,
            Map.Entry<ApiServiceTransaction, LockNodeResult> transactionAndLockResult
    ) {
        var readReq = transactionAndLockResult != null ?
                req.toBuilder()
                        .setTransactionalOptions(transactionAndLockResult.getKey().getTransactionalOptions())
                        .setPath(req.getYPath().withObjectRoot(transactionAndLockResult.getValue().nodeId))
                        .build() :
                req;
        if (readReq.getSerializationContext().getSkiffSerializer().isEmpty()) {
            return readReq;
        }
        return readReq.toBuilder()
                .setPath(
                        readReq.getYPath().withColumns(
                                readReq.getSerializationContext().getSkiffSerializer().get()
                                        .getEntityTableSchema().orElseThrow(IllegalStateException::new)
                                        .getColumnNames()
                        )
                )
                .build();
    }

    private static <T> WriteTable<T> getWriteReqByTransactionAndLockResult(
            WriteTable<T> req,
            Map.Entry<ApiServiceTransaction, LockNodeResult> transactionAndLockResult
    ) {
        return transactionAndLockResult != null ?
                req.toBuilder()
                        .setTransactionalOptions(transactionAndLockResult.getKey().getTransactionalOptions())
                        .setPath(req.getYPath().withObjectRoot(transactionAndLockResult.getValue().nodeId))
                        .build() :
                req;
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
        /*
         * If several mutating requests was done with the same RequestType instance,
         * it must have different mutation ids.
         * So we reset mutationId for every request.
         */
        if (req instanceof TableReq) {
            req = (RequestType) ((TableReq<?, ?>) req)
                    .toBuilder()
                    .setMutatingOptions(new MutatingOptions().setMutationId(GUID.create()))
                    .build();
        } else if (req instanceof MutateNode) {
            req = (RequestType) ((MutateNode<?, ?>) req)
                    .toBuilder()
                    .setMutatingOptions(new MutatingOptions().setMutationId(GUID.create()))
                    .build();
        } else if (req instanceof MutateNode.Builder) {
            ((MutateNode.Builder<?, ?>) req).setMutatingOptions(new MutatingOptions().setMutationId(GUID.create()));
        }

        if (req instanceof RequestBase) {
            req = (RequestType) ((RequestBase<?, ?>) req)
                    .toBuilder()
                    .setUserAgent(config.getVersion())
                    .build();
        }

        logger.debug("Starting request {}; {}; User-Agent: {}",
                builder,
                req.getArgumentsLogString(),
                config.getVersion());
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
}

@NonNullApi
@NonNullFields
class ModifyRowsWrapper implements HighLevelRequest<TReqModifyRows.Builder> {
    private final GUID transactionId;
    private final AbstractModifyRowsRequest<?, ?> request;

    ModifyRowsWrapper(GUID transactionId, AbstractModifyRowsRequest<?, ?> request) {
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

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, ?> builder) {
        builder.body().setPath(request.getPath());
        builder.body().setTimestamp(timestamp.getValue());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));

        request.serializeRowsetTo(builder.attachments());
    }
}
