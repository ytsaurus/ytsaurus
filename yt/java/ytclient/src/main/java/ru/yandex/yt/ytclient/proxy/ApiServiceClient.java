package ru.yandex.yt.ytclient.proxy;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.time.Instant;
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
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.rpcproxy.TCheckPermissionResult;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqAbandonJob;
import ru.yandex.yt.rpcproxy.TReqAbortJob;
import ru.yandex.yt.rpcproxy.TReqAbortOperation;
import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
import ru.yandex.yt.rpcproxy.TReqAddMember;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.rpcproxy.TReqAlterTableReplica;
import ru.yandex.yt.rpcproxy.TReqBuildSnapshot;
import ru.yandex.yt.rpcproxy.TReqCheckPermission;
import ru.yandex.yt.rpcproxy.TReqCommitTransaction;
import ru.yandex.yt.rpcproxy.TReqCompleteOperation;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TReqCopyNode;
import ru.yandex.yt.rpcproxy.TReqCreateNode;
import ru.yandex.yt.rpcproxy.TReqCreateObject;
import ru.yandex.yt.rpcproxy.TReqDumpJobContext;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqGCCollect;
import ru.yandex.yt.rpcproxy.TReqGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TReqGetFileFromCache;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqGetJob;
import ru.yandex.yt.rpcproxy.TReqGetOperation;
import ru.yandex.yt.rpcproxy.TReqGetTableMountInfo;
import ru.yandex.yt.rpcproxy.TReqGetTablePivotKeys;
import ru.yandex.yt.rpcproxy.TReqGetTabletInfos;
import ru.yandex.yt.rpcproxy.TReqLinkNode;
import ru.yandex.yt.rpcproxy.TReqLockNode;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.rpcproxy.TReqMoveNode;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqPollJobShell;
import ru.yandex.yt.rpcproxy.TReqPutFileToCache;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqRemoveMember;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.rpcproxy.TReqReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TReqResumeOperation;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqSuspendOperation;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TReqUpdateOperationParameters;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TRspAbandonJob;
import ru.yandex.yt.rpcproxy.TRspAbortJob;
import ru.yandex.yt.rpcproxy.TRspAbortOperation;
import ru.yandex.yt.rpcproxy.TRspAbortTransaction;
import ru.yandex.yt.rpcproxy.TRspAddMember;
import ru.yandex.yt.rpcproxy.TRspAlterTable;
import ru.yandex.yt.rpcproxy.TRspAlterTableReplica;
import ru.yandex.yt.rpcproxy.TRspBuildSnapshot;
import ru.yandex.yt.rpcproxy.TRspCheckPermission;
import ru.yandex.yt.rpcproxy.TRspCommitTransaction;
import ru.yandex.yt.rpcproxy.TRspCompleteOperation;
import ru.yandex.yt.rpcproxy.TRspConcatenateNodes;
import ru.yandex.yt.rpcproxy.TRspCopyNode;
import ru.yandex.yt.rpcproxy.TRspCreateNode;
import ru.yandex.yt.rpcproxy.TRspCreateObject;
import ru.yandex.yt.rpcproxy.TRspDumpJobContext;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspGCCollect;
import ru.yandex.yt.rpcproxy.TRspGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TRspGetFileFromCache;
import ru.yandex.yt.rpcproxy.TRspGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TRspGetJob;
import ru.yandex.yt.rpcproxy.TRspGetOperation;
import ru.yandex.yt.rpcproxy.TRspGetTableMountInfo;
import ru.yandex.yt.rpcproxy.TRspGetTablePivotKeys;
import ru.yandex.yt.rpcproxy.TRspGetTabletInfos;
import ru.yandex.yt.rpcproxy.TRspLinkNode;
import ru.yandex.yt.rpcproxy.TRspLockNode;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspModifyRows;
import ru.yandex.yt.rpcproxy.TRspMountTable;
import ru.yandex.yt.rpcproxy.TRspMoveNode;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspPollJobShell;
import ru.yandex.yt.rpcproxy.TRspPutFileToCache;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspRemoveMember;
import ru.yandex.yt.rpcproxy.TRspRemoveNode;
import ru.yandex.yt.rpcproxy.TRspReshardTable;
import ru.yandex.yt.rpcproxy.TRspReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TRspResumeOperation;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspStartOperation;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspSuspendOperation;
import ru.yandex.yt.rpcproxy.TRspTrimTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.rpcproxy.TRspUpdateOperationParameters;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.object.ConsumerSourceRet;
import ru.yandex.yt.ytclient.proxy.internal.FileReaderImpl;
import ru.yandex.yt.ytclient.proxy.internal.FileWriterImpl;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.proxy.internal.TableReaderImpl;
import ru.yandex.yt.ytclient.proxy.internal.TableWriterImpl;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
import ru.yandex.yt.ytclient.proxy.request.CheckPermission;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.LockNodeResult;
import ru.yandex.yt.ytclient.proxy.request.MasterReadOptions;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.ReadTableDirect;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.ReshardTable;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartOperation;
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
import ru.yandex.yt.ytree.TAttributeDictionary;

/**
 * Клиент для высокоуровневой работы с ApiService
 */
public class ApiServiceClient implements TransactionalClient {
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

    private YTreeNode parseByteString(ByteString byteString) {
        return YTreeBinarySerializer.deserialize(byteString.newInput());
    }

    public CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> builder =
                service.startTransaction();
        builder.body().setType(options.getType());
        Duration timeout = options.getTimeout();
        if (timeout != null) {
            builder.body().setTimeout(ApiServiceUtil.durationToYtMicros(timeout));
        }
        Instant deadline = options.getDeadline();
        if (deadline != null) {
            builder.body().setDeadline(deadline.toEpochMilli());
        }
        if (options.getId() != null && !options.getId().isEmpty()) {
            builder.body().setId(RpcUtil.toProto(options.getId()));
        }
        if (options.getParentId() != null && !options.getParentId().isEmpty()) {
            builder.body().setParentId(RpcUtil.toProto(options.getParentId()));
        }
        if (options.getAutoAbort() != null) {
            builder.body().setAutoAbort(options.getAutoAbort());
        }
        if (options.getPing() != null) {
            builder.body().setPing(options.getPing());
        }
        if (options.getPingAncestors() != null) {
            builder.body().setPingAncestors(options.getPingAncestors());
        }
        if (options.getSticky() != null) {
            builder.body().setSticky(options.getSticky());
        }
        if (options.getAtomicity() != null) {
            builder.body().setAtomicity(options.getAtomicity());
        }
        if (options.getDurability() != null) {
            builder.body().setDurability(options.getDurability());
        }
        if (options.getPrerequisiteTransactionIds() != null) {
            // builder.body().setPrerequisiteTransactionIds(options.getPrerequisiteTransactionIds());
            throw new RuntimeException("prerequisite_transaction_ids is not supported in RPC proxy API yet");
        }
        final boolean ping = builder.body().getPing();
        final boolean pingAncestors = builder.body().getPingAncestors();
        final boolean sticky = builder.body().getSticky();
        final Duration pingPeriod = options.getPingPeriod();

        return RpcUtil.apply(invoke(builder), response -> {
            GUID id = RpcUtil.fromProto(response.body().getId());
            YtTimestamp startTimestamp = YtTimestamp.valueOf(response.body().getStartTimestamp());
            RpcClient sender = response.sender();
            if (rpcClient != null && rpcClient.equals(sender)) {
                return new ApiServiceTransaction(this, id, startTimestamp, ping, pingAncestors, sticky, pingPeriod,
                        sender.executor());
            } else {
                return new ApiServiceTransaction(sender, rpcOptions, id, startTimestamp, ping, pingAncestors, sticky,
                        pingPeriod, sender.executor());
            }
        });
    }

    public CompletableFuture<Void> pingTransaction(GUID id, boolean sticky) {
        return pingTransaction(id, sticky, null);
    }

    public CompletableFuture<Void> pingTransaction(GUID id, boolean sticky, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqPingTransaction.Builder, RpcClientResponse<TRspPingTransaction>> builder =
                service.pingTransaction();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setTransactionId(RpcUtil.toProto(id));
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> commitTransaction(GUID id, boolean sticky) {
        return commitTransaction(id, sticky, null);
    }

    public CompletableFuture<Void> commitTransaction(GUID id, boolean sticky, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqCommitTransaction.Builder, RpcClientResponse<TRspCommitTransaction>> builder =
                service.commitTransaction();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setTransactionId(RpcUtil.toProto(id));
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> abortTransaction(GUID id, boolean sticky) {
        return abortTransaction(id, sticky, null);
    }

    public CompletableFuture<Void> abortTransaction(GUID id, boolean sticky, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqAbortTransaction.Builder, RpcClientResponse<TRspAbortTransaction>> builder =
                service.abortTransaction();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setTransactionId(RpcUtil.toProto(id));
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    /* nodes */
    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.getNode()),
                response -> parseByteString(response.body().getValue()));
    }

    public CompletableFuture<YTreeNode> getNode(String path) {
        return getNode(path, null);
    }

    public CompletableFuture<YTreeNode> getNode(String path, @Nullable Duration requestTimeout) {
        return getNode(new GetNode(path).setTimeout(requestTimeout));
    }

    @Override
    public CompletableFuture<YTreeNode> listNode(ListNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.listNode()),
                response -> parseByteString(response.body().getValue()));
    }

    public CompletableFuture<YTreeNode> listNode(String path) {
        return listNode(path, null);
    }

    public CompletableFuture<YTreeNode> listNode(String path, @Nullable Duration requestTimeout) {
        return listNode(new ListNode(path).setTimeout(requestTimeout));
    }

    @Override
    public CompletableFuture<Void> setNode(SetNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.setNode()),
                response -> null);
    }

    public CompletableFuture<Void> setNode(String path, byte[] data) {
        return setNode(path, data, null);
    }

    public CompletableFuture<Void> setNode(String path, byte[] data, @Nullable Duration requestTimeout) {
        return setNode(new SetNode(path, data).setTimeout(requestTimeout));
    }

    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return setNode(path, data.toBinary());
    }

    public CompletableFuture<Void> setNode(String path, YTreeNode data, @Nullable Duration requestTimeout) {
        return setNode(path, data.toBinary(), requestTimeout);
    }

    @Override
    public CompletableFuture<Boolean> existsNode(ExistsNode req) {
        return RpcUtil.apply(
                sendRequest(req, service.existsNode()),
                response -> response.body().getExists());
    }

    public CompletableFuture<Boolean> existsNode(String path) {
        return existsNode(path, null);
    }

    public CompletableFuture<Boolean> existsNode(String path, @Nullable Duration requestTimeout) {
        return existsNode(new ExistsNode(path).setTimeout(requestTimeout));
    }

    public CompletableFuture<TRspGetTableMountInfo> getMountInfo(String path) {
        return getMountInfo(path, null);
    }

    public CompletableFuture<TRspGetTableMountInfo> getMountInfo(String path, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGetTableMountInfo.Builder, RpcClientResponse<TRspGetTableMountInfo>> builder =
                service.getTableMountInfo();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);

        return RpcUtil.apply(invoke(builder), RpcClientResponse::body);
    }

    public CompletableFuture<List<YTreeNode>> getTablePivotKeys(String path, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGetTablePivotKeys.Builder, RpcClientResponse<TRspGetTablePivotKeys>> builder =
                service.getTablePivotKeys();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);

        return RpcUtil.apply(invoke(builder),
                response -> YTreeBinarySerializer.deserialize(
                        new ByteArrayInputStream(response.body().getValue().toByteArray())
                ).asList());
    }


    public CompletableFuture<GUID> createObject(ObjectType type, Map<String, YTreeNode> attributes) {
        return createObject(type, attributes, null);
    }

    public CompletableFuture<GUID> createObject(ObjectType type, Map<String, YTreeNode> attributes, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqCreateObject.Builder, RpcClientResponse<TRspCreateObject>> builder =
                service.createObject();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setType(type.value());

        if (!attributes.isEmpty()) {
            final TAttributeDictionary.Builder aBuilder = builder.body().getAttributesBuilder();
            for (Map.Entry<String, YTreeNode> me : attributes.entrySet()) {
                aBuilder.addAttributesBuilder()
                        .setKey(me.getKey())
                        .setValue(ByteString.copyFrom(me.getValue().toBinary()));
            }
        }

        return RpcUtil.apply(invoke(builder),
                response ->
                        RpcUtil.fromProto(response.body().getObjectId()));
    }

    public CompletableFuture<GUID> createNode(CreateNode req) {
        RpcClientRequestBuilder<TReqCreateNode.Builder, RpcClientResponse<TRspCreateNode>> builder =
                service.createNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder),
                response ->
                        RpcUtil.fromProto(response.body().getNodeId()));
    }

    public CompletableFuture<GUID> createNode(String path, ObjectType type) {
        return createNode(new CreateNode(path, type));
    }

    public CompletableFuture<GUID> createNode(String path, ObjectType type, @Nullable Duration requestTimeout) {
        return createNode(new CreateNode(path, type).setTimeout(requestTimeout));
    }

    public CompletableFuture<GUID> createNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        return createNode(path, type, attributes, null);
    }

    public CompletableFuture<GUID> createNode(String path,
                                              ObjectType type,
                                              Map<String, YTreeNode> attributes,
                                              @Nullable Duration requestTimeout) {
        return createNode(new CreateNode(path, type, attributes).setTimeout(requestTimeout));
    }

    @Override
    public CompletableFuture<Void> removeNode(RemoveNode req) {
        RpcClientRequestBuilder<TReqRemoveNode.Builder, RpcClientResponse<TRspRemoveNode>> builder =
                service.removeNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> removeNode(String path) {
        return removeNode(new RemoveNode(path));
    }

    public CompletableFuture<LockNodeResult> lockNode(LockNode req) {
        RpcClientRequestBuilder<TReqLockNode.Builder, RpcClientResponse<TRspLockNode>> builder = service.lockNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> new LockNodeResult(
                RpcUtil.fromProto(response.body().getNodeId()),
                RpcUtil.fromProto(response.body().getLockId())));
    }

    public CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode) {
        return lockNode(path, mode, null);
    }

    public CompletableFuture<LockNodeResult> lockNode(String path, LockMode mode, @Nullable Duration requestTimeout) {
        return lockNode(new LockNode(path, mode).setTimeout(requestTimeout));
    }

    public CompletableFuture<GUID> copyNode(CopyNode req) {
        RpcClientRequestBuilder<TReqCopyNode.Builder, RpcClientResponse<TRspCopyNode>> builder = service.copyNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder),
                response ->
                        RpcUtil.fromProto(response.body().getNodeId()));
    }

    public CompletableFuture<GUID> copyNode(String src, String dst) {
        return copyNode(src, dst, null);
    }

    public CompletableFuture<GUID> copyNode(String src, String dst, @Nullable Duration requestTimeout) {
        return copyNode(new CopyNode(src, dst).setTimeout(requestTimeout));
    }

    @Override
    public CompletableFuture<GUID> moveNode(MoveNode req) {
        RpcClientRequestBuilder<TReqMoveNode.Builder, RpcClientResponse<TRspMoveNode>> builder = service.moveNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder),
                response ->
                        RpcUtil.fromProto(response.body().getNodeId()));
    }

    public CompletableFuture<GUID> moveNode(String from, String to) {
        return moveNode(from, to, null);
    }

    public CompletableFuture<GUID> moveNode(String from, String to, @Nullable Duration requestTimeout) {
        return moveNode(new MoveNode(from, to).setTimeout(requestTimeout));
    }

    public CompletableFuture<GUID> linkNode(LinkNode req) {
        RpcClientRequestBuilder<TReqLinkNode.Builder, RpcClientResponse<TRspLinkNode>> builder = service.linkNode();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());

        return RpcUtil.apply(invoke(builder),
                response ->
                        RpcUtil.fromProto(response.body().getNodeId()));
    }

    public CompletableFuture<GUID> linkNode(String src, String dst) {
        return linkNode(new LinkNode(src, dst));
    }

    @Override
    public CompletableFuture<Void> concatenateNodes(ConcatenateNodes req) {
        RpcClientRequestBuilder<TReqConcatenateNodes.Builder, RpcClientResponse<TRspConcatenateNodes>> builder =
                service.concatenateNodes();
        req.writeHeaderTo(builder.header());
        req.writeTo(builder.body());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    @Override
    public CompletableFuture<Void> concatenateNodes(String[] from, String to) {
        return concatenateNodes(from, to, null);
    }

    public CompletableFuture<Void> concatenateNodes(String[] from, String to, @Nullable Duration requestTimeout) {
        return concatenateNodes(new ConcatenateNodes(from, to).setTimeout(requestTimeout));
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

    private <T> CompletableFuture<T> lookupRowsImpl(AbstractLookupRowsRequest<?> request,
                                                    Function<RpcClientResponse<TRspLookupRows>, T> responseReader) {

        RpcClientRequestBuilder<TReqLookupRows.Builder, RpcClientResponse<TRspLookupRows>> builder =
                service.lookupRows();
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
        RpcClientRequestBuilder<TReqModifyRows.Builder, RpcClientResponse<TRspModifyRows>> builder =
                service.modifyRows();
        request.writeHeaderTo(builder.header());
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
        builder.body().setPath(request.getPath());
        if (request.getRequireSyncReplica().isPresent()) {
            builder.body().setRequireSyncReplica(request.getRequireSyncReplica().get());
        }
        builder.body().addAllRowModificationTypes(request.getRowModificationTypes());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));
        request.serializeRowsetTo(builder.attachments());
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    // TODO: TReqBatchModifyRows

    public CompletableFuture<Long> buildSnapshot(GUID cellId, boolean setReadOnly) {
        return buildSnapshot(cellId, setReadOnly, null);
    }

    public CompletableFuture<Long> buildSnapshot(GUID cellId, boolean setReadOnly, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqBuildSnapshot.Builder, RpcClientResponse<TRspBuildSnapshot>> builder =
                service.buildSnapshot();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setCellId(RpcUtil.toProto(cellId))
                .setSetReadOnly(setReadOnly);

        return RpcUtil.apply(invoke(builder), response -> response.body().getSnapshotId());
    }

    public CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(cellId, null);
    }

    public CompletableFuture<Void> gcCollect(GUID cellId, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGCCollect.Builder, RpcClientResponse<TRspGCCollect>> builder =
                service.gcCollect();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        return RpcUtil.apply(invoke(builder), response -> null);
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
            Iterable<? extends List<?>> keys) {
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
    private void runTabletsMountedChecker(String tablePath, CompletableFuture<Void> futureToComplete, ScheduledExecutorService executorService) {
        getNode(tablePath + "/@tablets").thenAccept(tablets -> {
            List<YTreeNode> tabletPaths = tablets.asList();
            Stream<Boolean> tabletsMounted = tabletPaths.stream()
                    .map(node -> node.asMap().getOrThrow("state").stringValue().equals("mounted"));
            if (tabletsMounted.allMatch(mounted -> mounted)) {
                futureToComplete.complete(null);
            } else {
                executorService.schedule(() -> runTabletsMountedChecker(tablePath, futureToComplete, executorService), 1, TimeUnit.SECONDS);
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
                        executor.schedule(() -> runTabletsMountedChecker(path, result, executor), 1, TimeUnit.SECONDS);
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
        return unmountTable(path, force, null);
    }

    public CompletableFuture<Void> unmountTable(String path, boolean force, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqUnmountTable.Builder, RpcClientResponse<TRspUnmountTable>> builder =
                service.unmountTable();
        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body().setPath(path);
        builder.body().setForce(force);
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> unmountTable(String path) {
        return unmountTable(path, null);
    }

    public CompletableFuture<Void> unmountTable(String path, @Nullable Duration requestTimeout) {
        return unmountTable(path, false, requestTimeout);
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

    public CompletableFuture<String> getFileFromCache(
            String md5,
            String cachePath,
            MasterReadOptions mo) {
        return getFileFromCache(md5, cachePath, mo, null);
    }

    public CompletableFuture<String> getFileFromCache(
            String md5,
            String cachePath,
            MasterReadOptions mo,
            @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGetFileFromCache.Builder, RpcClientResponse<TRspGetFileFromCache>>
                builder = service.getFileFromCache();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setMd5(md5)
                .setCachePath(cachePath)
                .setMasterReadOptions(mo.writeTo(
                        TMasterReadOptions.newBuilder()
                ));

        return RpcUtil.apply(invoke(builder), response -> response.body().getResult().getPath());
    }

    public CompletableFuture<String> putFileToCache(
            String path,
            String md5,
            String cachePath,
            PrerequisiteOptions po,
            MasterReadOptions mro,
            MutatingOptions mo) {
        return putFileToCache(path, md5, cachePath, po, mro, mo, null);
    }

    public CompletableFuture<String> putFileToCache(
            String path,
            String md5,
            String cachePath,
            PrerequisiteOptions po,
            MasterReadOptions mro,
            MutatingOptions mo,
            @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqPutFileToCache.Builder, RpcClientResponse<TRspPutFileToCache>>
                builder = service.putFileToCache();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setPath(path)
                .setMd5(md5)
                .setCachePath(cachePath)
                .setMasterReadOptions(mro.writeTo(TMasterReadOptions.newBuilder()))
                .setMutatingOptions(mo.writeTo(TMutatingOptions.newBuilder()))
                .setPrerequisiteOptions(po.writeTo(TPrerequisiteOptions.newBuilder()));

        return RpcUtil.apply(invoke(builder), response -> response.body().getResult().getPath());
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

    public CompletableFuture<Void> abortOperation(GUID guid, String alias, String abortMessage) {
        return abortOperation(guid, alias, abortMessage, null);
    }

    public CompletableFuture<Void> abortOperation(GUID guid, String alias, String abortMessage, @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqAbortOperation.Builder, RpcClientResponse<TRspAbortOperation>>
                builder = service.abortOperation();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }
        if (abortMessage != null) {
            builder.body().setAbortMessage(abortMessage);
        }
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> suspendOperation(GUID guid, String alias, boolean abortRunningJobs) {
        return suspendOperation(guid, alias, abortRunningJobs, null);
    }

    public CompletableFuture<Void> suspendOperation(GUID guid, String alias, boolean abortRunningJobs, @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqSuspendOperation.Builder, RpcClientResponse<TRspSuspendOperation>>
                builder = service.suspendOperation();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }
        builder.body().setAbortRunningJobs(abortRunningJobs);
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> resumeOperation(GUID guid, String alias) {
        return resumeOperation(guid, alias, null);
    }

    public CompletableFuture<Void> resumeOperation(GUID guid, String alias, @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqResumeOperation.Builder, RpcClientResponse<TRspResumeOperation>>
                builder = service.resumeOperation();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> completeOperation(GUID guid, String alias) {
        return completeOperation(guid, alias, null);
    }

    public CompletableFuture<Void> completeOperation(GUID guid, String alias, @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqCompleteOperation.Builder, RpcClientResponse<TRspCompleteOperation>>
                builder = service.completeOperation();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }
        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> updateOperationParameters(GUID guid, String alias, YTreeNode parameters) {
        return updateOperationParameters(guid, alias, parameters, null);
    }

    public CompletableFuture<Void> updateOperationParameters(GUID guid, String alias, YTreeNode parameters, @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqUpdateOperationParameters.Builder, RpcClientResponse<TRspUpdateOperationParameters>>
                builder = service.updateOperationParameters();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }

        ByteString.Output output = ByteString.newOutput();

        YTreeBinarySerializer.serialize(parameters, output);
        builder.body().setParameters(output.toByteString());

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<YTreeNode> getOperation(
            GUID guid,
            String alias,
            List<String> attributes,
            boolean includeRuntime,
            MasterReadOptions mo) {
        return getOperation(guid, alias, attributes, includeRuntime, mo, null);
    }

    public CompletableFuture<YTreeNode> getOperation(GUID guid,
                                                     String alias,
                                                     List<String> attributes,
                                                     boolean includeRuntime,
                                                     MasterReadOptions mo,
                                                     @Nullable Duration requestTimeout) {
        if (guid == null && alias == null) {
            throw new IllegalArgumentException("guid or alias must be set");
        }
        if (guid != null && alias != null) {
            throw new IllegalArgumentException("one of guid or alias must be null");
        }

        RpcClientRequestBuilder<TReqGetOperation.Builder, RpcClientResponse<TRspGetOperation>>
                builder = service.getOperation();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        if (guid != null) {
            builder.body().setOperationId(RpcUtil.toProto(guid));
        }
        if (alias != null) {
            builder.body().setOperationAlias(alias);
        }

        builder.body()
                .addAllAttributes(attributes)
                .setIncludeRuntime(includeRuntime)
                .setMasterReadOptions(mo.writeTo(TMasterReadOptions.newBuilder()));

        return RpcUtil.apply(invoke(builder), response -> parseByteString(response.body().getMeta()));
    }

    /* */

    /* Jobs */

    public CompletableFuture<YTreeNode> getJob(GUID operatioinId, GUID jobId) {
        return getJob(operatioinId, jobId, null);
    }

    public CompletableFuture<YTreeNode> getJob(GUID operatioinId, GUID jobId, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqGetJob.Builder, RpcClientResponse<TRspGetJob>>
                builder = service.getJob();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setOperationId(RpcUtil.toProto(operatioinId))
                .setJobId(RpcUtil.toProto(jobId));

        return RpcUtil.apply(invoke(builder), response -> parseByteString(response.body().getInfo()));
    }

    public CompletableFuture<Void> dumpJobContext(GUID jobId) {
        return dumpJobContext(jobId, null);
    }

    public CompletableFuture<Void> dumpJobContext(GUID jobId, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqDumpJobContext.Builder, RpcClientResponse<TRspDumpJobContext>>
                builder = service.dumpJobContext();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setJobId(RpcUtil.toProto(jobId));

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<Void> abandonJob(GUID jobId) {
        return abandonJob(jobId, null);
    }

    public CompletableFuture<Void> abandonJob(GUID jobId, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqAbandonJob.Builder, RpcClientResponse<TRspAbandonJob>>
                builder = service.abandonJob();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setJobId(RpcUtil.toProto(jobId));

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    public CompletableFuture<YTreeNode> pollJobShell(GUID jobId, YTreeNode parameters) {
        return pollJobShell(jobId, parameters, null);
    }

    public CompletableFuture<YTreeNode> pollJobShell(GUID jobId, YTreeNode parameters, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqPollJobShell.Builder, RpcClientResponse<TRspPollJobShell>>
                builder = service.pollJobShell();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setJobId(RpcUtil.toProto(jobId));

        ByteString.Output output = ByteString.newOutput();

        YTreeBinarySerializer.serialize(parameters, output);
        builder.body().setParameters(output.toByteString());

        return RpcUtil.apply(invoke(builder), response -> parseByteString(response.body().getResult()));
    }

    public CompletableFuture<Void> abortJob(GUID jobId, @Nonnull Duration timeout) {
        return abortJob(jobId, timeout, null);
    }

    public CompletableFuture<Void> abortJob(GUID jobId, @Nonnull Duration timeout, @Nullable Duration requestTimeout) {
        RpcClientRequestBuilder<TReqAbortJob.Builder, RpcClientResponse<TRspAbortJob>>
                builder = service.abortJob();

        if (requestTimeout != null) {
            builder.setTimeout(requestTimeout);
        }
        builder.body()
                .setJobId(RpcUtil.toProto(jobId))
                .setInterruptTimeout(ApiServiceUtil.durationToYtMicros(timeout));

        return RpcUtil.apply(invoke(builder), response -> null);
    }

    /* */

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
}
