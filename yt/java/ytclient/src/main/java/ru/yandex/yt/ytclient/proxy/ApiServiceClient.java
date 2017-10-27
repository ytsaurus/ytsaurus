package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
import ru.yandex.yt.rpcproxy.TReqCommitTransaction;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqSelectRows;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspAbortTransaction;
import ru.yandex.yt.rpcproxy.TRspCommitTransaction;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspModifyRows;
import ru.yandex.yt.rpcproxy.TRspMountTable;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspSetNode;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspTrimTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.misc.YtTimestamp;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;
import ru.yandex.yt.ytclient.ytree.YTreeNode;

/**
 * Клиент для высокоуровневой работы с ApiService
 */
public class ApiServiceClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiServiceClient.class);

    private final ApiService service;
    private final Executor heavyExecutor;

    public ApiServiceClient(ApiService service, Executor heavyExecutor) {
        this.service = Objects.requireNonNull(service);
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
    }

    public ApiServiceClient(ApiService service) {
        this(service, ForkJoinPool.commonPool());
    }

    public ApiServiceClient(RpcClient client, RpcOptions options) {
        this(client.getService(ApiService.class, options));
    }

    public ApiServiceClient(RpcClient client) {
        this(client, new RpcOptions());
    }

    public ApiService getService() {
        return service;
    }

    public CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> builder =
                service.startTransaction();
        builder.body().setType(options.getType());
        Duration timeout = options.getTimeout();
        if (timeout != null) {
            builder.body().setTimeout(ApiServiceUtil.durationToYtMicros(timeout));
        }
        if (options.getId() != null && !options.getId().isEmpty()) {
            builder.body().setId(options.getId().toProto());
        }
        if (options.getParentId() != null && !options.getParentId().isEmpty()) {
            builder.body().setParentId(options.getParentId().toProto());
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
        final boolean ping = builder.body().getPing();
        final boolean sticky = builder.body().getSticky();
        return RpcUtil.apply(builder.invoke(), response -> {
            YtGuid id = YtGuid.fromProto(response.body().getId());
            YtTimestamp startTimestamp = YtTimestamp.valueOf(response.body().getStartTimestamp());
            return new ApiServiceTransaction(this, id, startTimestamp, ping, sticky);
        });
    }

    public CompletableFuture<Void> pingTransaction(YtGuid id, boolean sticky) {
        RpcClientRequestBuilder<TReqPingTransaction.Builder, RpcClientResponse<TRspPingTransaction>> builder =
                service.pingTransaction();
        builder.body().setTransactionId(id.toProto());
        builder.body().setSticky(sticky);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> commitTransaction(YtGuid id, boolean sticky) {
        RpcClientRequestBuilder<TReqCommitTransaction.Builder, RpcClientResponse<TRspCommitTransaction>> builder =
                service.commitTransaction();
        builder.body().setTransactionId(id.toProto());
        builder.body().setSticky(sticky);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> abortTransaction(YtGuid id, boolean sticky) {
        RpcClientRequestBuilder<TReqAbortTransaction.Builder, RpcClientResponse<TRspAbortTransaction>> builder =
                service.abortTransaction();
        builder.body().setTransactionId(id.toProto());
        builder.body().setSticky(sticky);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    /* nodes */
    public CompletableFuture<YTreeNode> getNode(String path) {
        RpcClientRequestBuilder<TReqGetNode.Builder, RpcClientResponse<TRspGetNode>> builder = service.getNode();
        builder.body().setPath(path);
        return RpcUtil.apply(builder.invoke(), response -> YTreeNode.parseByteString(response.body().getValue()));
    }

    public CompletableFuture<Void> setNode(String path, byte[] data) {
        RpcClientRequestBuilder<TReqSetNode.Builder, RpcClientResponse<TRspSetNode>> builder = service.setNode();
        builder.body()
                .setPath(path)
                .setValue(ByteString.copyFrom(data));
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return setNode(path, data.toBinary());
    }

    public CompletableFuture<Boolean> existsNode() {
        CompletableFuture<Boolean> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<YTreeNode> listNode() {
        CompletableFuture<YTreeNode> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<YtGuid> createNode() {
        CompletableFuture<YtGuid> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<Void> removeNode() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<Void> lockNode() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<YtGuid> copyNode() {
        CompletableFuture<YtGuid> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<YtGuid> moveNode() {
        CompletableFuture<YtGuid> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<YtGuid> linkNode() {
        CompletableFuture<YtGuid> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<Void> concatenateNodes() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    /* */
    public CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        RpcClientRequestBuilder<TReqLookupRows.Builder, RpcClientResponse<TRspLookupRows>> builder =
                service.lookupRows();
        builder.body().setPath(request.getPath());
        builder.body().addAllColumns(request.getLookupColumns());
        if (request.getKeepMissingRows().isPresent()) {
            builder.body().setKeepMissingRows(request.getKeepMissingRows().get());
        }
        if (timestamp != null) {
            builder.body().setTimestamp(timestamp.getValue());
        }
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));
        request.serializeRowsetTo(builder.attachments());
        return handleHeavyResponse(builder.invoke(), response -> {
            logger.trace("LookupRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
            return ApiServiceUtil
                    .deserializeUnversionedRowset(response.body().getRowsetDescriptor(), response.attachments());
        });
    }

    public CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request) {
        return lookupRows(request, null);
    }

    public CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        RpcClientRequestBuilder<TReqVersionedLookupRows.Builder, RpcClientResponse<TRspVersionedLookupRows>> builder =
                service.versionedLookupRows();
        builder.body().setPath(request.getPath());
        builder.body().addAllColumns(request.getLookupColumns());
        if (request.getKeepMissingRows().isPresent()) {
            builder.body().setKeepMissingRows(request.getKeepMissingRows().get());
        }
        if (timestamp != null) {
            builder.body().setTimestamp(timestamp.getValue());
        }
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));
        request.serializeRowsetTo(builder.attachments());
        return handleHeavyResponse(builder.invoke(), response -> {
            logger.trace("VersionedLookupRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
            return ApiServiceUtil
                    .deserializeVersionedRowset(response.body().getRowsetDescriptor(), response.attachments());
        });
    }

    public CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request) {
        return versionedLookupRows(request, null);
    }

    public CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(SelectRowsRequest.of(query));
    }

    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        RpcClientRequestBuilder<TReqSelectRows.Builder, RpcClientResponse<TRspSelectRows>> builder =
                service.selectRows();
        builder.body().setQuery(request.getQuery());
        if (request.getTimestamp().isPresent()) {
            builder.body().setTimestamp(request.getTimestamp().get().getValue());
        }
        if (request.getInputRowsLimit().isPresent()) {
            builder.body().setInputRowLimit(request.getInputRowsLimit().getAsLong());
        }
        if (request.getOutputRowsLimit().isPresent()) {
            builder.body().setOutputRowLimit(request.getOutputRowsLimit().getAsLong());
        }
        return handleHeavyResponse(builder.invoke(), response -> {
            logger.trace("SelectRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
            return ApiServiceUtil
                    .deserializeUnversionedRowset(response.body().getRowsetDescriptor(), response.attachments());
        });
    }

    public CompletableFuture<Void> modifyRows(YtGuid transactionId, ModifyRowsRequest request) {
        RpcClientRequestBuilder<TReqModifyRows.Builder, RpcClientResponse<TRspModifyRows>> builder =
                service.modifyRows();
        builder.body().setTransactionId(transactionId.toProto());
        builder.body().setPath(request.getPath());
        if (request.getRequireSyncReplica().isPresent()) {
            builder.body().setRequireSyncReplica(request.getRequireSyncReplica().get());
        }
        builder.body().addAllRowModificationTypes(request.getRowModificationTypes());
        builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(request.getSchema()));
        request.serializeRowsetTo(builder.attachments());
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<List<YtGuid>> getInSyncReplicas(String path, YtTimestamp timestamp) {
        RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, RpcClientResponse<TRspGetInSyncReplicas>> builder =
                service.getInSyncReplicas();

        builder.body().setPath(path);
        builder.body().setTimestamp(timestamp.getValue());

        return RpcUtil.apply(builder.invoke(),
                response ->
                        response.body().getReplicaIdsList().stream().map(YtGuid::fromProto).collect(Collectors.toList()));
    }

    public CompletableFuture<List<YtGuid>> getInSyncReplicas(String path) {
        return getInSyncReplicas(path, YtTimestamp.NULL);
    }

    /* tables */
    public CompletableFuture<Void> mountTable(String path, YtGuid cellId, boolean freeze) {
        RpcClientRequestBuilder<TReqMountTable.Builder, RpcClientResponse<TRspMountTable>> builder =
                service.mountTable();

        builder.body().setPath(path);
        builder.body().setFreeze(freeze);
        if (cellId != null) {
            builder.body().setCellId(cellId.toProto());
        }
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> mountTable(String path) {
        return mountTable(path, null, false);
    }

    public CompletableFuture<Void> mountTable(String path, boolean freeze) {
        return mountTable(path, null, freeze);
    }

    public CompletableFuture<Void> unmountTable(String path, boolean force) {
        RpcClientRequestBuilder<TReqUnmountTable.Builder, RpcClientResponse<TRspUnmountTable>> builder =
                service.unmountTable();
        builder.body().setPath(path);
        builder.body().setForce(force);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> unmountTable(String path) {
        return unmountTable(path, false);
    }

    public CompletableFuture<Void> remountTable(String path) {
        RpcClientRequestBuilder<TReqRemountTable.Builder, RpcClientResponse<TRspRemountTable>> builder =
                service.remountTable();
        builder.body().setPath(path);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> freezeTable(String path) {
        RpcClientRequestBuilder<TReqFreezeTable.Builder, RpcClientResponse<TRspFreezeTable>> builder =
                service.freezeTable();
        builder.body().setPath(path);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> unfreezeTable(String path) {
        RpcClientRequestBuilder<TReqUnfreezeTable.Builder, RpcClientResponse<TRspUnfreezeTable>> builder =
                service.unfreezeTable();
        builder.body().setPath(path);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> reshardTable() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        RpcClientRequestBuilder<TReqTrimTable.Builder, RpcClientResponse<TRspTrimTable>> builder =
                service.trimTable();
        builder.body().setPath(path);
        builder.body().setTabletIndex(tableIndex);
        builder.body().setTrimmedRowCount(trimmedRowCount);
        return RpcUtil.apply(builder.invoke(), response -> null);
    }

    public CompletableFuture<Void> alterTable() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }

    public CompletableFuture<Void> alterTableReplica() {
        CompletableFuture<Void> r = new CompletableFuture<>();
        r.completeExceptionally(new RuntimeException("unimplemented"));
        return r;
    }
    /* */

    private <T, Response> CompletableFuture<T> handleHeavyResponse(CompletableFuture<Response> future,
                                                                   Function<Response, T> fn) {
        return RpcUtil.applyAsync(future, fn, heavyExecutor);
    }
}
