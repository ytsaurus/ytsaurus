package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.EAtomicity;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentReader;
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentWireProtocolReader;
import ru.yandex.yt.ytclient.proxy.request.AbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.AlterTable;
import ru.yandex.yt.ytclient.proxy.request.AlterTableReplica;
import ru.yandex.yt.ytclient.proxy.request.BuildSnapshot;
import ru.yandex.yt.ytclient.proxy.request.CommitTransaction;
import ru.yandex.yt.ytclient.proxy.request.CreateObject;
import ru.yandex.yt.ytclient.proxy.request.FreezeTable;
import ru.yandex.yt.ytclient.proxy.request.GcCollect;
import ru.yandex.yt.ytclient.proxy.request.GenerateTimestamps;
import ru.yandex.yt.ytclient.proxy.request.GetInSyncReplicas;
import ru.yandex.yt.ytclient.proxy.request.GetOperation;
import ru.yandex.yt.ytclient.proxy.request.GetTablePivotKeys;
import ru.yandex.yt.ytclient.proxy.request.GetTabletInfos;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.PingTransaction;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.ReadTableDirect;
import ru.yandex.yt.ytclient.proxy.request.RemountTable;
import ru.yandex.yt.ytclient.proxy.request.ReshardTable;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.TabletInfo;
import ru.yandex.yt.ytclient.proxy.request.TrimTable;
import ru.yandex.yt.ytclient.proxy.request.UnfreezeTable;
import ru.yandex.yt.ytclient.proxy.request.UnmountTable;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public interface ApiServiceClient extends TransactionalClient {
    CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction);

    /**
     * @deprecated prefer to use {@link #startTransaction(StartTransaction)}
     */
    @Deprecated
    default CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        return startTransaction(options.toStartTransaction());
    }

    CompletableFuture<Void> pingTransaction(PingTransaction req);

    default CompletableFuture<Void> pingTransaction(GUID id) {
        return pingTransaction(new PingTransaction(id));
    }

    CompletableFuture<Void> commitTransaction(CommitTransaction req);

    default CompletableFuture<Void> commitTransaction(GUID id) {
        return commitTransaction(new CommitTransaction(id));
    }

    CompletableFuture<Void> abortTransaction(AbortTransaction req);

    default CompletableFuture<Void> abortTransaction(GUID id) {
        return abortTransaction(new AbortTransaction(id));
    }


    CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req);

    CompletableFuture<GUID> createObject(CreateObject req);

    <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest<?> request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    <T> CompletableFuture<Void> versionedLookupRows(
            LookupRowsRequest request,
            YTreeObjectSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    @Deprecated
    default CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        return lookupRows(request.setTimestamp(timestamp));
    }

    @Deprecated
    default CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request, YtTimestamp timestamp) {
        return versionedLookupRows(request.setTimestamp(timestamp));
    }

    default CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(query, null);
    }

    default CompletableFuture<UnversionedRowset> selectRows(String query, @Nullable Duration requestTimeout) {
        return selectRows(SelectRowsRequest.of(query).setTimeout(requestTimeout));
    }

    <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeObjectSerializer<T> serializer,
                                           ConsumerSource<T> consumer);

    CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?> request);

    CompletableFuture<Long> buildSnapshot(BuildSnapshot req);

    CompletableFuture<Void> gcCollect(GcCollect req);

    default CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    CompletableFuture<Void> mountTable(MountTable req);

    /**
     * Unmount table.
     *
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see CompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    CompletableFuture<Void> unmountTable(UnmountTable req);

    default CompletableFuture<Void> remountTable(String path) {
        return remountTable(new RemountTable(path));
    }

    CompletableFuture<Void> remountTable(RemountTable req);

    default CompletableFuture<Void> freezeTable(String path) {
        return freezeTable(path, null);
    }

    default CompletableFuture<Void> freezeTable(String path, @Nullable Duration requestTimeout) {
        return freezeTable(new FreezeTable(path).setTimeout(requestTimeout));
    }

    CompletableFuture<Void> freezeTable(FreezeTable req);

    default CompletableFuture<Void> unfreezeTable(String path) {
        return unfreezeTable(path, null);
    }

    default CompletableFuture<Void> unfreezeTable(String path, @Nullable Duration requestTimeout) {
        return unfreezeTable(new UnfreezeTable(path).setTimeout(requestTimeout));
    }

    default CompletableFuture<Void> unfreezeTable(FreezeTable req) {
        UnfreezeTable unfreezeReq = new UnfreezeTable(req.getPath());
        if (req.getTimeout().isPresent()) {
            unfreezeReq.setTimeout(req.getTimeout().get());
        }
        return unfreezeTable(unfreezeReq);
    }

    CompletableFuture<Void> unfreezeTable(UnfreezeTable req);

    CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp);

    default CompletableFuture<List<GUID>> getInSyncReplicas(
            String path,
            YtTimestamp timestamp,
            TableSchema schema,
            Iterable<? extends List<?>> keys
    ) {
        return getInSyncReplicas(new GetInSyncReplicas(path, schema, keys), timestamp);
    }

    CompletableFuture<List<TabletInfo>> getTabletInfos(GetTabletInfos req);

    default CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices) {
        GetTabletInfos req = new GetTabletInfos(path);
        req.setTabletIndexes(tabletIndices);
        return getTabletInfos(req);
    }

    CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req);

    default CompletableFuture<YtTimestamp> generateTimestamps(int count) {
        GenerateTimestamps req = new GenerateTimestamps(count);
        return generateTimestamps(req);
    }

    default CompletableFuture<YtTimestamp> generateTimestamps() {
        return generateTimestamps(1);
    }

    CompletableFuture<Void> reshardTable(ReshardTable req);

    default CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        TrimTable req = new TrimTable(path, tableIndex, trimmedRowCount);
        return trimTable(req);
    }

    CompletableFuture<Void> trimTable(TrimTable req);

    CompletableFuture<Void> alterTable(AlterTable req);

    CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity
    );

    CompletableFuture<Void> alterTableReplica(AlterTableReplica req);

    CompletableFuture<YTreeNode> getOperation(GetOperation req);

    <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req,
                                                    TableAttachmentReader<T> reader);

    default CompletableFuture<TableReader<byte[]>> readTableDirect(ReadTableDirect req) {
        return readTable(req, TableAttachmentReader.BYPASS);
    }

    default <T> CompletableFuture<TableReader<T>> readTable(ReadTable<T> req) {
        return readTable(req, new TableAttachmentWireProtocolReader<>(req.getDeserializer()));
    }
}
