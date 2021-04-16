package ru.yandex.yt.ytclient.proxy;

import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.yt.TReqDiscoverProxies;
import ru.yandex.yt.TRspDiscoverProxies;
import ru.yandex.yt.rpc.TReqDiscover;
import ru.yandex.yt.rpc.TRspDiscover;
import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
import ru.yandex.yt.rpcproxy.TReqAlterTable;
import ru.yandex.yt.rpcproxy.TReqAlterTableReplica;
import ru.yandex.yt.rpcproxy.TReqBuildSnapshot;
import ru.yandex.yt.rpcproxy.TReqCheckPermission;
import ru.yandex.yt.rpcproxy.TReqCommitTransaction;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TReqCopyNode;
import ru.yandex.yt.rpcproxy.TReqCreateNode;
import ru.yandex.yt.rpcproxy.TReqCreateObject;
import ru.yandex.yt.rpcproxy.TReqExistsNode;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqGCCollect;
import ru.yandex.yt.rpcproxy.TReqGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TReqGetOperation;
import ru.yandex.yt.rpcproxy.TReqGetTablePivotKeys;
import ru.yandex.yt.rpcproxy.TReqGetTabletInfos;
import ru.yandex.yt.rpcproxy.TReqLinkNode;
import ru.yandex.yt.rpcproxy.TReqListNode;
import ru.yandex.yt.rpcproxy.TReqLockNode;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.rpcproxy.TReqMountTable;
import ru.yandex.yt.rpcproxy.TReqMoveNode;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.rpcproxy.TReqSelectRows;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TRspAbortTransaction;
import ru.yandex.yt.rpcproxy.TRspAlterTable;
import ru.yandex.yt.rpcproxy.TRspAlterTableReplica;
import ru.yandex.yt.rpcproxy.TRspBuildSnapshot;
import ru.yandex.yt.rpcproxy.TRspCheckPermission;
import ru.yandex.yt.rpcproxy.TRspCommitTransaction;
import ru.yandex.yt.rpcproxy.TRspConcatenateNodes;
import ru.yandex.yt.rpcproxy.TRspCopyNode;
import ru.yandex.yt.rpcproxy.TRspCreateNode;
import ru.yandex.yt.rpcproxy.TRspCreateObject;
import ru.yandex.yt.rpcproxy.TRspExistsNode;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspGCCollect;
import ru.yandex.yt.rpcproxy.TRspGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TRspGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.rpcproxy.TRspGetOperation;
import ru.yandex.yt.rpcproxy.TRspGetTablePivotKeys;
import ru.yandex.yt.rpcproxy.TRspGetTabletInfos;
import ru.yandex.yt.rpcproxy.TRspLinkNode;
import ru.yandex.yt.rpcproxy.TRspListNode;
import ru.yandex.yt.rpcproxy.TRspLockNode;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspModifyRows;
import ru.yandex.yt.rpcproxy.TRspMountTable;
import ru.yandex.yt.rpcproxy.TRspMoveNode;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspRemoveNode;
import ru.yandex.yt.rpcproxy.TRspReshardTable;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspSetNode;
import ru.yandex.yt.rpcproxy.TRspStartOperation;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspTrimTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;

public class ApiServiceMethodTable {
    public static final RpcMethodDescriptor<TReqStartTransaction.Builder, TRspStartTransaction> START_TRANSACTION =
            apiServiceMethod("StartTransaction", TReqStartTransaction::newBuilder, TRspStartTransaction.parser());

    public static final RpcMethodDescriptor<TReqPingTransaction.Builder, TRspPingTransaction> PING_TRANSACTION =
            apiServiceMethod("PingTransaction", TReqPingTransaction::newBuilder, TRspPingTransaction.parser());

    public static final RpcMethodDescriptor<TReqCommitTransaction.Builder, TRspCommitTransaction> COMMIT_TRANSACTION =
            apiServiceMethod("CommitTransaction", TReqCommitTransaction::newBuilder, TRspCommitTransaction.parser());

    public static final RpcMethodDescriptor<TReqAbortTransaction.Builder, TRspAbortTransaction> ABORT_TRANSACTION =
            apiServiceMethod("AbortTransaction", TReqAbortTransaction::newBuilder, TRspAbortTransaction.parser());

    public static final RpcMethodDescriptor<TReqLookupRows.Builder, TRspLookupRows> LOOKUP_ROWS =
            apiServiceMethod("LookupRows", TReqLookupRows::newBuilder, TRspLookupRows.parser());

    public static final RpcMethodDescriptor<TReqVersionedLookupRows.Builder, TRspVersionedLookupRows>
            VERSIONED_LOOKUP_ROWS =
            apiServiceMethod(
                    "VersionedLookupRows",
                    TReqVersionedLookupRows::newBuilder,
                    TRspVersionedLookupRows.parser()
            );

    public static final RpcMethodDescriptor<TReqSelectRows.Builder, TRspSelectRows> SELECT_ROWS =
            apiServiceMethod("SelectRows", TReqSelectRows::newBuilder, TRspSelectRows.parser());

    public static final RpcMethodDescriptor<TReqModifyRows.Builder, TRspModifyRows> MODIFY_ROWS =
            apiServiceMethod("ModifyRows", TReqModifyRows::newBuilder, TRspModifyRows.parser());

    public static final RpcMethodDescriptor<TReqBuildSnapshot.Builder, TRspBuildSnapshot> BUILD_SNAPSHOT =
            apiServiceMethod("BuildSnapshot", TReqBuildSnapshot::newBuilder, TRspBuildSnapshot.parser());

    public static final RpcMethodDescriptor<TReqGCCollect.Builder, TRspGCCollect> GC_COLLECT =
            apiServiceMethod("GcCollect", TReqGCCollect::newBuilder, TRspGCCollect.parser());

    public static final RpcMethodDescriptor<TReqGetInSyncReplicas.Builder, TRspGetInSyncReplicas> GET_IN_SYNC_REPLICAS =
            apiServiceMethod("GetInSyncReplicas", TReqGetInSyncReplicas::newBuilder, TRspGetInSyncReplicas.parser());

    public static final RpcMethodDescriptor<TReqGetTabletInfos.Builder, TRspGetTabletInfos> GET_TABLET_INFOS =
            apiServiceMethod("GetTabletInfos", TReqGetTabletInfos::newBuilder, TRspGetTabletInfos.parser());

    public static final RpcMethodDescriptor<TReqGenerateTimestamps.Builder, TRspGenerateTimestamps>
            GENERATE_TIMESTAMPS =
            apiServiceMethod("GenerateTimestamps", TReqGenerateTimestamps::newBuilder, TRspGenerateTimestamps.parser());

    public static final RpcMethodDescriptor<TReqCreateObject.Builder, TRspCreateObject> CREATE_OBJECT =
            apiServiceMethod("CreateObject", TReqCreateObject::newBuilder, TRspCreateObject.parser());

    public static final RpcMethodDescriptor<TReqGetNode.Builder, TRspGetNode> GET_NODE =
            apiServiceMethod("GetNode", TReqGetNode::newBuilder, TRspGetNode.parser());

    public static final RpcMethodDescriptor<TReqSetNode.Builder, TRspSetNode> SET_NODE =
            apiServiceMethod("SetNode", TReqSetNode::newBuilder, TRspSetNode.parser());

    public static final RpcMethodDescriptor<TReqExistsNode.Builder, TRspExistsNode> EXISTS_NODE =
            apiServiceMethod("ExistsNode", TReqExistsNode::newBuilder, TRspExistsNode.parser());

    public static final RpcMethodDescriptor<TReqListNode.Builder, TRspListNode> LIST_NODE =
            apiServiceMethod("ListNode", TReqListNode::newBuilder, TRspListNode.parser());

    public static final RpcMethodDescriptor<TReqCreateNode.Builder, TRspCreateNode> CREATE_NODE =
            apiServiceMethod("CreateNode", TReqCreateNode::newBuilder, TRspCreateNode.parser());

    public static final RpcMethodDescriptor<TReqGetTablePivotKeys.Builder, TRspGetTablePivotKeys> GET_TABLE_PIVOT_KEYS =
            apiServiceMethod("GetTablePivotKeys", TReqGetTablePivotKeys::newBuilder, TRspGetTablePivotKeys.parser());

    public static final RpcMethodDescriptor<TReqRemoveNode.Builder, TRspRemoveNode> REMOVE_NODE =
            apiServiceMethod("RemoveNode", TReqRemoveNode::newBuilder, TRspRemoveNode.parser());

    public static final RpcMethodDescriptor<TReqLockNode.Builder, TRspLockNode> LOCK_NODE =
            apiServiceMethod("LockNode", TReqLockNode::newBuilder, TRspLockNode.parser());

    public static final RpcMethodDescriptor<TReqCopyNode.Builder, TRspCopyNode> COPY_NODE =
            apiServiceMethod("CopyNode", TReqCopyNode::newBuilder, TRspCopyNode.parser());

    public static final RpcMethodDescriptor<TReqMoveNode.Builder, TRspMoveNode> MOVE_NODE =
            apiServiceMethod("MoveNode", TReqMoveNode::newBuilder, TRspMoveNode.parser());

    public static final RpcMethodDescriptor<TReqLinkNode.Builder, TRspLinkNode> LINK_NODE =
            apiServiceMethod("LinkNode", TReqLinkNode::newBuilder, TRspLinkNode.parser());

    public static final RpcMethodDescriptor<TReqConcatenateNodes.Builder, TRspConcatenateNodes> CONCATENATE_NODES =
            apiServiceMethod("ConcatenateNodes", TReqConcatenateNodes::newBuilder, TRspConcatenateNodes.parser());

    public static final RpcMethodDescriptor<TReqMountTable.Builder, TRspMountTable> MOUNT_TABLE =
            apiServiceMethod("MountTable", TReqMountTable::newBuilder, TRspMountTable.parser());

    public static final RpcMethodDescriptor<TReqUnmountTable.Builder, TRspUnmountTable> UNMOUNT_TABLE =
            apiServiceMethod("UnmountTable", TReqUnmountTable::newBuilder, TRspUnmountTable.parser());

    public static final RpcMethodDescriptor<TReqRemountTable.Builder, TRspRemountTable> REMOUNT_TABLE =
            apiServiceMethod("RemountTable", TReqRemountTable::newBuilder, TRspRemountTable.parser());

    public static final RpcMethodDescriptor<TReqFreezeTable.Builder, TRspFreezeTable> FREEZE_TABLE =
            apiServiceMethod("FreezeTable", TReqFreezeTable::newBuilder, TRspFreezeTable.parser());

    public static final RpcMethodDescriptor<TReqUnfreezeTable.Builder, TRspUnfreezeTable> UNFREEZE_TABLE =
            apiServiceMethod("UnfreezeTable", TReqUnfreezeTable::newBuilder, TRspUnfreezeTable.parser());

    public static final RpcMethodDescriptor<TReqReshardTable.Builder, TRspReshardTable> RESHARD_TABLE =
            apiServiceMethod("ReshardTable", TReqReshardTable::newBuilder, TRspReshardTable.parser());

    public static final RpcMethodDescriptor<TReqTrimTable.Builder, TRspTrimTable> TRIM_TABLE =
            apiServiceMethod("TrimTable", TReqTrimTable::newBuilder, TRspTrimTable.parser());

    public static final RpcMethodDescriptor<TReqAlterTable.Builder, TRspAlterTable> ALTER_TABLE =
            apiServiceMethod("AlterTable", TReqAlterTable::newBuilder, TRspAlterTable.parser());

    public static final RpcMethodDescriptor<TReqAlterTableReplica.Builder, TRspAlterTableReplica> ALTER_TABLE_REPLICA =
            apiServiceMethod("AlterTableReplica", TReqAlterTableReplica::newBuilder, TRspAlterTableReplica.parser());

    public static final RpcMethodDescriptor<TReqStartOperation.Builder, TRspStartOperation> START_OPERATION =
            apiServiceMethod("StartOperation", TReqStartOperation::newBuilder, TRspStartOperation.parser());

    public static final RpcMethodDescriptor<TReqGetOperation.Builder, TRspGetOperation> GET_OPERATION =
            apiServiceMethod("GetOperation", TReqGetOperation::newBuilder, TRspGetOperation.parser());

    public static final RpcMethodDescriptor<TReqCheckPermission.Builder, TRspCheckPermission> CHECK_PERMISSION =
            apiServiceMethod("CheckPermission", TReqCheckPermission::newBuilder, TRspCheckPermission.parser());

    public static final RpcMethodDescriptor<TReqReadTable.Builder, TRspReadTable> READ_TABLE =
            apiServiceMethod("ReadTable", TReqReadTable::newBuilder, TRspReadTable.parser());

    public static final RpcMethodDescriptor<TReqWriteTable.Builder, TRspWriteTable> WRITE_TABLE =
            apiServiceMethod("WriteTable", TReqWriteTable::newBuilder, TRspWriteTable.parser());

    public static final RpcMethodDescriptor<TReqReadFile.Builder, TRspReadFile> READ_FILE =
            apiServiceMethod("ReadFile", TReqReadFile::newBuilder, TRspReadFile.parser());

    public static final RpcMethodDescriptor<TReqWriteFile.Builder, TRspWriteFile> WRITE_FILE =
            apiServiceMethod("WriteFile", TReqWriteFile::newBuilder, TRspWriteFile.parser());

    public static final RpcMethodDescriptor<TReqDiscover.Builder, TRspDiscover> DISCOVER =
            apiServiceMethod("Discover", TReqDiscover::newBuilder, TRspDiscover.parser());

    public static final RpcMethodDescriptor<TReqDiscoverProxies.Builder, TRspDiscoverProxies> DISCOVER_PROXIES =
            discoveryServiceMethod("DiscoverProxies", TReqDiscoverProxies::newBuilder, TRspDiscoverProxies.parser());

    private ApiServiceMethodTable() {
    }

    public static <TReqBuilder extends MessageLite.Builder, TRes extends MessageLite>
    RpcMethodDescriptor<TReqBuilder, TRes> apiServiceMethod(
            String name,
            Supplier<TReqBuilder> reqSupplier,
            Parser<TRes> parser
    ) {
        return new RpcMethodDescriptor<>(
                1,
                "ApiService",
                name,
                reqSupplier,
                parser);
    }

    public static <TReqBuilder extends MessageLite.Builder, TRes extends MessageLite>
    RpcMethodDescriptor<TReqBuilder, TRes> discoveryServiceMethod(
            String name,
            Supplier<TReqBuilder> reqSupplier,
            Parser<TRes> parser
    ) {
        return new RpcMethodDescriptor<>(
                0,
                "DiscoveryService",
                name,
                reqSupplier,
                parser);
    }
}
