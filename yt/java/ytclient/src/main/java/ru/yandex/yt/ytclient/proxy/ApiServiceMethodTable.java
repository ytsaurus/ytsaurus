package ru.yandex.yt.ytclient.proxy;

import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.yt.TReqDiscoverProxies;
import ru.yandex.yt.TRspDiscoverProxies;
import ru.yandex.yt.rpcproxy.TReqAbandonJob;
import ru.yandex.yt.rpcproxy.TReqAbortJob;
import ru.yandex.yt.rpcproxy.TReqAbortOperation;
import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
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
import ru.yandex.yt.rpcproxy.TReqExistsNode;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.rpcproxy.TReqGCCollect;
import ru.yandex.yt.rpcproxy.TReqGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TReqGetFileFromCache;
import ru.yandex.yt.rpcproxy.TReqGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TReqGetJob;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TReqGetOperation;
import ru.yandex.yt.rpcproxy.TReqGetTableMountInfo;
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
import ru.yandex.yt.rpcproxy.TReqPollJobShell;
import ru.yandex.yt.rpcproxy.TReqPutFileToCache;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.rpcproxy.TReqReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TReqResumeOperation;
import ru.yandex.yt.rpcproxy.TReqSelectRows;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqSuspendOperation;
import ru.yandex.yt.rpcproxy.TReqTrimTable;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.rpcproxy.TReqUpdateOperationParameters;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TReqWriteFile;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TRspAbandonJob;
import ru.yandex.yt.rpcproxy.TRspAbortJob;
import ru.yandex.yt.rpcproxy.TRspAbortOperation;
import ru.yandex.yt.rpcproxy.TRspAbortTransaction;
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
import ru.yandex.yt.rpcproxy.TRspExistsNode;
import ru.yandex.yt.rpcproxy.TRspFreezeTable;
import ru.yandex.yt.rpcproxy.TRspGCCollect;
import ru.yandex.yt.rpcproxy.TRspGenerateTimestamps;
import ru.yandex.yt.rpcproxy.TRspGetFileFromCache;
import ru.yandex.yt.rpcproxy.TRspGetInSyncReplicas;
import ru.yandex.yt.rpcproxy.TRspGetJob;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.rpcproxy.TRspGetOperation;
import ru.yandex.yt.rpcproxy.TRspGetTableMountInfo;
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
import ru.yandex.yt.rpcproxy.TRspPollJobShell;
import ru.yandex.yt.rpcproxy.TRspPutFileToCache;
import ru.yandex.yt.rpcproxy.TRspReadFile;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.rpcproxy.TRspRemountTable;
import ru.yandex.yt.rpcproxy.TRspRemoveNode;
import ru.yandex.yt.rpcproxy.TRspReshardTable;
import ru.yandex.yt.rpcproxy.TRspReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TRspResumeOperation;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspSetNode;
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

public class ApiServiceMethodTable {
    public static final RpcMethodDescriptor<TReqStartTransaction.Builder, TRspStartTransaction> startTransaction =
            apiServiceMethod("StartTransaction", TReqStartTransaction::newBuilder, TRspStartTransaction.parser());

    public static final RpcMethodDescriptor<TReqPingTransaction.Builder, TRspPingTransaction> pingTransaction =
            apiServiceMethod("PingTransaction", TReqPingTransaction::newBuilder, TRspPingTransaction.parser());

    public static final RpcMethodDescriptor<TReqCommitTransaction.Builder, TRspCommitTransaction> commitTransaction =
            apiServiceMethod("CommitTransaction", TReqCommitTransaction::newBuilder, TRspCommitTransaction.parser());

    public static final RpcMethodDescriptor<TReqAbortTransaction.Builder, TRspAbortTransaction> abortTransaction =
            apiServiceMethod("AbortTransaction", TReqAbortTransaction::newBuilder, TRspAbortTransaction.parser());

    public static final RpcMethodDescriptor<TReqLookupRows.Builder, TRspLookupRows> lookupRows =
            apiServiceMethod("LookupRows", TReqLookupRows::newBuilder, TRspLookupRows.parser());

    public static final RpcMethodDescriptor<TReqVersionedLookupRows.Builder, TRspVersionedLookupRows> versionedLookupRows =
            apiServiceMethod("VersionedLookupRows", TReqVersionedLookupRows::newBuilder, TRspVersionedLookupRows.parser());

    public static final RpcMethodDescriptor<TReqSelectRows.Builder, TRspSelectRows> selectRows =
            apiServiceMethod("SelectRows", TReqSelectRows::newBuilder, TRspSelectRows.parser());

    public static final RpcMethodDescriptor<TReqModifyRows.Builder, TRspModifyRows> modifyRows =
            apiServiceMethod("ModifyRows", TReqModifyRows::newBuilder, TRspModifyRows.parser());

    public static final RpcMethodDescriptor<TReqBuildSnapshot.Builder, TRspBuildSnapshot> buildSnapshot =
            apiServiceMethod("BuildSnapshot", TReqBuildSnapshot::newBuilder, TRspBuildSnapshot.parser());

    public static final RpcMethodDescriptor<TReqGCCollect.Builder, TRspGCCollect> gcCollect =
            apiServiceMethod("GcCollect", TReqGCCollect::newBuilder, TRspGCCollect.parser());

    public static final RpcMethodDescriptor<TReqGetInSyncReplicas.Builder, TRspGetInSyncReplicas> getInSyncReplicas =
            apiServiceMethod("GetInSyncReplicas", TReqGetInSyncReplicas::newBuilder, TRspGetInSyncReplicas.parser());

    public static final RpcMethodDescriptor<TReqGetTabletInfos.Builder, TRspGetTabletInfos> getTabletInfos =
            apiServiceMethod("GetTabletInfos", TReqGetTabletInfos::newBuilder, TRspGetTabletInfos.parser());

    public static final RpcMethodDescriptor<TReqGenerateTimestamps.Builder, TRspGenerateTimestamps> generateTimestamps =
            apiServiceMethod("GenerateTimestamps", TReqGenerateTimestamps::newBuilder, TRspGenerateTimestamps.parser());

    public static final RpcMethodDescriptor<TReqCreateObject.Builder, TRspCreateObject> createObject =
            apiServiceMethod("CreateObject", TReqCreateObject::newBuilder, TRspCreateObject.parser());

    public static final RpcMethodDescriptor<TReqGetNode.Builder, TRspGetNode> getNode =
            apiServiceMethod("GetNode", TReqGetNode::newBuilder, TRspGetNode.parser());

    public static final RpcMethodDescriptor<TReqSetNode.Builder, TRspSetNode> setNode =
            apiServiceMethod("SetNode", TReqSetNode::newBuilder, TRspSetNode.parser());

    public static final RpcMethodDescriptor<TReqExistsNode.Builder, TRspExistsNode> existsNode =
            apiServiceMethod("ExistsNode", TReqExistsNode::newBuilder, TRspExistsNode.parser());

    public static final RpcMethodDescriptor<TReqListNode.Builder, TRspListNode> listNode =
            apiServiceMethod("ListNode", TReqListNode::newBuilder, TRspListNode.parser());

    public static final RpcMethodDescriptor<TReqCreateNode.Builder, TRspCreateNode> createNode =
            apiServiceMethod("CreateNode", TReqCreateNode::newBuilder, TRspCreateNode.parser());

    public static final RpcMethodDescriptor<TReqGetTableMountInfo.Builder, TRspGetTableMountInfo> getTableMountInfo =
            apiServiceMethod("GetTableMountInfo", TReqGetTableMountInfo::newBuilder, TRspGetTableMountInfo.parser());

    public static final RpcMethodDescriptor<TReqGetTablePivotKeys.Builder, TRspGetTablePivotKeys> getTablePivotKeys =
            apiServiceMethod("GetTablePivotKeys", TReqGetTablePivotKeys::newBuilder, TRspGetTablePivotKeys.parser());

    public static final RpcMethodDescriptor<TReqRemoveNode.Builder, TRspRemoveNode> removeNode =
            apiServiceMethod("RemoveNode", TReqRemoveNode::newBuilder, TRspRemoveNode.parser());

    public static final RpcMethodDescriptor<TReqLockNode.Builder, TRspLockNode> lockNode =
            apiServiceMethod("LockNode", TReqLockNode::newBuilder, TRspLockNode.parser());

    public static final RpcMethodDescriptor<TReqCopyNode.Builder, TRspCopyNode> copyNode =
            apiServiceMethod("CopyNode", TReqCopyNode::newBuilder, TRspCopyNode.parser());

    public static final RpcMethodDescriptor<TReqMoveNode.Builder, TRspMoveNode> moveNode =
            apiServiceMethod("MoveNode", TReqMoveNode::newBuilder, TRspMoveNode.parser());

    public static final RpcMethodDescriptor<TReqLinkNode.Builder, TRspLinkNode> linkNode =
            apiServiceMethod("LinkNode", TReqLinkNode::newBuilder, TRspLinkNode.parser());

    public static final RpcMethodDescriptor<TReqConcatenateNodes.Builder, TRspConcatenateNodes> concatenateNodes =
            apiServiceMethod("ConcatenateNodes", TReqConcatenateNodes::newBuilder, TRspConcatenateNodes.parser());

    public static final RpcMethodDescriptor<TReqMountTable.Builder, TRspMountTable> mountTable =
            apiServiceMethod("MountTable", TReqMountTable::newBuilder, TRspMountTable.parser());

    public static final RpcMethodDescriptor<TReqUnmountTable.Builder, TRspUnmountTable> unmountTable =
            apiServiceMethod("UnmountTable", TReqUnmountTable::newBuilder, TRspUnmountTable.parser());

    public static final RpcMethodDescriptor<TReqRemountTable.Builder, TRspRemountTable> remountTable =
            apiServiceMethod("RemountTable", TReqRemountTable::newBuilder, TRspRemountTable.parser());

    public static final RpcMethodDescriptor<TReqFreezeTable.Builder, TRspFreezeTable> freezeTable =
            apiServiceMethod("FreezeTable", TReqFreezeTable::newBuilder, TRspFreezeTable.parser());

    public static final RpcMethodDescriptor<TReqUnfreezeTable.Builder, TRspUnfreezeTable> unfreezeTable =
            apiServiceMethod("UnfreezeTable", TReqUnfreezeTable::newBuilder, TRspUnfreezeTable.parser());

    public static final RpcMethodDescriptor<TReqReshardTable.Builder, TRspReshardTable> reshardTable =
            apiServiceMethod("ReshardTable", TReqReshardTable::newBuilder, TRspReshardTable.parser());

    public static final RpcMethodDescriptor<TReqReshardTableAutomatic.Builder, TRspReshardTableAutomatic> reshardTableAutomatic =
            apiServiceMethod("ReshardTableAutomatic", TReqReshardTableAutomatic::newBuilder, TRspReshardTableAutomatic.parser());

    public static final RpcMethodDescriptor<TReqTrimTable.Builder, TRspTrimTable> trimTable =
            apiServiceMethod("TrimTable", TReqTrimTable::newBuilder, TRspTrimTable.parser());

    public static final RpcMethodDescriptor<TReqAlterTable.Builder, TRspAlterTable> alterTable =
            apiServiceMethod("AlterTable", TReqAlterTable::newBuilder, TRspAlterTable.parser());

    public static final RpcMethodDescriptor<TReqAlterTableReplica.Builder, TRspAlterTableReplica> alterTableReplica =
            apiServiceMethod("AlterTableReplica", TReqAlterTableReplica::newBuilder, TRspAlterTableReplica.parser());

    public static final RpcMethodDescriptor<TReqGetFileFromCache.Builder, TRspGetFileFromCache> getFileFromCache =
            apiServiceMethod("GetFileFromCache", TReqGetFileFromCache::newBuilder, TRspGetFileFromCache.parser());

    public static final RpcMethodDescriptor<TReqPutFileToCache.Builder, TRspPutFileToCache> putFileToCache =
            apiServiceMethod("PutFileToCache", TReqPutFileToCache::newBuilder, TRspPutFileToCache.parser());

    public static final RpcMethodDescriptor<TReqStartOperation.Builder, TRspStartOperation> startOperation =
            apiServiceMethod("StartOperation", TReqStartOperation::newBuilder, TRspStartOperation.parser());

    public static final RpcMethodDescriptor<TReqAbortOperation.Builder, TRspAbortOperation> abortOperation =
            apiServiceMethod("AbortOperation", TReqAbortOperation::newBuilder, TRspAbortOperation.parser());

    public static final RpcMethodDescriptor<TReqSuspendOperation.Builder, TRspSuspendOperation> suspendOperation =
            apiServiceMethod("SuspendOperation", TReqSuspendOperation::newBuilder, TRspSuspendOperation.parser());

    public static final RpcMethodDescriptor<TReqResumeOperation.Builder, TRspResumeOperation> resumeOperation =
            apiServiceMethod("ResumeOperation", TReqResumeOperation::newBuilder, TRspResumeOperation.parser());

    public static final RpcMethodDescriptor<TReqCompleteOperation.Builder, TRspCompleteOperation> completeOperation =
            apiServiceMethod("CompleteOperation", TReqCompleteOperation::newBuilder, TRspCompleteOperation.parser());

    public static final RpcMethodDescriptor<TReqUpdateOperationParameters.Builder, TRspUpdateOperationParameters> updateOperationParameters =
            apiServiceMethod("UpdateOperationParameters", TReqUpdateOperationParameters::newBuilder, TRspUpdateOperationParameters.parser());

    public static final RpcMethodDescriptor<TReqGetOperation.Builder, TRspGetOperation> getOperation =
            apiServiceMethod("GetOperation", TReqGetOperation::newBuilder, TRspGetOperation.parser());

    public static final RpcMethodDescriptor<TReqGetJob.Builder, TRspGetJob> getJob =
            apiServiceMethod("GetJob", TReqGetJob::newBuilder, TRspGetJob.parser());

    public static final RpcMethodDescriptor<TReqDumpJobContext.Builder, TRspDumpJobContext> dumpJobContext =
            apiServiceMethod("DumpJobContext", TReqDumpJobContext::newBuilder, TRspDumpJobContext.parser());

    public static final RpcMethodDescriptor<TReqAbandonJob.Builder, TRspAbandonJob> abandonJob =
            apiServiceMethod("AbandonJob", TReqAbandonJob::newBuilder, TRspAbandonJob.parser());

    public static final RpcMethodDescriptor<TReqPollJobShell.Builder, TRspPollJobShell> pollJobShell =
            apiServiceMethod("PollJobShell", TReqPollJobShell::newBuilder, TRspPollJobShell.parser());

    public static final RpcMethodDescriptor<TReqAbortJob.Builder, TRspAbortJob> abortJob =
            apiServiceMethod("AbortJob", TReqAbortJob::newBuilder, TRspAbortJob.parser());

    public static final RpcMethodDescriptor<TReqCheckPermission.Builder, TRspCheckPermission> checkPermission =
            apiServiceMethod("CheckPermission", TReqCheckPermission::newBuilder, TRspCheckPermission.parser());

    public static final RpcMethodDescriptor<TReqReadTable.Builder, TRspReadTable> readTable =
            apiServiceMethod("ReadTable", TReqReadTable::newBuilder, TRspReadTable.parser());

    public static final RpcMethodDescriptor<TReqWriteTable.Builder, TRspWriteTable> writeTable =
            apiServiceMethod("WriteTable", TReqWriteTable::newBuilder, TRspWriteTable.parser());

    public static final RpcMethodDescriptor<TReqReadFile.Builder, TRspReadFile> readFile =
            apiServiceMethod("ReadFile", TReqReadFile::newBuilder, TRspReadFile.parser());

    public static final RpcMethodDescriptor<TReqWriteFile.Builder, TRspWriteFile> writeFile =
            apiServiceMethod("WriteFile", TReqWriteFile::newBuilder, TRspWriteFile.parser());


    public static final RpcMethodDescriptor<TReqDiscoverProxies.Builder, TRspDiscoverProxies> discoverProxies =
            discoveryServiceMethod("DiscoverProxies", TReqDiscoverProxies::newBuilder, TRspDiscoverProxies.parser());

    static public <TReqBuilder extends MessageLite.Builder, TRes extends MessageLite>
    RpcMethodDescriptor<TReqBuilder, TRes> apiServiceMethod(
            String name,
            Supplier<TReqBuilder> reqSupplier,
            Parser<TRes> parser)
    {
        return new RpcMethodDescriptor<>(
                1,
                "ApiService",
                name,
                reqSupplier,
                parser);
    }

    static public <TReqBuilder extends MessageLite.Builder, TRes extends MessageLite>
    RpcMethodDescriptor<TReqBuilder, TRes> discoveryServiceMethod(
            String name,
            Supplier<TReqBuilder> reqSupplier,
            Parser<TRes> parser)
    {
        return new RpcMethodDescriptor<>(
                0,
                "DiscoveryService",
                name,
                reqSupplier,
                parser);
    }
}
