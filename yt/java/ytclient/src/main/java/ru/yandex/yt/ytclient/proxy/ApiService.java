package ru.yandex.yt.ytclient.proxy;

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
import ru.yandex.yt.rpcproxy.TReqRemoveMember;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TReqReshardTable;
import ru.yandex.yt.rpcproxy.TReqReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TReqResumeOperation;
import ru.yandex.yt.rpcproxy.TReqSelectRows;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TReqSignalJob;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TReqStraceJob;
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
import ru.yandex.yt.rpcproxy.TRspRemoveMember;
import ru.yandex.yt.rpcproxy.TRspRemoveNode;
import ru.yandex.yt.rpcproxy.TRspReshardTable;
import ru.yandex.yt.rpcproxy.TRspReshardTableAutomatic;
import ru.yandex.yt.rpcproxy.TRspResumeOperation;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspSetNode;
import ru.yandex.yt.rpcproxy.TRspSignalJob;
import ru.yandex.yt.rpcproxy.TRspStartOperation;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.rpcproxy.TRspStraceJob;
import ru.yandex.yt.rpcproxy.TRspSuspendOperation;
import ru.yandex.yt.rpcproxy.TRspTrimTable;
import ru.yandex.yt.rpcproxy.TRspUnfreezeTable;
import ru.yandex.yt.rpcproxy.TRspUnmountTable;
import ru.yandex.yt.rpcproxy.TRspUpdateOperationParameters;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRspWriteFile;
import ru.yandex.yt.rpcproxy.TRspWriteTable;
import ru.yandex.yt.ytclient.rpc.DiscoverableRpcService;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.annotations.RpcService;

/**
 * Сервис для работы с API через RPC
 */
@RpcService(protocolVersion = 1)
public interface ApiService extends DiscoverableRpcService {
    RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> startTransaction();

    RpcClientRequestBuilder<TReqPingTransaction.Builder, RpcClientResponse<TRspPingTransaction>> pingTransaction();

    RpcClientRequestBuilder<TReqCommitTransaction.Builder, RpcClientResponse<TRspCommitTransaction>> commitTransaction();

    RpcClientRequestBuilder<TReqAbortTransaction.Builder, RpcClientResponse<TRspAbortTransaction>> abortTransaction();

    RpcClientRequestBuilder<TReqLookupRows.Builder, RpcClientResponse<TRspLookupRows>> lookupRows();

    RpcClientRequestBuilder<TReqVersionedLookupRows.Builder, RpcClientResponse<TRspVersionedLookupRows>> versionedLookupRows();

    RpcClientRequestBuilder<TReqSelectRows.Builder, RpcClientResponse<TRspSelectRows>> selectRows();

    RpcClientRequestBuilder<TReqModifyRows.Builder, RpcClientResponse<TRspModifyRows>> modifyRows();

    RpcClientRequestBuilder<TReqBuildSnapshot.Builder, RpcClientResponse<TRspBuildSnapshot>> buildSnapshot();

    RpcClientRequestBuilder<TReqGCCollect.Builder, RpcClientResponse<TRspGCCollect>> gcCollect();

    RpcClientRequestBuilder<TReqGetInSyncReplicas.Builder, RpcClientResponse<TRspGetInSyncReplicas>> getInSyncReplicas();

    RpcClientRequestBuilder<TReqGetTabletInfos.Builder, RpcClientResponse<TRspGetTabletInfos>> getTabletInfos();

    RpcClientRequestBuilder<TReqGenerateTimestamps.Builder, RpcClientResponse<TRspGenerateTimestamps>> generateTimestamps();

    /* objects */
    RpcClientRequestBuilder<TReqCreateObject.Builder, RpcClientResponse<TRspCreateObject>> createObject();

    /* nodes */
    RpcClientRequestBuilder<TReqGetNode.Builder, RpcClientResponse<TRspGetNode>> getNode();

    RpcClientRequestBuilder<TReqSetNode.Builder, RpcClientResponse<TRspSetNode>> setNode();

    RpcClientRequestBuilder<TReqExistsNode.Builder, RpcClientResponse<TRspExistsNode>> existsNode();

    RpcClientRequestBuilder<TReqListNode.Builder, RpcClientResponse<TRspListNode>> listNode();

    RpcClientRequestBuilder<TReqCreateNode.Builder, RpcClientResponse<TRspCreateNode>> createNode();

    RpcClientRequestBuilder<TReqGetTableMountInfo.Builder, RpcClientResponse<TRspGetTableMountInfo>> getTableMountInfo();

    RpcClientRequestBuilder<TReqRemoveNode.Builder, RpcClientResponse<TRspRemoveNode>> removeNode();

    RpcClientRequestBuilder<TReqLockNode.Builder, RpcClientResponse<TRspLockNode>> lockNode();

    RpcClientRequestBuilder<TReqCopyNode.Builder, RpcClientResponse<TRspCopyNode>> copyNode();

    RpcClientRequestBuilder<TReqMoveNode.Builder, RpcClientResponse<TRspMoveNode>> moveNode();

    RpcClientRequestBuilder<TReqLinkNode.Builder, RpcClientResponse<TRspLinkNode>> linkNode();

    RpcClientRequestBuilder<TReqConcatenateNodes.Builder, RpcClientResponse<TRspConcatenateNodes>> concatenateNodes();

    /* tables */
    RpcClientRequestBuilder<TReqMountTable.Builder, RpcClientResponse<TRspMountTable>> mountTable();

    RpcClientRequestBuilder<TReqUnmountTable.Builder, RpcClientResponse<TRspUnmountTable>> unmountTable();

    RpcClientRequestBuilder<TReqRemountTable.Builder, RpcClientResponse<TRspRemountTable>> remountTable();

    RpcClientRequestBuilder<TReqFreezeTable.Builder, RpcClientResponse<TRspFreezeTable>> freezeTable();

    RpcClientRequestBuilder<TReqUnfreezeTable.Builder, RpcClientResponse<TRspUnfreezeTable>> unfreezeTable();

    RpcClientRequestBuilder<TReqReshardTable.Builder, RpcClientResponse<TRspReshardTable>> reshardTable();

    RpcClientRequestBuilder<TReqReshardTableAutomatic.Builder, RpcClientResponse<TRspReshardTableAutomatic>> reshardTableAutomatic();

    RpcClientRequestBuilder<TReqTrimTable.Builder, RpcClientResponse<TRspTrimTable>> trimTable();

    RpcClientRequestBuilder<TReqAlterTable.Builder, RpcClientResponse<TRspAlterTable>> alterTable();

    RpcClientRequestBuilder<TReqAlterTableReplica.Builder, RpcClientResponse<TRspAlterTableReplica>> alterTableReplica();

    RpcClientRequestBuilder<TReqGetFileFromCache.Builder, RpcClientResponse<TRspGetFileFromCache>> getFileFromCache();

    RpcClientRequestBuilder<TReqPutFileToCache.Builder, RpcClientResponse<TRspPutFileToCache>> putFileToCache();
    /* */

    /* operations */
    RpcClientRequestBuilder<TReqStartOperation.Builder, RpcClientResponse<TRspStartOperation>> startOperation();

    RpcClientRequestBuilder<TReqAbortOperation.Builder, RpcClientResponse<TRspAbortOperation>> abortOperation();

    RpcClientRequestBuilder<TReqSuspendOperation.Builder, RpcClientResponse<TRspSuspendOperation>> suspendOperation();

    RpcClientRequestBuilder<TReqResumeOperation.Builder, RpcClientResponse<TRspResumeOperation>> resumeOperation();

    RpcClientRequestBuilder<TReqCompleteOperation.Builder, RpcClientResponse<TRspCompleteOperation>> completeOperation();

    RpcClientRequestBuilder<TReqUpdateOperationParameters.Builder, RpcClientResponse<TRspUpdateOperationParameters>>
        updateOperationParameters();

    RpcClientRequestBuilder<TReqGetOperation.Builder, RpcClientResponse<TRspGetOperation>> getOperation();
    /* */
    /* Jobs */
    RpcClientRequestBuilder<TReqGetJob.Builder, RpcClientResponse<TRspGetJob>> getJob();

    RpcClientRequestBuilder<TReqStraceJob.Builder, RpcClientResponse<TRspStraceJob>> straceJob();

    RpcClientRequestBuilder<TReqDumpJobContext.Builder, RpcClientResponse<TRspDumpJobContext>> dumpJobContext();

    RpcClientRequestBuilder<TReqSignalJob.Builder, RpcClientResponse<TRspSignalJob>> signalJob();

    RpcClientRequestBuilder<TReqAbandonJob.Builder, RpcClientResponse<TRspAbandonJob>> abandonJob();

    RpcClientRequestBuilder<TReqPollJobShell.Builder, RpcClientResponse<TRspPollJobShell>> pollJobShell();

    RpcClientRequestBuilder<TReqAbortJob.Builder, RpcClientResponse<TRspAbortJob>> abortJob();
    /* */
    RpcClientRequestBuilder<TReqAddMember.Builder, RpcClientResponse<TRspAddMember>> addMember();

    RpcClientRequestBuilder<TReqRemoveMember.Builder, RpcClientResponse<TRspRemoveMember>> removeMember();

    RpcClientRequestBuilder<TReqCheckPermission.Builder, RpcClientResponse<TRspCheckPermission>> checkPermission();
    /* */

    RpcClientRequestBuilder<TReqReadTable.Builder, RpcClientResponse<TRspReadTable>> readTable();

    RpcClientRequestBuilder<TReqWriteTable.Builder, RpcClientResponse<TRspWriteTable>> writeTable();

    RpcClientRequestBuilder<TReqReadFile.Builder, RpcClientResponse<TRspReadFile>> readFile();

    RpcClientRequestBuilder<TReqWriteFile.Builder, RpcClientResponse<TRspWriteFile>> writeFile();
}
