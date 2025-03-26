package tech.ytsaurus.client;

import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import tech.ytsaurus.TReqDiscoverProxies;
import tech.ytsaurus.TRspDiscoverProxies;
import tech.ytsaurus.rpc.TReqDiscover;
import tech.ytsaurus.rpc.TRspDiscover;
import tech.ytsaurus.rpcproxy.TReqAbortJob;
import tech.ytsaurus.rpcproxy.TReqAbortOperation;
import tech.ytsaurus.rpcproxy.TReqAbortQuery;
import tech.ytsaurus.rpcproxy.TReqAbortTransaction;
import tech.ytsaurus.rpcproxy.TReqAdvanceQueueConsumer;
import tech.ytsaurus.rpcproxy.TReqAlterQuery;
import tech.ytsaurus.rpcproxy.TReqAlterTable;
import tech.ytsaurus.rpcproxy.TReqAlterTableReplica;
import tech.ytsaurus.rpcproxy.TReqBuildSnapshot;
import tech.ytsaurus.rpcproxy.TReqCheckClusterLiveness;
import tech.ytsaurus.rpcproxy.TReqCheckPermission;
import tech.ytsaurus.rpcproxy.TReqCommitTransaction;
import tech.ytsaurus.rpcproxy.TReqCompleteOperation;
import tech.ytsaurus.rpcproxy.TReqConcatenateNodes;
import tech.ytsaurus.rpcproxy.TReqCopyNode;
import tech.ytsaurus.rpcproxy.TReqCreateNode;
import tech.ytsaurus.rpcproxy.TReqCreateObject;
import tech.ytsaurus.rpcproxy.TReqExistsNode;
import tech.ytsaurus.rpcproxy.TReqFreezeTable;
import tech.ytsaurus.rpcproxy.TReqGCCollect;
import tech.ytsaurus.rpcproxy.TReqGenerateTimestamps;
import tech.ytsaurus.rpcproxy.TReqGetFileFromCache;
import tech.ytsaurus.rpcproxy.TReqGetInSyncReplicas;
import tech.ytsaurus.rpcproxy.TReqGetJob;
import tech.ytsaurus.rpcproxy.TReqGetJobStderr;
import tech.ytsaurus.rpcproxy.TReqGetNode;
import tech.ytsaurus.rpcproxy.TReqGetOperation;
import tech.ytsaurus.rpcproxy.TReqGetQuery;
import tech.ytsaurus.rpcproxy.TReqGetQueryResult;
import tech.ytsaurus.rpcproxy.TReqGetTablePivotKeys;
import tech.ytsaurus.rpcproxy.TReqGetTabletInfos;
import tech.ytsaurus.rpcproxy.TReqLinkNode;
import tech.ytsaurus.rpcproxy.TReqListJobs;
import tech.ytsaurus.rpcproxy.TReqListNode;
import tech.ytsaurus.rpcproxy.TReqListQueries;
import tech.ytsaurus.rpcproxy.TReqListQueueConsumerRegistrations;
import tech.ytsaurus.rpcproxy.TReqLockNode;
import tech.ytsaurus.rpcproxy.TReqLookupRows;
import tech.ytsaurus.rpcproxy.TReqModifyRows;
import tech.ytsaurus.rpcproxy.TReqMountTable;
import tech.ytsaurus.rpcproxy.TReqMoveNode;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;
import tech.ytsaurus.rpcproxy.TReqPartitionTables;
import tech.ytsaurus.rpcproxy.TReqPingTransaction;
import tech.ytsaurus.rpcproxy.TReqPullQueueConsumer;
import tech.ytsaurus.rpcproxy.TReqPutFileToCache;
import tech.ytsaurus.rpcproxy.TReqReadFile;
import tech.ytsaurus.rpcproxy.TReqReadQueryResult;
import tech.ytsaurus.rpcproxy.TReqReadShuffleData;
import tech.ytsaurus.rpcproxy.TReqReadTable;
import tech.ytsaurus.rpcproxy.TReqRegisterQueueConsumer;
import tech.ytsaurus.rpcproxy.TReqRemountTable;
import tech.ytsaurus.rpcproxy.TReqRemoveNode;
import tech.ytsaurus.rpcproxy.TReqReshardTable;
import tech.ytsaurus.rpcproxy.TReqResumeOperation;
import tech.ytsaurus.rpcproxy.TReqSelectRows;
import tech.ytsaurus.rpcproxy.TReqSetNode;
import tech.ytsaurus.rpcproxy.TReqStartOperation;
import tech.ytsaurus.rpcproxy.TReqStartQuery;
import tech.ytsaurus.rpcproxy.TReqStartShuffle;
import tech.ytsaurus.rpcproxy.TReqStartTransaction;
import tech.ytsaurus.rpcproxy.TReqSuspendOperation;
import tech.ytsaurus.rpcproxy.TReqTrimTable;
import tech.ytsaurus.rpcproxy.TReqUnfreezeTable;
import tech.ytsaurus.rpcproxy.TReqUnmountTable;
import tech.ytsaurus.rpcproxy.TReqUpdateOperationParameters;
import tech.ytsaurus.rpcproxy.TReqVersionedLookupRows;
import tech.ytsaurus.rpcproxy.TReqWriteFile;
import tech.ytsaurus.rpcproxy.TReqWriteShuffleData;
import tech.ytsaurus.rpcproxy.TReqWriteTable;
import tech.ytsaurus.rpcproxy.TRspAbortJob;
import tech.ytsaurus.rpcproxy.TRspAbortOperation;
import tech.ytsaurus.rpcproxy.TRspAbortQuery;
import tech.ytsaurus.rpcproxy.TRspAbortTransaction;
import tech.ytsaurus.rpcproxy.TRspAdvanceQueueConsumer;
import tech.ytsaurus.rpcproxy.TRspAlterQuery;
import tech.ytsaurus.rpcproxy.TRspAlterTable;
import tech.ytsaurus.rpcproxy.TRspAlterTableReplica;
import tech.ytsaurus.rpcproxy.TRspBuildSnapshot;
import tech.ytsaurus.rpcproxy.TRspCheckClusterLiveness;
import tech.ytsaurus.rpcproxy.TRspCheckPermission;
import tech.ytsaurus.rpcproxy.TRspCommitTransaction;
import tech.ytsaurus.rpcproxy.TRspCompleteOperation;
import tech.ytsaurus.rpcproxy.TRspConcatenateNodes;
import tech.ytsaurus.rpcproxy.TRspCopyNode;
import tech.ytsaurus.rpcproxy.TRspCreateNode;
import tech.ytsaurus.rpcproxy.TRspCreateObject;
import tech.ytsaurus.rpcproxy.TRspExistsNode;
import tech.ytsaurus.rpcproxy.TRspFreezeTable;
import tech.ytsaurus.rpcproxy.TRspGCCollect;
import tech.ytsaurus.rpcproxy.TRspGenerateTimestamps;
import tech.ytsaurus.rpcproxy.TRspGetFileFromCache;
import tech.ytsaurus.rpcproxy.TRspGetInSyncReplicas;
import tech.ytsaurus.rpcproxy.TRspGetJob;
import tech.ytsaurus.rpcproxy.TRspGetJobStderr;
import tech.ytsaurus.rpcproxy.TRspGetNode;
import tech.ytsaurus.rpcproxy.TRspGetOperation;
import tech.ytsaurus.rpcproxy.TRspGetQuery;
import tech.ytsaurus.rpcproxy.TRspGetQueryResult;
import tech.ytsaurus.rpcproxy.TRspGetTablePivotKeys;
import tech.ytsaurus.rpcproxy.TRspGetTabletInfos;
import tech.ytsaurus.rpcproxy.TRspLinkNode;
import tech.ytsaurus.rpcproxy.TRspListJobs;
import tech.ytsaurus.rpcproxy.TRspListNode;
import tech.ytsaurus.rpcproxy.TRspListQueries;
import tech.ytsaurus.rpcproxy.TRspListQueueConsumerRegistrations;
import tech.ytsaurus.rpcproxy.TRspLockNode;
import tech.ytsaurus.rpcproxy.TRspLookupRows;
import tech.ytsaurus.rpcproxy.TRspModifyRows;
import tech.ytsaurus.rpcproxy.TRspMountTable;
import tech.ytsaurus.rpcproxy.TRspMoveNode;
import tech.ytsaurus.rpcproxy.TRspMultiLookup;
import tech.ytsaurus.rpcproxy.TRspPartitionTables;
import tech.ytsaurus.rpcproxy.TRspPingTransaction;
import tech.ytsaurus.rpcproxy.TRspPullQueueConsumer;
import tech.ytsaurus.rpcproxy.TRspPutFileToCache;
import tech.ytsaurus.rpcproxy.TRspReadFile;
import tech.ytsaurus.rpcproxy.TRspReadQueryResult;
import tech.ytsaurus.rpcproxy.TRspReadShuffleData;
import tech.ytsaurus.rpcproxy.TRspReadTable;
import tech.ytsaurus.rpcproxy.TRspRegisterQueueConsumer;
import tech.ytsaurus.rpcproxy.TRspRemountTable;
import tech.ytsaurus.rpcproxy.TRspRemoveNode;
import tech.ytsaurus.rpcproxy.TRspReshardTable;
import tech.ytsaurus.rpcproxy.TRspResumeOperation;
import tech.ytsaurus.rpcproxy.TRspSelectRows;
import tech.ytsaurus.rpcproxy.TRspSetNode;
import tech.ytsaurus.rpcproxy.TRspStartOperation;
import tech.ytsaurus.rpcproxy.TRspStartQuery;
import tech.ytsaurus.rpcproxy.TRspStartShuffle;
import tech.ytsaurus.rpcproxy.TRspStartTransaction;
import tech.ytsaurus.rpcproxy.TRspSuspendOperation;
import tech.ytsaurus.rpcproxy.TRspTrimTable;
import tech.ytsaurus.rpcproxy.TRspUnfreezeTable;
import tech.ytsaurus.rpcproxy.TRspUnmountTable;
import tech.ytsaurus.rpcproxy.TRspUpdateOperationParameters;
import tech.ytsaurus.rpcproxy.TRspVersionedLookupRows;
import tech.ytsaurus.rpcproxy.TRspWriteFile;
import tech.ytsaurus.rpcproxy.TRspWriteShuffleData;
import tech.ytsaurus.rpcproxy.TRspWriteTable;

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

    public static final RpcMethodDescriptor<TReqMultiLookup.Builder, TRspMultiLookup> MULTI_LOOKUP =
            apiServiceMethod("MultiLookup", TReqMultiLookup::newBuilder, TRspMultiLookup.parser());

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

    public static final RpcMethodDescriptor<TReqPartitionTables.Builder, TRspPartitionTables> PARTITION_TABLES =
            apiServiceMethod("PartitionTables", TReqPartitionTables::newBuilder, TRspPartitionTables.parser());

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

    public static final RpcMethodDescriptor<TReqAbortOperation.Builder, TRspAbortOperation> ABORT_OPERATION =
            apiServiceMethod("AbortOperation", TReqAbortOperation::newBuilder, TRspAbortOperation.parser());

    public static final RpcMethodDescriptor<TReqCompleteOperation.Builder, TRspCompleteOperation> COMPLETE_OPERATION =
            apiServiceMethod("CompleteOperation", TReqCompleteOperation::newBuilder, TRspCompleteOperation.parser());

    public static final RpcMethodDescriptor<TReqSuspendOperation.Builder, TRspSuspendOperation> SUSPEND_OPERATION =
            apiServiceMethod("SuspendOperation", TReqSuspendOperation::newBuilder, TRspSuspendOperation.parser());

    public static final RpcMethodDescriptor<TReqResumeOperation.Builder, TRspResumeOperation> RESUME_OPERATION =
            apiServiceMethod("ResumeOperation", TReqResumeOperation::newBuilder, TRspResumeOperation.parser());

    public static final RpcMethodDescriptor<TReqUpdateOperationParameters.Builder, TRspUpdateOperationParameters>
            UPDATE_OPERATION_PARAMETERS = apiServiceMethod("UpdateOperationParameters",
            TReqUpdateOperationParameters::newBuilder, TRspUpdateOperationParameters.parser());

    public static final RpcMethodDescriptor<TReqGetJob.Builder, TRspGetJob> GET_JOB =
            apiServiceMethod("GetJob", TReqGetJob::newBuilder, TRspGetJob.parser());

    public static final RpcMethodDescriptor<TReqListJobs.Builder, TRspListJobs> LIST_JOBS =
            apiServiceMethod("ListJobs", TReqListJobs::newBuilder, TRspListJobs.parser());

    public static final RpcMethodDescriptor<TReqGetJobStderr.Builder, TRspGetJobStderr> GET_JOB_STDERR =
            apiServiceMethod("GetJobStderr", TReqGetJobStderr::newBuilder, TRspGetJobStderr.parser());

    public static final RpcMethodDescriptor<TReqAbortJob.Builder, TRspAbortJob> ABORT_JOB =
            apiServiceMethod("AbortJob", TReqAbortJob::newBuilder, TRspAbortJob.parser());

    public static final RpcMethodDescriptor<TReqCheckPermission.Builder, TRspCheckPermission> CHECK_PERMISSION =
            apiServiceMethod("CheckPermission", TReqCheckPermission::newBuilder, TRspCheckPermission.parser());

    public static final RpcMethodDescriptor<TReqCheckClusterLiveness.Builder, TRspCheckClusterLiveness>
            CHECK_CLUSTER_LIVENESS = apiServiceMethod(
            "CheckClusterLiveness", TReqCheckClusterLiveness::newBuilder, TRspCheckClusterLiveness.parser());

    public static final RpcMethodDescriptor<TReqPullQueueConsumer.Builder, TRspPullQueueConsumer> PULL_CONSUMER =
            apiServiceMethod("PullConsumer", TReqPullQueueConsumer::newBuilder, TRspPullQueueConsumer.parser());

    public static final RpcMethodDescriptor<TReqAdvanceQueueConsumer.Builder, TRspAdvanceQueueConsumer>
            ADVANCE_CONSUMER = apiServiceMethod(
            "AdvanceConsumer", TReqAdvanceQueueConsumer::newBuilder, TRspAdvanceQueueConsumer.parser());

    public static final RpcMethodDescriptor<TReqRegisterQueueConsumer.Builder, TRspRegisterQueueConsumer>
            REGISTER_QUEUE_CONSUMER = apiServiceMethod(
            "RegisterQueueConsumer",
            TReqRegisterQueueConsumer::newBuilder, TRspRegisterQueueConsumer.parser()
    );

    public static final RpcMethodDescriptor<
            TReqListQueueConsumerRegistrations.Builder, TRspListQueueConsumerRegistrations>
            LIST_QUEUE_CONSUMER_REGISTRATIONS = apiServiceMethod(
            "ListQueueConsumerRegistrations",
            TReqListQueueConsumerRegistrations::newBuilder, TRspListQueueConsumerRegistrations.parser()
    );

    public static final RpcMethodDescriptor<TReqStartQuery.Builder, TRspStartQuery> START_QUERY =
            apiServiceMethod("StartQuery", TReqStartQuery::newBuilder, TRspStartQuery.parser());

    public static final RpcMethodDescriptor<TReqAbortQuery.Builder, TRspAbortQuery> ABORT_QUERY =
            apiServiceMethod("AbortQuery", TReqAbortQuery::newBuilder, TRspAbortQuery.parser());

    public static final RpcMethodDescriptor<TReqGetQueryResult.Builder, TRspGetQueryResult> GET_QUERY_RESULT =
            apiServiceMethod("GetQueryResult", TReqGetQueryResult::newBuilder, TRspGetQueryResult.parser());

    public static final RpcMethodDescriptor<TReqReadQueryResult.Builder, TRspReadQueryResult> READ_QUERY_RESULT =
            apiServiceMethod("ReadQueryResult", TReqReadQueryResult::newBuilder, TRspReadQueryResult.parser());

    public static final RpcMethodDescriptor<TReqGetQuery.Builder, TRspGetQuery> GET_QUERY =
            apiServiceMethod("GetQuery", TReqGetQuery::newBuilder, TRspGetQuery.parser());

    public static final RpcMethodDescriptor<TReqListQueries.Builder, TRspListQueries> LIST_QUERIES =
            apiServiceMethod("ListQueries", TReqListQueries::newBuilder, TRspListQueries.parser());

    public static final RpcMethodDescriptor<TReqAlterQuery.Builder, TRspAlterQuery> ALTER_QUERY =
            apiServiceMethod("AlterQuery", TReqAlterQuery::newBuilder, TRspAlterQuery.parser());

    public static final RpcMethodDescriptor<TReqReadTable.Builder, TRspReadTable> READ_TABLE =
            apiServiceMethod("ReadTable", TReqReadTable::newBuilder, TRspReadTable.parser());

    public static final RpcMethodDescriptor<TReqWriteTable.Builder, TRspWriteTable> WRITE_TABLE =
            apiServiceMethod("WriteTable", TReqWriteTable::newBuilder, TRspWriteTable.parser());

    public static final RpcMethodDescriptor<TReqReadFile.Builder, TRspReadFile> READ_FILE =
            apiServiceMethod("ReadFile", TReqReadFile::newBuilder, TRspReadFile.parser());

    public static final RpcMethodDescriptor<TReqWriteFile.Builder, TRspWriteFile> WRITE_FILE =
            apiServiceMethod("WriteFile", TReqWriteFile::newBuilder, TRspWriteFile.parser());

    public static final RpcMethodDescriptor<TReqStartShuffle.Builder, TRspStartShuffle> START_SHUFFLE =
            apiServiceMethod("StartShuffle", TReqStartShuffle::newBuilder, TRspStartShuffle.parser());

    public static final RpcMethodDescriptor<TReqWriteShuffleData.Builder, TRspWriteShuffleData> WRITE_SHUFFLE_DATA =
            apiServiceMethod("WriteShuffleData", TReqWriteShuffleData::newBuilder, TRspWriteShuffleData.parser());

    public static final RpcMethodDescriptor<TReqReadShuffleData.Builder, TRspReadShuffleData> READ_SHUFFLE_DATA =
            apiServiceMethod("ReadShuffleData", TReqReadShuffleData::newBuilder, TRspReadShuffleData.parser());

    public static final RpcMethodDescriptor<TReqGetFileFromCache.Builder, TRspGetFileFromCache> GET_FILE_FROM_CACHE =
            apiServiceMethod("GetFileFromCache", TReqGetFileFromCache::newBuilder, TRspGetFileFromCache.parser());

    public static final RpcMethodDescriptor<TReqPutFileToCache.Builder, TRspPutFileToCache> PUT_FILE_TO_CACHE =
            apiServiceMethod("PutFileToCache", TReqPutFileToCache::newBuilder, TRspPutFileToCache.parser());

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
