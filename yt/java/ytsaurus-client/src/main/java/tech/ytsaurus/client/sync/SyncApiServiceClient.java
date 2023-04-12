package tech.ytsaurus.client.sync;

import java.util.List;

import tech.ytsaurus.client.request.AbortJob;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.AbortTransaction;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.AlterTableReplica;
import tech.ytsaurus.client.request.BuildSnapshot;
import tech.ytsaurus.client.request.CheckClusterLiveness;
import tech.ytsaurus.client.request.CommitTransaction;
import tech.ytsaurus.client.request.CreateObject;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.GcCollect;
import tech.ytsaurus.client.request.GenerateTimestamps;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.GetJob;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetJobStderrResult;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.GetTablePivotKeys;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.client.request.ListJobsResult;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PingTransaction;
import tech.ytsaurus.client.request.RemountTable;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.ResumeOperation;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TrimTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.ysontree.YTreeNode;

interface SyncApiServiceClient extends SyncTransactionalClient {
    void abortJob(AbortJob req);

    void abortOperation(AbortOperation req);

    SyncApiServiceTransaction startTransaction(StartTransaction req);

    void abortTransaction(AbortTransaction req);

    void commitTransaction(CommitTransaction req);

    void pingTransaction(PingTransaction req);

    void alterTable(AlterTable req);

    void alterTableReplica(AlterTableReplica req);

    Long buildSnapshot(BuildSnapshot req);

    void checkClusterLiveness(CheckClusterLiveness req);

    GUID createObject(CreateObject req);

    void freezeTable(FreezeTable req);

    void gcCollect(GcCollect req);

    YtTimestamp generateTimestamps(GenerateTimestamps req);

    List<GUID> getInSyncReplicas(GetInSyncReplicas req, YtTimestamp timestamp);

    YTreeNode getJob(GetJob req);

    GetJobStderrResult getJobStderr(GetJobStderr req);

    YTreeNode getOperation(GetOperation req);

    List<YTreeNode> getTablePivotKeys(GetTablePivotKeys req);

    List<TabletInfo> getTabletInfos(GetTabletInfos req);

    ListJobsResult listJobs(ListJobs req);

    <T> void lookupRows(
            AbstractLookupRowsRequest<?, ?> req,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    void resumeOperation(ResumeOperation req);

    void suspendOperation(SuspendOperation req);

    void modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> req);

    void remountTable(RemountTable req);

    void reshardTable(ReshardTable req);

    void trimTable(TrimTable req);

    void unfreezeTable(UnfreezeTable req);

    /**
     * Mount table.
     * <p>
     * This method doesn't wait until tablets become mounted.
     *
     * @see MountTable
     * @see SyncCompoundClient#mountTableAndWaitTablets(MountTable)
     */
    void mountTable(MountTable req);

    /**
     * Unmount table.
     * <p>
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see SyncCompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    void unmountTable(UnmountTable req);

    void updateOperationParameters(UpdateOperationParameters req);
}
