package tech.ytsaurus.client.sync;

import java.util.List;

import tech.ytsaurus.client.ApiServiceClient;
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

class SyncApiServiceClientImpl
        extends SyncTransactionalClientImpl
        implements SyncApiServiceClient {
    private final ApiServiceClient client;

    protected SyncApiServiceClientImpl(ApiServiceClient client) {
        super(client);
        this.client = client;
    }

    static SyncApiServiceClientImpl wrap(ApiServiceClient client) {
        return new SyncApiServiceClientImpl(client);
    }

    @Override
    public void abortJob(AbortJob req) {
        client.abortJob(req).join();
    }

    @Override
    public void abortOperation(AbortOperation req) {
        client.abortOperation(req).join();
    }

    @Override
    public SyncApiServiceTransaction startTransaction(StartTransaction req) {
        return SyncApiServiceTransaction.wrap(client.startTransaction(req).join());
    }

    @Override
    public void abortTransaction(AbortTransaction req) {
        client.abortTransaction(req).join();
    }

    @Override
    public void commitTransaction(CommitTransaction req) {
        client.commitTransaction(req).join();
    }

    @Override
    public void pingTransaction(PingTransaction req) {
        client.pingTransaction(req).join();
    }

    @Override
    public void alterTable(AlterTable req) {
        client.alterTable(req).join();
    }

    @Override
    public void alterTableReplica(AlterTableReplica req) {
        client.alterTableReplica(req).join();
    }

    @Override
    public Long buildSnapshot(BuildSnapshot req) {
        return client.buildSnapshot(req).join();
    }

    @Override
    public void checkClusterLiveness(CheckClusterLiveness req) {
        client.checkClusterLiveness(req).join();
    }

    @Override
    public GUID createObject(CreateObject req) {
        return client.createObject(req).join();
    }

    @Override
    public void freezeTable(FreezeTable req) {
        client.freezeTable(req).join();
    }

    @Override
    public void gcCollect(GcCollect req) {
        client.gcCollect(req).join();
    }

    @Override
    public YtTimestamp generateTimestamps(GenerateTimestamps req) {
        return client.generateTimestamps(req).join();
    }

    @Override
    public List<GUID> getInSyncReplicas(GetInSyncReplicas req, YtTimestamp timestamp) {
        return client.getInSyncReplicas(req, timestamp).join();
    }

    @Override
    public YTreeNode getJob(GetJob req) {
        return client.getJob(req).join();
    }

    @Override
    public GetJobStderrResult getJobStderr(GetJobStderr req) {
        return client.getJobStderr(req).join();
    }

    @Override
    public YTreeNode getOperation(GetOperation req) {
        return client.getOperation(req).join();
    }

    @Override
    public List<YTreeNode> getTablePivotKeys(GetTablePivotKeys req) {
        return client.getTablePivotKeys(req).join();
    }

    @Override
    public List<TabletInfo> getTabletInfos(GetTabletInfos req) {
        return client.getTabletInfos(req).join();
    }

    @Override
    public ListJobsResult listJobs(ListJobs req) {
        return client.listJobs(req).join();
    }

    @Override
    public <T> void lookupRows(
            AbstractLookupRowsRequest<?, ?> req,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        client.lookupRows(req, serializer, consumer).join();
    }

    @Override
    public void resumeOperation(ResumeOperation req) {
        client.resumeOperation(req).join();
    }

    @Override
    public void suspendOperation(SuspendOperation req) {
        client.suspendOperation(req).join();
    }

    @Override
    public void modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> req) {
        client.modifyRows(transactionId, req).join();
    }

    @Override
    public void mountTable(MountTable req) {
        client.mountTable(req).join();
    }

    @Override
    public void remountTable(RemountTable req) {
        client.remountTable(req).join();
    }

    @Override
    public void reshardTable(ReshardTable req) {
        client.reshardTable(req).join();
    }

    @Override
    public void trimTable(TrimTable req) {
        client.trimTable(req).join();
    }

    @Override
    public void unfreezeTable(UnfreezeTable req) {
        client.unfreezeTable(req).join();
    }

    @Override
    public void unmountTable(UnmountTable req) {
        client.unmountTable(req).join();
    }

    @Override
    public void updateOperationParameters(UpdateOperationParameters req) {
        client.updateOperationParameters(req).join();
    }
}
