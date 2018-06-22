#pragma once

#include "client_base.h"

#include <yt/ytlib/api/client.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public NApi::IClient
    , public TClientBase
{
public:
    TClient(
        TConnectionPtr connection,
        NRpc::IChannelPtr channel);

    virtual TFuture<void> Terminate() override;
    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    // Transactions
    virtual NApi::ITransactionPtr AttachTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        const NApi::TTransactionAttachOptions& options) override;

    // Tables
    virtual TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const NApi::TMountTableOptions& options) override;

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const NApi::TUnmountTableOptions& options) override;

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const NApi::TRemountTableOptions& options) override;

    virtual TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const NApi::TFreezeTableOptions& options) override;

    virtual TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const NApi::TUnfreezeTableOptions& options) override;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const NApi::TReshardTableOptions& options) override;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const NApi::TReshardTableOptions& options) override;

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const NApi::TTrimTableOptions& options) override;

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const NApi::TAlterTableOptions& options) override;

    virtual TFuture<void> AlterTableReplica(
        const NTabletClient::TTableReplicaId& replicaId,
        const NApi::TAlterTableReplicaOptions& options) override;

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TGetInSyncReplicasOptions& options) override;

    virtual TFuture<std::vector<NApi::TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const NApi::TGetTabletsInfoOptions& options) override;

    // Files
    virtual TFuture<NApi::TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const NApi::TGetFileFromCacheOptions& options) override;

    virtual TFuture<NApi::TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const NApi::TPutFileToCacheOptions& options) override;

    // Security
    virtual TFuture<void> AddMember(
        const TString&,
        const TString&,
        const NApi::TAddMemberOptions&) override
    {
        ThrowUnimplemented("add_member");
    }

    virtual TFuture<void> RemoveMember(
        const TString&,
        const TString&,
        const NApi::TRemoveMemberOptions&) override
    {
        ThrowUnimplemented("remove_member");
    }

    virtual TFuture<NApi::TCheckPermissionResult> CheckPermission(
        const TString&,
        const NYPath::TYPath&,
        NYTree::EPermission,
        const NApi::TCheckPermissionOptions&) override
    {
        ThrowUnimplemented("check_permission");
    }

    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType,
        const NYson::TYsonString&,
        const NApi::TStartOperationOptions&) override
    {
        ThrowUnimplemented("start_operation");
    }

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationId&,
        const NApi::TAbortOperationOptions&) override
    {
        ThrowUnimplemented("abort_operation");
    }

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationId&,
        const NApi::TSuspendOperationOptions&) override
    {
        ThrowUnimplemented("suspend_operation");
    }

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationId&,
        const NApi::TResumeOperationOptions&) override
    {
        ThrowUnimplemented("resume_operation");
    }

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationId&,
        const NApi::TCompleteOperationOptions&) override
    {
        ThrowUnimplemented("complete_operation");
    }

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationId&,
        const NYson::TYsonString&,
        const NApi::TUpdateOperationParametersOptions&) override
    {
        ThrowUnimplemented("opdate_operation_parameters");
    }

    virtual TFuture<NYson::TYsonString> GetOperation(
        const NScheduler::TOperationId&,
        const NApi::TGetOperationOptions&) override
    {
        ThrowUnimplemented("get_operation");
    }

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId&,
        const NYPath::TYPath&,
        const NApi::TDumpJobContextOptions&) override
    {
        ThrowUnimplemented("dump_job_context");
    }

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobInputOptions&) override
    {
        ThrowUnimplemented("get_job_input");
    }

    virtual TFuture<TSharedRef> GetJobStderr(
        const NJobTrackerClient::TOperationId&,
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobStderrOptions&) override
    {
        ThrowUnimplemented("get_job_stderr");
    }

    virtual TFuture<TSharedRef> GetJobFailContext(
        const NJobTrackerClient::TOperationId&,
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobFailContextOptions&) override
    {
        ThrowUnimplemented("get_job_fail_context");
    }

    virtual TFuture<NApi::TListOperationsResult> ListOperations(
        const NApi::TListOperationsOptions&) override
    {
        ThrowUnimplemented("list_operations");
    }

    virtual TFuture<NApi::TListJobsResult> ListJobs(
        const NJobTrackerClient::TOperationId&,
        const NApi::TListJobsOptions&) override
    {
        ThrowUnimplemented("list_jobs");
    }

    virtual TFuture<NYson::TYsonString> GetJob(
        const NJobTrackerClient::TOperationId&,
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobOptions&) override
    {
        ThrowUnimplemented("get_job");
    }

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TStraceJobOptions&) override
    {
        ThrowUnimplemented("strace_job");
    }

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId&,
        const TString&,
        const NApi::TSignalJobOptions&) override
    {
        ThrowUnimplemented("signal_job");
    }

    virtual TFuture<void> AbandonJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TAbandonJobOptions&) override
    {
        ThrowUnimplemented("abandon_job");
    }

    virtual TFuture<NYson::TYsonString> PollJobShell(
        const NJobTrackerClient::TJobId&,
        const NYson::TYsonString&,
        const NApi::TPollJobShellOptions&) override
    {
        ThrowUnimplemented("poll_job_shell");
    }

    virtual TFuture<void> AbortJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TAbortJobOptions&) override
    {
        ThrowUnimplemented("abort_job");
    }

    // Metadata
    virtual TFuture<NApi::TClusterMeta> GetClusterMeta(
        const NApi::TGetClusterMetaOptions&) override
    {
        ThrowUnimplemented("get_cluster_meta");
    }

    virtual TFuture<NApi::TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath&,
        const NApi::TLocateSkynetShareOptions&) override
    {
        ThrowUnimplemented("locate_skynet_share");
    }

    TFuture<NTableClient::TColumnarStatistics> GetColumnarStatistics(
        const NYPath::TRichYPath&,
        const NApi::TGetColumnarStatisticsOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

private:
    const TConnectionPtr Connection_;
    const NRpc::IChannelPtr Channel_;
    const NTabletClient::ITableMountCachePtr TableMountCache_;

    TSpinLock TimestampProviderSpinLock_;
    std::atomic<bool> TimestampProviderInitialized_ = {false};
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;

    virtual TConnectionPtr GetRpcProxyConnection() override;
    virtual TClientPtr GetRpcProxyClient() override;
    virtual NRpc::IChannelPtr GetChannel() override;
};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
