#pragma once

#include "client_base.h"

#include <yt/client/api/client.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NApi {
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
        const TString& group,
        const TString& member,
        const NApi::TAddMemberOptions& options) override;

    virtual TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const NApi::TRemoveMemberOptions& options) override;

    virtual TFuture<NApi::TCheckPermissionResult> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const NApi::TCheckPermissionOptions& options);

    virtual TFuture<NApi::TCheckPermissionByAclResult> CheckPermissionByAcl(
        const TNullable<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const NApi::TCheckPermissionByAclOptions& options) override
    {
        ThrowUnimplemented("check_permission_by_acl");
    }

    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const NApi::TStartOperationOptions& options) override;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NApi::TAbortOperationOptions& options) override;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NApi::TSuspendOperationOptions& options) override;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NApi::TResumeOperationOptions& options) override;

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NApi::TCompleteOperationOptions& options) override;

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const NApi::TUpdateOperationParametersOptions& options) override;

    virtual TFuture<NYson::TYsonString> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NApi::TGetOperationOptions& options) override;

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId& jobId,
        const NYPath::TYPath& path,
        const NApi::TDumpJobContextOptions& options) override;

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
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TGetJobOptions& options) override;

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TStraceJobOptions& options) override;

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId& jobId,
        const TString& signalName,
        const NApi::TSignalJobOptions& options) override;

    virtual TFuture<void> AbandonJob(
        const NJobTrackerClient::TJobId& job_id,
        const NApi::TAbandonJobOptions& options) override;

    virtual TFuture<NYson::TYsonString> PollJobShell(
        const NJobTrackerClient::TJobId& jobId,
        const NYson::TYsonString& parameters,
        const NApi::TPollJobShellOptions& options) override;

    virtual TFuture<void> AbortJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TAbortJobOptions& options) override;

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

    TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>&,
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
} // namespace NApi
} // namespace NYT
