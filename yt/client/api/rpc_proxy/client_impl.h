#pragma once

#include "client_base.h"

#include <yt/client/api/client.h>

#include <yt/core/misc/lazy_ptr.h>

#include <yt/core/rpc/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public NApi::IClient
    , public TClientBase
{
public:
    TClient(
        TConnectionPtr connection,
        TDynamicChannelPoolPtr channelPool,
        const TClientOptions& options);

    virtual void Terminate() override;
    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    // Transactions
    virtual NApi::ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
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

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> ReshardTableAutomatic(
        const NYPath::TYPath& path,
        const NApi::TReshardTableAutomaticOptions& options) override;

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const NApi::TTrimTableOptions& options) override;

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const NApi::TAlterTableOptions& options) override;

    virtual TFuture<void> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const NApi::TAlterTableReplicaOptions& options) override;

    virtual TFuture<NYson::TYsonString> GetTablePivotKeys(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options) override;

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TGetInSyncReplicasOptions& options) override;

    virtual TFuture<std::vector<NApi::TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const NApi::TGetTabletsInfoOptions& options) override;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> BalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const NApi::TBalanceTabletCellsOptions& options) override;

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

    virtual TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const NApi::TCheckPermissionOptions& options) override;

    virtual TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const NApi::TCheckPermissionByAclOptions& options) override;

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
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const NApi::TDumpJobContextOptions& options) override;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        NJobTrackerClient::TJobId jobId,
        const NApi::TGetJobInputOptions& options) override;

    virtual TFuture<NYson::TYsonString> GetJobInputPaths(
        NJobTrackerClient::TJobId jobId,
        const NApi::TGetJobInputPathsOptions& options) override;

    virtual TFuture<TSharedRef> GetJobStderr(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const NApi::TGetJobStderrOptions& options) override;

    virtual TFuture<TSharedRef> GetJobFailContext(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const NApi::TGetJobFailContextOptions& options) override;

    virtual TFuture<NApi::TListOperationsResult> ListOperations(
        const NApi::TListOperationsOptions& options) override;

    virtual TFuture<NApi::TListJobsResult> ListJobs(
        NJobTrackerClient::TOperationId,
        const NApi::TListJobsOptions&) override;

    virtual TFuture<NYson::TYsonString> GetJob(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const NApi::TGetJobOptions& options) override;

    virtual TFuture<NYson::TYsonString> StraceJob(
        NJobTrackerClient::TJobId jobId,
        const NApi::TStraceJobOptions& options) override;

    virtual TFuture<void> SignalJob(
        NJobTrackerClient::TJobId jobId,
        const TString& signalName,
        const NApi::TSignalJobOptions& options) override;

    virtual TFuture<void> AbandonJob(
        NJobTrackerClient::TJobId job_id,
        const NApi::TAbandonJobOptions& options) override;

    virtual TFuture<NYson::TYsonString> PollJobShell(
        NJobTrackerClient::TJobId jobId,
        const NYson::TYsonString& parameters,
        const NApi::TPollJobShellOptions& options) override;

    virtual TFuture<void> AbortJob(
        NJobTrackerClient::TJobId jobId,
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

    virtual TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const NApi::TGetColumnarStatisticsOptions& options) override;

    virtual TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const NApi::TTruncateJournalOptions& options) override;

private:
    const TConnectionPtr Connection_;
    const TDynamicChannelPoolPtr ChannelPool_;
    const NRpc::IChannelPtr Channel_;
    const TClientOptions ClientOptions_;

    TLazyIntrusivePtr<NTabletClient::ITableMountCache> TableMountCache_;

    TLazyIntrusivePtr<NTransactionClient::ITimestampProvider> TimestampProvider_;
    NTransactionClient::ITimestampProviderPtr CreateTimestampProvider() const;

    virtual TConnectionPtr GetRpcProxyConnection() override;
    virtual TClientPtr GetRpcProxyClient() override;
    virtual NRpc::IChannelPtr GetChannel() override;
    virtual NRpc::IChannelPtr GetStickyChannel() override;
};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
