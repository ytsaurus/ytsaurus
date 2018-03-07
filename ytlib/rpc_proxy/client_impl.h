#pragma once

#include "client_base.h"
#include "discovering_channel.h"

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
        const NApi::TClientOptions& options);

    virtual TFuture<void> Terminate() override;

    // Transactions
    virtual NApi::ITransactionPtr AttachTransaction(
        const NTransactionClient::TTransactionId&,
        const NApi::TTransactionAttachOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

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
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> RemoveMember(
        const TString&,
        const TString&,
        const NApi::TRemoveMemberOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TCheckPermissionResult> CheckPermission(
        const TString&,
        const NYPath::TYPath&,
        NYTree::EPermission,
        const NApi::TCheckPermissionOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType,
        const NYson::TYsonString&,
        const NApi::TStartOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationId&,
        const NApi::TAbortOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationId&,
        const NApi::TSuspendOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationId&,
        const NApi::TResumeOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationId&,
        const NApi::TCompleteOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationId&,
        const NApi::TUpdateOperationParametersOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> GetOperation(
        const NScheduler::TOperationId&,
        const NApi::TGetOperationOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId&,
        const NYPath::TYPath&,
        const NApi::TDumpJobContextOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobInputOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<TSharedRef> GetJobStderr(
        const NJobTrackerClient::TOperationId&,
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobStderrOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TListOperationsResult> ListOperations(
        const NApi::TListOperationsOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TListJobsResult> ListJobs(
        const NJobTrackerClient::TOperationId&,
        const NApi::TListJobsOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> GetJob(
        const NJobTrackerClient::TOperationId&,
        const NJobTrackerClient::TJobId&,
        const NApi::TGetJobOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TStraceJobOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId&,
        const TString&,
        const NApi::TSignalJobOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbandonJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TAbandonJobOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> PollJobShell(
        const NJobTrackerClient::TJobId&,
        const NYson::TYsonString&,
        const NApi::TPollJobShellOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbortJob(
        const NJobTrackerClient::TJobId&,
        const NApi::TAbortJobOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    // Metadata
    virtual TFuture<NApi::TClusterMeta> GetClusterMeta(
        const NApi::TGetClusterMetaOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath&,
        const NApi::TLocateSkynetShareOptions&) override
    {
        Y_UNIMPLEMENTED();
    }

private:
    const TConnectionPtr Connection_;
    const NRpc::IChannelPtr Channel_;

    virtual TConnectionPtr GetRpcProxyConnection() override;
    virtual TClientPtr GetRpcProxyClient() override;
    virtual NRpc::IChannelPtr GetChannel() override;
};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
