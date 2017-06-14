#pragma once

#include "rpc_proxy_client_base.h"

#include <yt/ytlib/api/client.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyClient
    : public NApi::IClient
    , public TRpcProxyClientBase
{
public:
    TRpcProxyClient(
        TRpcProxyConnectionPtr connection,
        NRpc::IChannelPtr channel);
    ~TRpcProxyClient();

    virtual TFuture<void> Terminate() override
    {
        Y_UNIMPLEMENTED();
    }

    // Transactions
    virtual NApi::ITransactionPtr AttachTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        const NApi::TTransactionAttachOptions& options) override
    {
        Y_UNIMPLEMENTED();
    };

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

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const NApi::TAlterTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const NApi::TTrimTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AlterTableReplica(
        const NTabletClient::TTableReplicaId& replicaId,
        const NApi::TAlterTableReplicaOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    // Security
    virtual TFuture<void> AddMember(
        const TString& group,
        const TString& member,
        const NApi::TAddMemberOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const NApi::TRemoveMemberOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TCheckPermissionResult> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const NApi::TCheckPermissionOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const NApi::TStartOperationOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationId& operationId,
        const NApi::TAbortOperationOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationId& operationId,
        const NApi::TSuspendOperationOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationId& operationId,
        const NApi::TResumeOperationOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationId& operationId,
        const NApi::TCompleteOperationOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId& jobId,
        const NYPath::TYPath& path,
        const NApi::TDumpJobContextOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TGetJobInputOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<TSharedRef> GetJobStderr(
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TGetJobStderrOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<std::vector<NApi::TJob>> ListJobs(
        const NJobTrackerClient::TOperationId& operationId,
        const NApi::TListJobsOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TStraceJobOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId& jobId,
        const TString& signalName,
        const NApi::TSignalJobOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbandonJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TAbandonJobOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> PollJobShell(
        const NJobTrackerClient::TJobId& jobId,
        const NYson::TYsonString& parameters,
        const NApi::TPollJobShellOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> AbortJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TAbortJobOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    // Metadata
    virtual TFuture<NApi::TClusterMeta> GetClusterMeta(
        const NApi::TGetClusterMetaOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

private:
    const TRpcProxyConnectionPtr Connection_;
    const NRpc::IChannelPtr Channel_;

    virtual TRpcProxyConnectionPtr GetRpcProxyConnection() override;
    virtual NRpc::IChannelPtr GetChannel() override;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
