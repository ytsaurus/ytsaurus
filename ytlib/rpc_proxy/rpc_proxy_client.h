#pragma once

#include "public.h"
#include "rpc_proxy_connection.h"

#include <yt/ytlib/api/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyClient
    : public NApi::IClient
{
public:
    TRpcProxyClient(
        TRpcProxyConnectionPtr connection,
        const NApi::TClientOptions& options);
    ~TRpcProxyClient();

    // TODO(sandello): Implement me!

    virtual NApi::IConnectionPtr GetConnection() override
    {
        return Connection_;
    };


    // Transactions
    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }


    // Tables
    virtual TFuture<NApi::IRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const NApi::TLookupRowsOptions& options) override;


    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const Stroka& query,
        const NApi::TSelectRowsOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    // TODO(babenko): batch read and batch write

    // Cypress
    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const NApi::TGetNodeOptions& options) override;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const NApi::TSetNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const NApi::TRemoveNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const NApi::TListNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const NApi::TCreateNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const NApi::TLockNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TCopyNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TMoveNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const NApi::TLinkNodeOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TYPath>& srcPaths,
        const NYPath::TYPath& dstPath,
        const NApi::TConcatenateNodesOptions& options) override
    {
        Y_UNIMPLEMENTED();
    };

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const NApi::TNodeExistsOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }


    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const NApi::TCreateObjectOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }


    // Files
    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const NApi::TFileReaderOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual NApi::IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath& path,
        const NApi::TFileWriterOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }


    // Journals
    virtual NApi::IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const NApi::TJournalReaderOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual NApi::IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const NApi::TJournalWriterOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }


    // Tables
    virtual TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const NApi::TTableReaderOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    //! Returns a async flag indicating completion.
    virtual TFuture<void> Terminate() override
    {
        Y_UNIMPLEMENTED();
    };

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
        const NApi::TMountTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    };

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const NApi::TUnmountTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const NApi::TRemountTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const NApi::TFreezeTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const NApi::TUnfreezeTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const NApi::TReshardTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const NApi::TReshardTableOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

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

    virtual TFuture<void> EnableTableReplica(
        const NTabletClient::TTableReplicaId& replicaId,
        const NApi::TEnableTableReplicaOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> DisableTableReplica(
        const NTabletClient::TTableReplicaId& replicaId,
        const NApi::TDisableTableReplicaOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    // Security
    virtual TFuture<void> AddMember(
        const Stroka& group,
        const Stroka& member,
        const NApi::TAddMemberOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> RemoveMember(
        const Stroka& group,
        const Stroka& member,
        const NApi::TRemoveMemberOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<NApi::TCheckPermissionResult> CheckPermission(
        const Stroka& user,
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

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId& jobId,
        const NApi::TStraceJobOptions& options) override
    {
        Y_UNIMPLEMENTED();
    }

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId& jobId,
        const Stroka& signalName,
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

    NRpc::IChannelPtr Channel_;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
