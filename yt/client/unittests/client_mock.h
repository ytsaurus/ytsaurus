#pragma once

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>

#include <yt/client/table_client/name_table.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/test_framework/framework.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TMockClient
    : public IClient
{
public:
    // IClientBase
    IConnectionPtr Connection;

    virtual IConnectionPtr GetConnection() override
    {
        return Connection;
    }

    MOCK_METHOD2(StartTransaction, TFuture<ITransactionPtr>(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options));

    MOCK_METHOD4(LookupRows, TFuture<IUnversionedRowsetPtr>(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options));

    MOCK_METHOD4(VersionedLookupRows, TFuture<IVersionedRowsetPtr>(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options));

    MOCK_METHOD2(SelectRows, TFuture<TSelectRowsResult>(
        const TString& query,
        const TSelectRowsOptions& options));

    MOCK_METHOD2(ExplainQuery, TFuture<NYson::TYsonString>(
        const TString& query,
        const TExplainQueryOptions& options));

    MOCK_METHOD2(CreateTableReader, TFuture<ITableReaderPtr>(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options));

    MOCK_METHOD2(CreateTableWriter, TFuture<ITableWriterPtr>(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options));

    MOCK_METHOD2(GetNode, TFuture<NYson::TYsonString>(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options));

    MOCK_METHOD3(SetNode, TFuture<void>(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options));

    MOCK_METHOD2(RemoveNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options));

    MOCK_METHOD2(ListNode, TFuture<NYson::TYsonString>(
        const NYPath::TYPath& path,
        const TListNodeOptions& options));

    MOCK_METHOD3(CreateNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options));

    MOCK_METHOD3(LockNode, TFuture<TLockNodeResult>(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options));

    MOCK_METHOD2(UnlockNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options));

    MOCK_METHOD3(CopyNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options));

    MOCK_METHOD3(MoveNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options));

    MOCK_METHOD3(LinkNode, TFuture<NCypressClient::TNodeId>(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options));

    MOCK_METHOD3(ConcatenateNodes, TFuture<void>(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options));

    MOCK_METHOD3(ExternalizeNode, TFuture<void>(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options));

    MOCK_METHOD2(InternalizeNode, TFuture<void>(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options));

    MOCK_METHOD2(NodeExists, TFuture<bool>(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options));

    MOCK_METHOD2(CreateObject, TFuture<NObjectClient::TObjectId>(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options));

    MOCK_METHOD2(CreateFileReader, TFuture<IFileReaderPtr>(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options));

    MOCK_METHOD2(CreateFileWriter, IFileWriterPtr(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options));

    MOCK_METHOD2(CreateJournalReader, IJournalReaderPtr(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options));

    MOCK_METHOD2(CreateJournalWriter, IJournalWriterPtr(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options));

    // ICLient
    NTabletClient::ITableMountCachePtr TableMountCache;
    NTransactionClient::ITimestampProviderPtr TimestampProvider;

    MOCK_METHOD0(Terminate, void());

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override
    {
        return TableMountCache;
    }
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override
    {
        return TimestampProvider;
    }

    MOCK_METHOD2(AttachTransaction, ITransactionPtr(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options));

    MOCK_METHOD2(MountTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TMountTableOptions& options));

    MOCK_METHOD2(UnmountTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options));

    MOCK_METHOD2(RemountTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options));

    MOCK_METHOD2(FreezeTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options));

    MOCK_METHOD2(UnfreezeTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options));

    MOCK_METHOD3(ReshardTable, TFuture<void>(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const TReshardTableOptions& options));

    MOCK_METHOD3(ReshardTable, TFuture<void>(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options));

    MOCK_METHOD2(ReshardTableAutomatic, TFuture<std::vector<NTabletClient::TTabletActionId>>(
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options));

    MOCK_METHOD4(TrimTable, TFuture<void>(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options));

    MOCK_METHOD2(AlterTable, TFuture<void>(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options));

    MOCK_METHOD2(AlterTableReplica, TFuture<void>(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options));

    MOCK_METHOD2(GetTablePivotKeys, TFuture<NYson::TYsonString>(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options));

    MOCK_METHOD4(GetInSyncReplicas, TFuture<std::vector<NTabletClient::TTableReplicaId>>(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options));

    MOCK_METHOD3(GetTabletInfos, TFuture<std::vector<TTabletInfo>>(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options));

    MOCK_METHOD3(BalanceTabletCells, TFuture<std::vector<NTabletClient::TTabletActionId>>(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options));

    MOCK_METHOD2(LocateSkynetShare, TFuture<TSkynetSharePartsLocationsPtr>(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options));

    MOCK_METHOD2(GetColumnarStatistics, TFuture<std::vector<NTableClient::TColumnarStatistics>>(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options));

    MOCK_METHOD3(TruncateJournal, TFuture<void>(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options));

    MOCK_METHOD2(GetFileFromCache, TFuture<TGetFileFromCacheResult>(
        const TString& md5,
        const TGetFileFromCacheOptions& options));

    MOCK_METHOD3(PutFileToCache, TFuture<TPutFileToCacheResult>(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options));

    MOCK_METHOD3(AddMember, TFuture<void>(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options));

    MOCK_METHOD3(RemoveMember, TFuture<void>(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options));

    MOCK_METHOD4(CheckPermission, TFuture<TCheckPermissionResponse>(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options));

    MOCK_METHOD4(CheckPermissionByAcl, TFuture<TCheckPermissionByAclResult>(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options));

    MOCK_METHOD3(StartOperation, TFuture<NScheduler::TOperationId>(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options));

    MOCK_METHOD2(AbortOperation, TFuture<void>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options));

    MOCK_METHOD2(SuspendOperation, TFuture<void>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options));

    MOCK_METHOD2(ResumeOperation, TFuture<void>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options));

    MOCK_METHOD2(CompleteOperation, TFuture<void>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options));

    MOCK_METHOD3(UpdateOperationParameters, TFuture<void>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options));

    MOCK_METHOD2(GetOperation, TFuture<NYson::TYsonString>(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options));

    MOCK_METHOD3(DumpJobContext, TFuture<void>(
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options));

    MOCK_METHOD2(GetJobInput, TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options));

    MOCK_METHOD2(GetJobInputPaths, TFuture<NYson::TYsonString>(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options));

    MOCK_METHOD3(GetJobStderr, TFuture<TSharedRef>(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options));

    MOCK_METHOD3(GetJobFailContext, TFuture<TSharedRef>(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options));

    MOCK_METHOD1(ListOperations, TFuture<TListOperationsResult>(
        const TListOperationsOptions& options));

    MOCK_METHOD2(ListJobs, TFuture<TListJobsResult>(
        NJobTrackerClient::TOperationId operationId,
        const TListJobsOptions& options));

    MOCK_METHOD3(GetJob, TFuture<NYson::TYsonString>(
        NScheduler::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options));

    MOCK_METHOD2(AbandonJob, TFuture<void>(
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options));

    MOCK_METHOD3(PollJobShell, TFuture<NYson::TYsonString>(
        NJobTrackerClient::TJobId jobId,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options));

    MOCK_METHOD2(AbortJob, TFuture<void>(
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options));

    MOCK_METHOD1(GetClusterMeta, TFuture<TClusterMeta>(
        const TGetClusterMetaOptions& options));
};

DEFINE_REFCOUNTED_TYPE(TMockClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
