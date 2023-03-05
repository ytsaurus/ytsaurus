#include "client.h"

#include "config.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/misc/method_helpers.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NApi;

DECLARE_REFCOUNTED_CLASS(TFederatedClient);

////////////////////////////////////////////////////////////////////////////////

class TFederatedTransaction
    : public virtual ITransaction
{
public:
    TFederatedTransaction(TFederatedClientPtr federatedClient, size_t clientId, ITransactionPtr underlying);

    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) override;

    TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;

    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;

    void ModifyRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const TModifyRowsOptions& options) override;

    TFuture<TTransactionFlushResult> Flush() override;

    TFuture<void> Ping(const NApi::TTransactionPingOptions& options = {}) override;

    TFuture<TTransactionCommitResult> Commit(const TTransactionCommitOptions& options = TTransactionCommitOptions()) override;

    TFuture<void> Abort(const TTransactionAbortOptions& options = TTransactionAbortOptions()) override;

    TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath&, NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TUnversionedRow>&,
        const TVersionedLookupRowsOptions&) override;

    TFuture<std::vector<IUnversionedRowsetPtr>> MultiLookup(
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&) override;

    TFuture<NYson::TYsonString> ExplainQuery(const TString&, const TExplainQueryOptions&) override;

    TFuture<NYson::TYsonString> GetNode(const NYPath::TYPath&, const TGetNodeOptions&) override;

    TFuture<NYson::TYsonString> ListNode(const NYPath::TYPath&, const TListNodeOptions&) override;

    TFuture<bool> NodeExists(const NYPath::TYPath&, const TNodeExistsOptions&) override;

    TFuture<TPullRowsResult> PullRows(const NYPath::TYPath&, const TPullRowsOptions&) override;

    IClientPtr GetClient() const override
    {
        return Underlying_->GetClient();
    }

    NTransactionClient::ETransactionType GetType() const override
    {
        return Underlying_->GetType();
    }

    NTransactionClient::TTransactionId GetId() const override
    {
        return Underlying_->GetId();
    }

    NTransactionClient::TTimestamp GetStartTimestamp() const override
    {
        return Underlying_->GetStartTimestamp();
    }

    virtual NTransactionClient::EAtomicity GetAtomicity() const override
    {
        return Underlying_->GetAtomicity();
    }

    virtual NTransactionClient::EDurability GetDurability() const override
    {
        return Underlying_->GetDurability();
    }

    virtual TDuration GetTimeout() const override
    {
        return Underlying_->GetTimeout();
    }

    void Detach() override {
        return Underlying_->Detach();
    }

    void RegisterAlienTransaction(const ITransactionPtr& transaction) override
    {
        return Underlying_->RegisterAlienTransaction(transaction);
    }

    IConnectionPtr GetConnection() override
    {
        return Underlying_->GetConnection();
    }

    void SubscribeCommitted(const TCommittedHandler& handler) override
    {
        Underlying_->SubscribeCommitted(handler);
    }

    void UnsubscribeCommitted(const TCommittedHandler& handler) override
    {
        Underlying_->UnsubscribeCommitted(handler);
    }

    void SubscribeAborted(const TAbortedHandler& handler) override
    {
        Underlying_->SubscribeAborted(handler);
    }

    void UnsubscribeAborted(const TAbortedHandler& handler) override
    {
        Underlying_->UnsubscribeAborted(handler);
    }

    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (const NYPath::TYPath&, const NYTree::IMapNodePtr&, const TMultisetAttributesNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (const NYPath::TYPath&, const TRemoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (const NYPath::TYPath&, NObjectClient::EObjectType, const TCreateNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (const NYPath::TYPath&, NCypressClient::ELockMode, const TLockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (const NYPath::TYPath&, const TUnlockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TCopyNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TMoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TLinkNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (const std::vector<NYPath::TRichYPath>&, const NYPath::TRichYPath&, const TConcatenateNodesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (const NYPath::TYPath&, NObjectClient::TCellTag, const TExternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (const NYPath::TYPath&, const TInternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (NObjectClient::EObjectType, const TCreateObjectOptions&));

    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (const NYPath::TRichYPath&, const TTableReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (const NYPath::TYPath&, const TFileReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (const NYPath::TRichYPath&, const TTableWriterOptions&));
    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (const NYPath::TRichYPath&, const TFileWriterOptions&));
    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (const NYPath::TYPath&, const TJournalReaderOptions&));
    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (const NYPath::TYPath&, const TJournalWriterOptions&));

private:
    template <typename TResultType>
    auto CreateResultHandler();

    TFederatedClientPtr FederatedClient_;
    size_t ClientId_;
    ITransactionPtr Underlying_;
};

DECLARE_REFCOUNTED_TYPE(TFederatedTransaction);

////////////////////////////////////////////////////////////////////////////////

struct TClientDescr final
{
    TClientDescr(IClientPtr client, int priority)
        : Client(std::move(client))
        , Priority(priority)
    { }

    IClientPtr Client;
    int Priority;
    std::atomic<int> HasErrors{false};
};
DECLARE_REFCOUNTED_STRUCT(TClientDescr);
DEFINE_REFCOUNTED_TYPE(TClientDescr);

////////////////////////////////////////////////////////////////////////////////

class TFederatedClient
    : public virtual IClient
{
public:
    TFederatedClient(const std::vector<IClientPtr>& clients, TFederatedClientConfigPtr config);

    TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;

    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;

    TFuture<std::vector<IUnversionedRowsetPtr>> MultiLookup(const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&) override;

    TFuture<IVersionedRowsetPtr> VersionedLookupRows(const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&) override;

    TFuture<TPullRowsResult> PullRows(const NYPath::TYPath&, const TPullRowsOptions&) override;

    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(const TString&, const TExplainQueryOptions&) override;

    TFuture<NYson::TYsonString> GetNode(const NYPath::TYPath&, const TGetNodeOptions&) override;

    TFuture<NYson::TYsonString> ListNode(const NYPath::TYPath&, const TListNodeOptions&) override;

    TFuture<bool> NodeExists(const NYPath::TYPath&, const TNodeExistsOptions&) override;

    IConnectionPtr GetConnection() override
    {
        auto [client, clientId] = GetActiveClient();
        return client->GetConnection();
    }

    std::optional<TStringBuf> GetClusterName(bool fetchIfNull) override
    {
        auto [client, clientId] = GetActiveClient();
        return client->GetClusterName(fetchIfNull);
    }

    void Terminate() override
    { }

    // IClientBase Unsupported methods.
    UNIMPLEMENTED_METHOD(TFuture<void>, SetNode, (const NYPath::TYPath&, const NYson::TYsonString&, const TSetNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MultisetAttributesNode, (const NYPath::TYPath&, const NYTree::IMapNodePtr&, const TMultisetAttributesNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveNode, (const NYPath::TYPath&, const TRemoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CreateNode, (const NYPath::TYPath&, NObjectClient::EObjectType, const TCreateNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TLockNodeResult>, LockNode, (const NYPath::TYPath&, NCypressClient::ELockMode, const TLockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnlockNode, (const NYPath::TYPath&, const TUnlockNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, CopyNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TCopyNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, MoveNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TMoveNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NCypressClient::TNodeId>, LinkNode, (const NYPath::TYPath&, const NYPath::TYPath&, const TLinkNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ConcatenateNodes, (const std::vector<NYPath::TRichYPath>&, const NYPath::TRichYPath&, const TConcatenateNodesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ExternalizeNode, (const NYPath::TYPath&, NObjectClient::TCellTag, const TExternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, InternalizeNode, (const NYPath::TYPath&, const TInternalizeNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NObjectClient::TObjectId>, CreateObject, (NObjectClient::EObjectType, const TCreateObjectOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TQueryResult>, GetQueryResult, (NQueryTrackerClient::TQueryId, i64, const TGetQueryResultOptions&));

    // IClient unsupported methods.
    UNIMPLEMENTED_METHOD(TFuture<void>, RegisterQueueConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, bool, const TRegisterQueueConsumerOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnregisterQueueConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, const TUnregisterQueueConsumerOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (const NYPath::TRichYPath&, i64, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullQueueOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, i64, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullConsumerOptions&));
    UNIMPLEMENTED_METHOD(const NTabletClient::ITableMountCachePtr&, GetTableMountCache, ());
    UNIMPLEMENTED_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, ());
    UNIMPLEMENTED_METHOD(const NTransactionClient::ITimestampProviderPtr&, GetTimestampProvider, ());
    UNIMPLEMENTED_METHOD(ITransactionPtr, AttachTransaction, (NTransactionClient::TTransactionId, const TTransactionAttachOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MountTable, (const NYPath::TYPath&, const TMountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnmountTable, (const NYPath::TYPath&, const TUnmountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemountTable, (const NYPath::TYPath&, const TRemountTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, FreezeTable, (const NYPath::TYPath&, const TFreezeTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UnfreezeTable, (const NYPath::TYPath&, const TUnfreezeTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (const NYPath::TYPath&, const std::vector<NTableClient::TUnversionedOwningRow>&, const TReshardTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ReshardTable, (const NYPath::TYPath&, int, const TReshardTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, ReshardTableAutomatic, (const NYPath::TYPath&, const TReshardTableAutomaticOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TrimTable, (const NYPath::TYPath&, int, i64, const TTrimTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTable, (const NYPath::TYPath&, const TAlterTableOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterTableReplica, (NTabletClient::TTableReplicaId, const TAlterTableReplicaOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterReplicationCard, (NChaosClient::TReplicationCardId, const TAlterReplicationCardOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (const NYPath::TYPath&, const NTableClient::TNameTablePtr&, const TSharedRange<NTableClient::TUnversionedRow>&, const TGetInSyncReplicasOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTableReplicaId>>, GetInSyncReplicas, (const NYPath::TYPath&, const TGetInSyncReplicasOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<TTabletInfo>>, GetTabletInfos, (const NYPath::TYPath&, const std::vector<int>&, const TGetTabletInfosOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetTabletErrorsResult>, GetTabletErrors, (const NYPath::TYPath&, const TGetTabletErrorsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTabletClient::TTabletActionId>>, BalanceTabletCells, (const TString&, const std::vector<NYPath::TYPath>&, const TBalanceTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSkynetSharePartsLocationsPtr>, LocateSkynetShare, (const NYPath::TRichYPath&, const TLocateSkynetShareOptions&));
    UNIMPLEMENTED_METHOD(TFuture<std::vector<NTableClient::TColumnarStatistics>>, GetColumnarStatistics, (const std::vector<NYPath::TRichYPath>&, const TGetColumnarStatisticsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TMultiTablePartitions>, PartitionTables, (const std::vector<NYPath::TRichYPath>&, const TPartitionTablesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetTablePivotKeys, (const NYPath::TYPath&, const TGetTablePivotKeysOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CreateTableBackup, (const TBackupManifestPtr&, const TCreateTableBackupOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RestoreTableBackup, (const TBackupManifestPtr&, const TRestoreTableBackupOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TruncateJournal, (const NYPath::TYPath&, i64, const TTruncateJournalOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGetFileFromCacheResult>, GetFileFromCache, (const TString&, const TGetFileFromCacheOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TPutFileToCacheResult>, PutFileToCache, (const NYPath::TYPath&, const TString&, const TPutFileToCacheOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AddMember, (const TString&, const TString&, const TAddMemberOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveMember, (const TString&, const TString&, const TRemoveMemberOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionResponse>, CheckPermission, (const TString&, const NYPath::TYPath&, NYTree::EPermission, const TCheckPermissionOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCheckPermissionByAclResult>, CheckPermissionByAcl, (const std::optional<TString>&, NYTree::EPermission, NYTree::INodePtr, const TCheckPermissionByAclOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferAccountResources, (const TString&, const TString&, NYTree::INodePtr, const TTransferAccountResourcesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, TransferPoolResources, (const TString&, const TString&, const TString&, NYTree::INodePtr, const TTransferPoolResourcesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NScheduler::TOperationId>, StartOperation, (NScheduler::EOperationType, const NYson::TYsonString&, const TStartOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortOperation, (const NScheduler::TOperationIdOrAlias&, const TAbortOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendOperation, (const NScheduler::TOperationIdOrAlias&, const TSuspendOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeOperation, (const NScheduler::TOperationIdOrAlias&, const TResumeOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CompleteOperation, (const NScheduler::TOperationIdOrAlias&, const TCompleteOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateOperationParameters, (const NScheduler::TOperationIdOrAlias&, const NYson::TYsonString&, const TUpdateOperationParametersOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TOperation>, GetOperation, (const NScheduler::TOperationIdOrAlias&, const TGetOperationOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, DumpJobContext, (NJobTrackerClient::TJobId, const NYPath::TYPath&, const TDumpJobContextOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr>, GetJobInput, (NJobTrackerClient::TJobId, const TGetJobInputOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobInputPaths, (NJobTrackerClient::TJobId, const TGetJobInputPathsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJobSpec, (NJobTrackerClient::TJobId, const TGetJobSpecOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobStderr, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobStderrOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TSharedRef>, GetJobFailContext, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobFailContextOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListOperationsResult>, ListOperations, (const TListOperationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListJobsResult>, ListJobs, (const NScheduler::TOperationIdOrAlias&, const TListJobsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NYson::TYsonString>, GetJob, (const NScheduler::TOperationIdOrAlias&, NJobTrackerClient::TJobId, const TGetJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbandonJob, (NJobTrackerClient::TJobId, const TAbandonJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TPollJobShellResponse>, PollJobShell, (NJobTrackerClient::TJobId, const std::optional<TString>&, const NYson::TYsonString&, const TPollJobShellOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortJob, (NJobTrackerClient::TJobId, const TAbortJobOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TClusterMeta>, GetClusterMeta, (const TGetClusterMetaOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, CheckClusterLiveness, (const TCheckClusterLivenessOptions&));
    UNIMPLEMENTED_METHOD(TFuture<int>, BuildSnapshot, (const TBuildSnapshotOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TCellIdToSnapshotIdMap>, BuildMasterSnapshots, (const TBuildMasterSnapshotsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SwitchLeader, (NObjectClient::TCellId, const TString&, const TSwitchLeaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResetStateHash, (NObjectClient::TCellId, const TResetStateHashOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, GCCollect, (const TGCCollectOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, KillProcess, (const TString&, const TKillProcessOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TString>, WriteCoreDump, (const TString&, const TWriteCoreDumpOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TGuid>, WriteLogBarrier, (const TString&, const TWriteLogBarrierOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TString>, WriteOperationControllerCoreDump, (NJobTrackerClient::TOperationId, const TWriteOperationControllerCoreDumpOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, HealExecNode, (const TString&, const THealExecNodeOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendCoordinator, (NObjectClient::TCellId, const TSuspendCoordinatorOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeCoordinator, (NObjectClient::TCellId, const TResumeCoordinatorOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, MigrateReplicationCards, (NObjectClient::TCellId, const TMigrateReplicationCardsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendChaosCells, (const std::vector<NObjectClient::TCellId>&, const TSuspendChaosCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeChaosCells, (const std::vector<NObjectClient::TCellId>&, const TResumeChaosCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SuspendTabletCells, (const std::vector<NObjectClient::TCellId>&, const TSuspendTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, ResumeTabletCells, (const std::vector<NObjectClient::TCellId>&, const TResumeTabletCellsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NChaosClient::TReplicationCardPtr>, GetReplicationCard, (NChaosClient::TReplicationCardId, const TGetReplicationCardOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, UpdateChaosTableReplicaProgress, (NChaosClient::TReplicaId, const TUpdateChaosTableReplicaProgressOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NNodeTrackerClient::TMaintenanceId>, AddMaintenance, (const TString&, NNodeTrackerClient::EMaintenanceType, const TString&, const TAddMaintenanceOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RemoveMaintenance, (const TString&, NNodeTrackerClient::TMaintenanceId, const TRemoveMaintenanceOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TDisableChunkLocationsResult>, DisableChunkLocations, (const TString&, const std::vector<TGuid>&, const TDisableChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TDestroyChunkLocationsResult>, DestroyChunkLocations, (const TString&, const std::vector<TGuid>&, const TDestroyChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TResurrectChunkLocationsResult>, ResurrectChunkLocations, (const TString&, const std::vector<TGuid>&, const TResurrectChunkLocationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, SetUserPassword, (const TString&, const TString&, const TString&, const TSetUserPasswordOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TIssueTokenResult>, IssueToken, (const TString&, const TString&, const TIssueTokenOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, RevokeToken, (const TString&, const TString&, const TString&, const TRevokeTokenOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListUserTokensResult>, ListUserTokens, (const TString&, const TString&, const TListUserTokensOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueryTrackerClient::TQueryId>, StartQuery, (NQueryTrackerClient::EQueryEngine, const TString&, const TStartQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AbortQuery, (NQueryTrackerClient::TQueryId, const TAbortQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IUnversionedRowsetPtr>, ReadQueryResult, (NQueryTrackerClient::TQueryId, i64, const TReadQueryResultOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TQuery>, GetQuery, (NQueryTrackerClient::TQueryId, const TGetQueryOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TListQueriesResult>, ListQueries, (const TListQueriesOptions&));
    UNIMPLEMENTED_METHOD(TFuture<void>, AlterQuery, (NQueryTrackerClient::TQueryId, const TAlterQueryOptions&));

    UNIMPLEMENTED_METHOD(TFuture<ITableReaderPtr>, CreateTableReader, (const NYPath::TRichYPath&, const TTableReaderOptions&));
    UNIMPLEMENTED_METHOD(TFuture<ITableWriterPtr>, CreateTableWriter, (const NYPath::TRichYPath&, const TTableWriterOptions&));
    UNIMPLEMENTED_METHOD(TFuture<IFileReaderPtr>, CreateFileReader, (const NYPath::TYPath&, const TFileReaderOptions&));
    UNIMPLEMENTED_METHOD(IFileWriterPtr, CreateFileWriter, (const NYPath::TRichYPath&, const TFileWriterOptions&));
    UNIMPLEMENTED_METHOD(IJournalReaderPtr, CreateJournalReader, (const NYPath::TYPath&, const TJournalReaderOptions&));
    UNIMPLEMENTED_METHOD(IJournalWriterPtr, CreateJournalWriter, (const NYPath::TYPath&, const TJournalWriterOptions&));

    friend class TFederatedTransaction;

private:
    struct TActiveClientInfo {
        IClientPtr Client;
        size_t Id;
    };

    void HandleError(const TErrorOr<void>& error, size_t clientId);
    void UpdateActiveClient();
    TActiveClientInfo GetActiveClient();

    template <typename TReturnType, typename TRetryCallback>
    auto CreateResultHandler(int clientId, int retryAttemptsCount, TRetryCallback callback);

    std::optional<TString> GetDataCenter(const IClientPtr& client);

    void CheckClustersHealth();

    TFuture<IUnversionedRowsetPtr> DoLookupRows(
        int retryAttemptsCount,
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options);
    TFuture<TSelectRowsResult> DoSelectRows(
        int retryAttemptsCount,
        const TString& query,
        const TSelectRowsOptions& options);
    TFuture<std::vector<IUnversionedRowsetPtr>> DoMultiLookup(int retryAttemptsCount, const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&);
    TFuture<IVersionedRowsetPtr> DoVersionedLookupRows(int retryAttemptsCount, const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&);
    TFuture<TPullRowsResult> DoPullRows(int retryAttemptsCount, const NYPath::TYPath&, const TPullRowsOptions&);
    TFuture<NYson::TYsonString> DoExplainQuery(int retryAttemptsCount, const TString&, const TExplainQueryOptions&);
    TFuture<NYson::TYsonString> DoGetNode(int retryAttemptsCount, const NYPath::TYPath&, const TGetNodeOptions&);
    TFuture<NYson::TYsonString> DoListNode(int retryAttemptsCount, const NYPath::TYPath&, const TListNodeOptions&);
    TFuture<bool> DoNodeExists(int retryAttemptsCount, const NYPath::TYPath&, const TNodeExistsOptions&);
    TFuture<ITransactionPtr> DoStartTransaction(
        int retryAttemptsCount,
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options);

private:
    std::vector<TClientDescrPtr> Clients_;
    IClientPtr ActiveClient_;
    std::atomic<size_t> ActiveClientId_;

    NThreading::TReaderWriterSpinLock Lock_;

    TFederatedClientConfigPtr Config_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
};

DECLARE_REFCOUNTED_TYPE(TFederatedTransaction);

////////////////////////////////////////////////////////////////////////////////

TFederatedTransaction::TFederatedTransaction(TFederatedClientPtr federatedClient, size_t clientId, ITransactionPtr underlying)
    : FederatedClient_(std::move(federatedClient))
    , ClientId_(clientId)
    , Underlying_(std::move(underlying))
{ }

template <typename TResultType>
auto TFederatedTransaction::CreateResultHandler()
{
    return [this, this_ = MakeStrong(this)] (const TErrorOr<TResultType>& result) {
        if (!result.IsOK()) {
            FederatedClient_->HandleError(result, ClientId_);
        }
        return result;
    };
}

#define TRANSACTION_METHOD_IMPL(ResultType, MethodName, Args)                           \
TFuture<ResultType> TFederatedTransaction::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                     \
{                                                                                             \
    return Underlying_->MethodName(Y_PASS_METHOD_USED_ARGS(Args)).Apply(BIND(CreateResultHandler<ResultType>()));  \
} Y_SEMICOLON_GUARD

TRANSACTION_METHOD_IMPL(IUnversionedRowsetPtr, LookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TLookupRowsOptions&));
TRANSACTION_METHOD_IMPL(TSelectRowsResult, SelectRows, (const TString&, const TSelectRowsOptions&));
TRANSACTION_METHOD_IMPL(void, Ping, (const NApi::TTransactionPingOptions&));
TRANSACTION_METHOD_IMPL(TTransactionCommitResult, Commit, (const TTransactionCommitOptions&));
TRANSACTION_METHOD_IMPL(void, Abort, (const TTransactionAbortOptions&));
TRANSACTION_METHOD_IMPL(IVersionedRowsetPtr, VersionedLookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&));
TRANSACTION_METHOD_IMPL(std::vector<IUnversionedRowsetPtr>, MultiLookup, (const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&));
TRANSACTION_METHOD_IMPL(TPullRowsResult, PullRows, (const NYPath::TYPath&, const TPullRowsOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, ExplainQuery, (const TString&, const TExplainQueryOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, GetNode, (const NYPath::TYPath&, const TGetNodeOptions&));
TRANSACTION_METHOD_IMPL(NYson::TYsonString, ListNode, (const NYPath::TYPath&, const TListNodeOptions&));
TRANSACTION_METHOD_IMPL(bool, NodeExists, (const NYPath::TYPath&, const TNodeExistsOptions&));

void TFederatedTransaction::ModifyRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    Underlying_->ModifyRows(path, nameTable, modifications, options);
}

TFuture<TTransactionFlushResult> TFederatedTransaction::Flush()
{
    return Underlying_->Flush().Apply(BIND(CreateResultHandler<TTransactionFlushResult>()));
}

TFuture<ITransactionPtr> TFederatedTransaction::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Underlying_->StartTransaction(type, options).Apply(BIND(
        [this, this_ = MakeStrong(this)] (const TErrorOr<ITransactionPtr>& result) -> ITransactionPtr {
            if (!result.IsOK()) {
                FederatedClient_->HandleError(result, ClientId_);
            }
            return New<TFederatedTransaction>(FederatedClient_, ClientId_, result.ValueOrThrow());
        }
    ));
}

DEFINE_REFCOUNTED_TYPE(TFederatedTransaction);

////////////////////////////////////////////////////////////////////////////////

TFederatedClient::TFederatedClient(const std::vector<IClientPtr>& clients, TFederatedClientConfigPtr config)
    : Config_(std::move(config))
    , Executor_(New<NConcurrency::TPeriodicExecutor>(
            NYT::NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TFederatedClient::CheckClustersHealth, MakeWeak(this)),
            Config_->CheckClustersHealthPeriod))
{
    // TODO: check that Clients_ is not empty

    for (const auto& client : clients) {
        auto dataCenter = GetDataCenter(client);
        if (!dataCenter) {
            continue;
        }
        int priority = *dataCenter == NNet::GetLocalYPCluster() ? 1 : 0;
        Clients_.push_back(New<TClientDescr>(client, priority));
    }
    std::sort(Clients_.begin(), Clients_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs->Priority > rhs->Priority;
    });

    ActiveClient_ = Clients_.front()->Client;
    ActiveClientId_ = 0;

    Executor_->Start();
}

void TFederatedClient::CheckClustersHealth()
{
    TCheckClusterLivenessOptions options;
    options.CheckCypressRoot = true;
    options.CheckTabletCellBundle = Config_->BundleName;

    auto activeClientId = ActiveClientId_.load();
    std::optional<size_t> betterClientId;

    std::vector<TFuture<void>> checks;
    checks.reserve(Clients_.size());

    for (const auto& clientDescr : Clients_) {
        checks.emplace_back(clientDescr->Client->CheckClusterLiveness(options));
    }

    for (size_t id = 0; id < checks.size(); ++id) {
        const auto& check = checks[id];
        auto hasErrors = !NConcurrency::WaitFor(check).IsOK();
        Clients_[id]->HasErrors = hasErrors;
        if (!betterClientId && !hasErrors && id < activeClientId) {
            betterClientId = id;
        }
    }

    if (betterClientId.has_value() && ActiveClientId_ == activeClientId) {
        auto newClientId = *betterClientId;
        auto guard = NThreading::WriterGuard(Lock_);
        ActiveClient_ = Clients_[newClientId]->Client;
        ActiveClientId_ = newClientId;
        return;
    }

    // If active cluster isn't health, try change it.
    if (Clients_[activeClientId]->HasErrors) {
        auto guard = NThreading::WriterGuard(Lock_);
        // Check that active client wasn't changed.
        if (ActiveClientId_ == activeClientId && Clients_[activeClientId]->HasErrors) {
            UpdateActiveClient();
        }
    }
}

std::optional<TString> TFederatedClient::GetDataCenter(const IClientPtr& client)
{
    TListNodeOptions options;
    options.MaxSize = 1;

    auto items = NConcurrency::WaitFor(client->ListNode("//sys/rpc_proxies", options))
         .ValueOrThrow();
    auto itemsList = NYTree::ConvertTo<NYTree::IListNodePtr>(items);
    if (itemsList->GetChildCount() < 1) {
        return std::nullopt;
    }
    auto host = itemsList->GetChildren()[0];
    return NNet::InferYPClusterFromHostName(host->GetValue<TString>());
}

template <typename TResultType, typename TRetryCallback>
auto TFederatedClient::CreateResultHandler(int clientId, int retryAttemptsCount, TRetryCallback callback)
{
    return [this, this_ = MakeStrong(this), clientId, retryAttemptsCount, callback=std::move(callback)] (const TErrorOr<TResultType>& resultOrError) {
        if (!resultOrError.IsOK()) {
            HandleError(resultOrError, clientId);
            if (retryAttemptsCount > 1) {
                return callback(retryAttemptsCount - 1);
            }
        }
        return MakeFuture(resultOrError);
    };
}

TFuture<ITransactionPtr> TFederatedClient::StartTransaction(
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    return DoStartTransaction(Config_->ClusterRetryAttempts, type, options);
}

TFuture<ITransactionPtr> TFederatedClient::DoStartTransaction(
    int retryAttemptsCount,
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    auto [client, clientId] = GetActiveClient();

    return client->StartTransaction(type, options)
        .Apply(BIND([this, this_ = MakeStrong(this), clientId=clientId, retryAttemptsCount, type, &options] (const TErrorOr<ITransactionPtr>& transactionOrError) {
            if (!transactionOrError.IsOK()) {
                HandleError(transactionOrError, clientId);
                if (retryAttemptsCount > 1) {
                    return DoStartTransaction(retryAttemptsCount - 1, type, options);
                }
                return MakeFuture(transactionOrError);
            }
            return MakeFuture(ITransactionPtr{New<TFederatedTransaction>(std::move(this_), clientId, transactionOrError.Value())});
        }
    ));
}

#define CLIENT_METHOD_IMPL(ResultType, MethodName, Args, CaptureList)                                                   \
TFuture<ResultType> TFederatedClient::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                      \
{                                                                                                                       \
    return Do##MethodName(Config_->ClusterRetryAttempts, Y_PASS_METHOD_USED_ARGS(Args));                  \
}                                                                                                  \
                                                                                                                        \
TFuture<ResultType> TFederatedClient::Do##MethodName(int retryAttempsCount, Y_METHOD_USED_ARGS_DECLARATION(Args))          \
{                                                                                                                       \
    auto [client, clientId] = GetActiveClient();                                                                        \
                                                                                                                        \
    return client->MethodName(Y_PASS_METHOD_USED_ARGS(Args)).Apply(BIND(CreateResultHandler<ResultType>(                \
        clientId, retryAttempsCount, [this, Y_METHOD_UNUSED_ARGS_DECLARATION(CaptureList)] (int attemptsCount) {                                          \
            return Do##MethodName(attemptsCount, Y_PASS_METHOD_USED_ARGS(Args));                                       \
        }                                                                                                               \
    )));                                                                                                                \
} Y_SEMICOLON_GUARD

CLIENT_METHOD_IMPL(IUnversionedRowsetPtr, LookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TLegacyKey>&, const TLookupRowsOptions&), (&a1, a2, &a3, &a4));
CLIENT_METHOD_IMPL(TSelectRowsResult, SelectRows, (const TString&, const TSelectRowsOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(std::vector<IUnversionedRowsetPtr>, MultiLookup, (const std::vector<TMultiLookupSubrequest>&, const TMultiLookupOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(IVersionedRowsetPtr, VersionedLookupRows, (const NYPath::TYPath&, NTableClient::TNameTablePtr, const TSharedRange<NTableClient::TUnversionedRow>&, const TVersionedLookupRowsOptions&), (&a1, a2, &a3, &a4));
CLIENT_METHOD_IMPL(TPullRowsResult, PullRows, (const NYPath::TYPath&, const TPullRowsOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(NYson::TYsonString, ExplainQuery, (const TString&, const TExplainQueryOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(NYson::TYsonString, GetNode, (const NYPath::TYPath&, const TGetNodeOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(NYson::TYsonString, ListNode, (const NYPath::TYPath&, const TListNodeOptions&), (&a1, &a2));
CLIENT_METHOD_IMPL(bool, NodeExists, (const NYPath::TYPath&, const TNodeExistsOptions&), (&a1, &a2));

void TFederatedClient::HandleError(const TErrorOr<void>& /*error*/, size_t clientId)
{
    // TODO(nadya73): check error and do nothing if it's not fatal error

    Clients_[clientId]->HasErrors = true;
    if (ActiveClientId_ != clientId) {
        return;
    }

    auto guard = WriterGuard(Lock_);
    if (ActiveClientId_ != clientId) {
        return;
    }

    UpdateActiveClient();
}

void TFederatedClient::UpdateActiveClient()
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(Lock_);

    for (size_t id = 0; id < Clients_.size(); ++id) {
        const auto& client = Clients_[id];
        if (!client->HasErrors) {
            ActiveClient_ = client->Client;
            ActiveClientId_ = id;
            break;
        }
    }
}

TFederatedClient::TActiveClientInfo TFederatedClient::GetActiveClient()
{
    auto guard = ReaderGuard(Lock_);
    return {ActiveClient_, ActiveClientId_.load()};
}

DEFINE_REFCOUNTED_TYPE(TFederatedClient);

////////////////////////////////////////////////////////////////////////////////

NApi::IClientPtr CreateFederatedClient(const std::vector<NApi::IClientPtr>& clients, TFederatedClientConfigPtr config)
{
    return New<TFederatedClient>(clients, config);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NClient::NFederated
