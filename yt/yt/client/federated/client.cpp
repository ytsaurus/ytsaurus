#include "client.h"

#include "config.h"
#include "private.h"

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

namespace {

using namespace NYT::NApi;

const auto& Logger = FederatedClientLogger;

DECLARE_REFCOUNTED_CLASS(TClient)

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> GetDataCenterByClient(const IClientPtr& client)
{
    TListNodeOptions options;
    options.MaxSize = 1;

    auto items = NConcurrency::WaitFor(client->ListNode("//sys/rpc_proxies", options))
        .ValueOrThrow();
    auto itemsList = NYTree::ConvertTo<NYTree::IListNodePtr>(items);
    if (!itemsList->GetChildCount()) {
        return std::nullopt;
    }
    auto host = itemsList->GetChildren()[0];
    return NNet::InferYPClusterFromHostName(host->GetValue<TString>());
}

class TTransaction
    : public ITransaction
{
public:
    TTransaction(TClientPtr client, size_t clientId, ITransactionPtr underlying);

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

    void Detach() override
    {
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

    TClientPtr Client_;
    size_t ClientId_;
    ITransactionPtr Underlying_;
};

DECLARE_REFCOUNTED_TYPE(TTransaction);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TClientDescription);

struct TClientDescription final
{
    TClientDescription(IClientPtr client, int priority)
        : Client(std::move(client))
        , Priority(priority)
    { }

    IClientPtr Client;
    int Priority;
    std::atomic<bool> HasErrors{false};
};

DEFINE_REFCOUNTED_TYPE(TClientDescription);

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(const std::vector<IClientPtr>& underlyingClients, TFederationConfigPtr config);

    TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options = {}) override;
    TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) override;
    TFuture<std::vector<IUnversionedRowsetPtr>> MultiLookup(
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&) override;
    TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath&, NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TUnversionedRow>&,
        const TVersionedLookupRowsOptions&) override;
    TFuture<TPullRowsResult> PullRows(const NYPath::TYPath&, const TPullRowsOptions&) override;

    TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options) override;

    TFuture<NYson::TYsonString> ExplainQuery(const TString&, const TExplainQueryOptions&) override;

    TFuture<NYson::TYsonString> GetNode(const NYPath::TYPath&, const TGetNodeOptions&) override;
    TFuture<NYson::TYsonString> ListNode(const NYPath::TYPath&, const TListNodeOptions&) override;
    TFuture<bool> NodeExists(const NYPath::TYPath&, const TNodeExistsOptions&) override;

    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    TFuture<std::vector<TTabletInfo>> GetTabletInfos(const NYPath::TYPath&, const std::vector<int>&, const TGetTabletInfosOptions&) override;

    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    ITransactionPtr AttachTransaction(NTransactionClient::TTransactionId, const TTransactionAttachOptions&) override;

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
    UNIMPLEMENTED_METHOD(TFuture<std::vector<TListQueueConsumerRegistrationsResult>>, ListQueueConsumerRegistrations, (const std::optional<NYPath::TRichYPath>&, const std::optional<NYPath::TRichYPath>&, const TListQueueConsumerRegistrationsOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue, (const NYPath::TRichYPath&, i64, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullQueueOptions&));
    UNIMPLEMENTED_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullConsumer, (const NYPath::TRichYPath&, const NYPath::TRichYPath&, i64, int, const NQueueClient::TQueueRowBatchReadOptions&, const TPullConsumerOptions&));
    UNIMPLEMENTED_METHOD(const NChaosClient::IReplicationCardCachePtr&, GetReplicationCardCache, ());
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
    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceId>, AddMaintenance, (EMaintenanceComponent, const TString&, EMaintenanceType, const TString&, const TAddMaintenanceOptions&));
    UNIMPLEMENTED_METHOD(TFuture<TMaintenanceCounts>, RemoveMaintenance, (EMaintenanceComponent, const TString&, const TMaintenanceFilter&, const TRemoveMaintenanceOptions&));
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

    friend class TTransaction;

private:
    struct TActiveClientInfo
    {
        IClientPtr Client;
        size_t Id;
    };

    void HandleError(const TErrorOr<void>& error, size_t clientId);
    void UpdateActiveClient();
    TActiveClientInfo GetActiveClient();

    template <typename TReturnType, typename TRetryCallback>
    auto CreateResultHandler(int clientId, int retryAttemptsCount, TRetryCallback callback);

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
    TFuture<std::vector<IUnversionedRowsetPtr>> DoMultiLookup(
        int retryAttemptsCount,
        const std::vector<TMultiLookupSubrequest>&,
        const TMultiLookupOptions&);
    TFuture<IVersionedRowsetPtr> DoVersionedLookupRows(
        int retryAttemptsCount,
        const NYPath::TYPath&, NTableClient::TNameTablePtr,
        const TSharedRange<NTableClient::TUnversionedRow>&,
        const TVersionedLookupRowsOptions&);
    TFuture<TPullRowsResult> DoPullRows(int retryAttemptsCount, const NYPath::TYPath&, const TPullRowsOptions&);

    TFuture<NYson::TYsonString> DoExplainQuery(int retryAttemptsCount, const TString&, const TExplainQueryOptions&);

    TFuture<NYson::TYsonString> DoGetNode(int retryAttemptsCount, const NYPath::TYPath&, const TGetNodeOptions&);
    TFuture<NYson::TYsonString> DoListNode(int retryAttemptsCount, const NYPath::TYPath&, const TListNodeOptions&);
    TFuture<bool> DoNodeExists(int retryAttemptsCount, const NYPath::TYPath&, const TNodeExistsOptions&);
    TFuture<std::vector<TTabletInfo>> DoGetTabletInfos(
        int retryAttemptsCount,
        const NYPath::TYPath&,
        const std::vector<int>&,
        const TGetTabletInfosOptions&);
    TFuture<ITransactionPtr> DoStartTransaction(
        int retryAttemptsCount,
        NTransactionClient::ETransactionType type,
        const NApi::TTransactionStartOptions& options);

private:
    const TFederationConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    std::vector<TClientDescriptionPtr> UnderlyingClients_;
    IClientPtr ActiveClient_;
    std::atomic<size_t> ActiveClientId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
};

DECLARE_REFCOUNTED_TYPE(TTransaction);

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TClientPtr client, size_t clientId, ITransactionPtr underlying)
    : Client_(std::move(client))
    , ClientId_(clientId)
    , Underlying_(std::move(underlying))
{ }

template <typename TResultType>
auto TTransaction::CreateResultHandler()
{
    return [this, this_ = MakeStrong(this)] (const TErrorOr<TResultType>& result) {
        if (!result.IsOK()) {
            Client_->HandleError(result, ClientId_);
        }
        return result;
    };
}

#define TRANSACTION_METHOD_IMPL(ResultType, MethodName, Args)                                                           \
TFuture<ResultType> TTransaction::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                  \
{                                                                                                                       \
    return Underlying_->MethodName(Y_PASS_METHOD_USED_ARGS(Args)).Apply(BIND(CreateResultHandler<ResultType>()));       \
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

void TTransaction::ModifyRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications,
    const TModifyRowsOptions& options)
{
    Underlying_->ModifyRows(path, nameTable, modifications, options);
}

TFuture<TTransactionFlushResult> TTransaction::Flush()
{
    return Underlying_->Flush().Apply(BIND(CreateResultHandler<TTransactionFlushResult>()));
}

TFuture<ITransactionPtr> TTransaction::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    return Underlying_->StartTransaction(type, options).Apply(BIND(
        [this, this_ = MakeStrong(this)] (const TErrorOr<ITransactionPtr>& result) -> ITransactionPtr {
            if (!result.IsOK()) {
                Client_->HandleError(result, ClientId_);
            }
            return New<TTransaction>(Client_, ClientId_, result.ValueOrThrow());
        }
    ));
}

DEFINE_REFCOUNTED_TYPE(TTransaction);

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(const std::vector<IClientPtr>& underlyingClients, TFederationConfigPtr config)
    : Config_(std::move(config))
    , Executor_(New<NConcurrency::TPeriodicExecutor>(
        NRpc::TDispatcher::Get()->GetLightInvoker(),
        BIND(&TClient::CheckClustersHealth, MakeWeak(this)),
        Config_->CheckClustersHealthPeriod))
{
    YT_VERIFY(!underlyingClients.empty());

    UnderlyingClients_.reserve(underlyingClients.size());
    const auto& localDatacenter = NNet::GetLocalYPCluster();
    for (const auto& client : underlyingClients) {
        int priority = GetDataCenterByClient(client) == localDatacenter ? 1 : 0;
        UnderlyingClients_.push_back(NYT::New<TClientDescription>(client, priority));
    }
    std::stable_sort(UnderlyingClients_.begin(), UnderlyingClients_.end(), [](const auto& lhs, const auto& rhs) {
        return lhs->Priority > rhs->Priority;
    });

    ActiveClientId_ = 0;
    ActiveClient_ = UnderlyingClients_[ActiveClientId_]->Client;

    Executor_->Start();
}

void TClient::CheckClustersHealth()
{
    TCheckClusterLivenessOptions options;
    options.CheckCypressRoot = true;
    options.CheckTabletCellBundle = Config_->BundleName;

    auto activeClientId = ActiveClientId_.load();
    std::optional<size_t> betterClientId;

    std::vector<TFuture<void>> checks;
    checks.reserve(UnderlyingClients_.size());

    for (const auto& clientDescription : UnderlyingClients_) {
        checks.emplace_back(clientDescription->Client->CheckClusterLiveness(options));
    }

    for (size_t id = 0; id < checks.size(); ++id) {
        const auto& check = checks[id];
        bool hasErrors = !NConcurrency::WaitFor(check).IsOK();
        UnderlyingClients_[id]->HasErrors = hasErrors;
        if (!betterClientId && !hasErrors && id < activeClientId) {
            betterClientId = id;
        }
    }

    if (betterClientId.has_value() && ActiveClientId_ == activeClientId) {
        auto newClientId = *betterClientId;
        auto guard = NThreading::WriterGuard(Lock_);
        ActiveClient_ = UnderlyingClients_[newClientId]->Client;
        ActiveClientId_ = newClientId;
        return;
    }

    // If active cluster is not healthy, try changing it.
    if (UnderlyingClients_[activeClientId]->HasErrors) {
        auto guard = NThreading::WriterGuard(Lock_);
        // Check that active client wasn't changed.
        if (ActiveClientId_ == activeClientId && UnderlyingClients_[activeClientId]->HasErrors) {
            UpdateActiveClient();
        }
    }
}

template <typename TResultType, typename TRetryCallback>
auto TClient::CreateResultHandler(int clientId, int retryAttemptsCount, TRetryCallback callback)
{
    return [
        this,
        this_ = MakeStrong(this),
        clientId,
        retryAttemptsCount,
        callback = std::move(callback)
    ] (const TErrorOr<TResultType>& resultOrError)
    {
        if (!resultOrError.IsOK()) {
            HandleError(resultOrError, clientId);
            if (retryAttemptsCount > 1) {
                return callback(retryAttemptsCount - 1);
            }
        }
        return MakeFuture(resultOrError);
    };
}

TFuture<ITransactionPtr> TClient::StartTransaction(
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    return DoStartTransaction(Config_->ClusterRetryAttempts, type, options);
}

TFuture<ITransactionPtr> TClient::DoStartTransaction(
    int retryAttemptsCount,
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    auto [client, clientId] = GetActiveClient();

    return client->StartTransaction(type, options)
        .Apply(BIND([this, this_ = MakeStrong(this), clientId = clientId, retryAttemptsCount, type, &options]
            (const TErrorOr<ITransactionPtr>& transactionOrError)
        {
            if (!transactionOrError.IsOK()) {
                HandleError(transactionOrError, clientId);
                if (retryAttemptsCount > 1) {
                    return DoStartTransaction(retryAttemptsCount - 1, type, options);
                }
                return MakeFuture(transactionOrError);
            }
            return MakeFuture(ITransactionPtr{
                New<TTransaction>(std::move(this_), clientId, transactionOrError.Value())
            });
        }
    ));
}

#define CLIENT_METHOD_IMPL(ResultType, MethodName, Args, CaptureList)                                                   \
TFuture<ResultType> TClient::MethodName(Y_METHOD_USED_ARGS_DECLARATION(Args))                                       \
{                                                                                                                       \
    return Do##MethodName(Config_->ClusterRetryAttempts, Y_PASS_METHOD_USED_ARGS(Args));                                \
}                                                                                                                       \
                                                                                                                        \
TFuture<ResultType> TClient::Do##MethodName(int retryAttempsCount, Y_METHOD_USED_ARGS_DECLARATION(Args))            \
{                                                                                                                       \
    auto [client, clientId] = GetActiveClient();                                                                        \
                                                                                                                        \
    return client->MethodName(Y_PASS_METHOD_USED_ARGS(Args)).Apply(BIND(CreateResultHandler<ResultType>(                \
        clientId, retryAttempsCount, [this, Y_METHOD_UNUSED_ARGS_DECLARATION(CaptureList)] (int attemptsCount) {        \
            return Do##MethodName(attemptsCount, Y_PASS_METHOD_USED_ARGS(Args));                                        \
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
CLIENT_METHOD_IMPL(std::vector<TTabletInfo>, GetTabletInfos, (const NYPath::TYPath&, const std::vector<int>&, const TGetTabletInfosOptions&), (&a1, &a2, &a3));

const NTabletClient::ITableMountCachePtr& TClient::GetTableMountCache()
{
    auto [client, _] = GetActiveClient();
    return client->GetTableMountCache();
}

const NTransactionClient::ITimestampProviderPtr& TClient::GetTimestampProvider()
{
    auto [client, _] = GetActiveClient();
    return client->GetTimestampProvider();
}

ITransactionPtr TClient::AttachTransaction(
    NTransactionClient::TTransactionId transactionId,
    const TTransactionAttachOptions& options)
{
    auto transactionClusterTag = NObjectClient::CellTagFromId(transactionId);
    for (const auto& clientDescription : UnderlyingClients_) {
        const auto& client = clientDescription->Client;
        auto clientClusterTag = client->GetConnection()->GetClusterTag();
        if (clientClusterTag == transactionClusterTag) {
            return client->AttachTransaction(transactionId, options);
        }
    }
    THROW_ERROR_EXCEPTION("There is no corresponding client for the transaction (TransactionId: %v)", transactionId);
}

void TClient::HandleError(const TErrorOr<void>& /*error*/, size_t clientId)
{
    // TODO(nadya73): check error and do nothing if it's not fatal error

    UnderlyingClients_[clientId]->HasErrors = true;
    if (ActiveClientId_ != clientId) {
        return;
    }

    auto guard = WriterGuard(Lock_);
    if (ActiveClientId_ != clientId) {
        return;
    }

    UpdateActiveClient();
}

void TClient::UpdateActiveClient()
{
    VERIFY_WRITER_SPINLOCK_AFFINITY(Lock_);

    auto activeClientId = ActiveClientId_.load();

    for (size_t id = 0; id < UnderlyingClients_.size(); ++id) {
        const auto& clientDescription = UnderlyingClients_[id];
        if (!clientDescription->HasErrors) {
            if (activeClientId != id) {
                YT_LOG_DEBUG("Active client was changed (PreviousClientId: %v, NewClientId: %v)",
                    activeClientId,
                    id);
            }

            ActiveClient_ = clientDescription->Client;
            ActiveClientId_ = id;
            break;
        }
    }
}

TClient::TActiveClientInfo TClient::GetActiveClient()
{
    auto guard = ReaderGuard(Lock_);
    YT_LOG_DEBUG("Request will be send to the active client (ClientId: %v)",
        ActiveClientId_.load());
    return {ActiveClient_, ActiveClientId_.load()};
}

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace

NApi::IClientPtr CreateClient(const std::vector<NApi::IClientPtr>& clients, TFederationConfigPtr config)
{
    return New<TClient>(clients, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NClient::NFederated
