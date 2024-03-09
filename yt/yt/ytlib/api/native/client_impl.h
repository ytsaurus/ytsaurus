#pragma once

#include "client.h"
#include "private.h"

#include <yt/yt/ytlib/chaos_client/alien_cell.h>

#include <yt/yt/ytlib/chunk_pools/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>
#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/yt/ytlib/query_client/executor.h>
#include <yt/yt/ytlib/query_client/tracked_memory_chunk_provider.h>
#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_service_proxy.h>

#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/table_replica_ypath.h>
#include <yt/yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/bundle_controller_client/bundle_controller_settings.h>

#include <yt/yt/flow/lib/client/controller/controller_service_proxy.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/lazy_ptr.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClient)

class TClient
    : public IClient
    , public NApi::IInternalClient
    , public NApi::TClusterAwareClientBase
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options,
        INodeMemoryTrackerPtr memoryTracker);

    NApi::IConnectionPtr GetConnection() override;
    const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    const NChaosClient::IReplicationCardCachePtr& GetReplicationCardCache() override;
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;
    const IConnectionPtr& GetNativeConnection() override;
    const NTransactionClient::TTransactionManagerPtr& GetTransactionManager() override;
    const TClientCounters& GetCounters() const override;
    NQueryClient::IFunctionRegistryPtr GetFunctionRegistry() override;
    NQueryClient::TFunctionImplCachePtr GetFunctionImplCache() override;

    const TClientOptions& GetOptions() override;

    NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) override;
    NRpc::IChannelPtr GetCypressChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) override;
    NRpc::IChannelPtr GetCellChannelOrThrow(NObjectClient::TCellId cellId) override;
    NRpc::IChannelPtr GetSchedulerChannel() override;
    const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() override;

    void Terminate() override;

    bool DoesOperationsArchiveExist() override;

    // Transactions
    TFuture<ITransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options) override;
    ITransactionPtr AttachNativeTransaction(
        NCypressClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options) override;
    TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options) override;
    NApi::ITransactionPtr AttachTransaction(
        NCypressClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options) override;

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_OVERLOADED_METHOD(returnType, method, doMethod, signature, args) \
private: \
    returnType doMethod signature; \
\
public: \
    virtual TFuture<returnType> method signature override \
    { \
        return Execute( \
            TStringBuf(#method), \
            options, \
            BIND( \
                &TClient::doMethod, \
                Unretained(this), \
                DROP_BRACES args)); \
    }

#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    IMPLEMENT_OVERLOADED_METHOD(returnType, method, Do##method, signature, args)

    IMPLEMENT_METHOD(TUnversionedLookupRowsResult, LookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(TVersionedLookupRowsResult, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(std::vector<TUnversionedLookupRowsResult>, MultiLookupRows, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options),
        (subrequests, options))
    IMPLEMENT_METHOD(TSelectRowsResult, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))
    IMPLEMENT_METHOD(TPullRowsResult, PullRows, (
        const NYPath::TYPath& path,
        const TPullRowsOptions& options),
        (path, options))
    IMPLEMENT_METHOD(NYson::TYsonString, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options),
        (query, options))
    IMPLEMENT_OVERLOADED_METHOD(std::vector<NTabletClient::TTableReplicaId>, GetInSyncReplicas, DoGetInSyncReplicasWithKeys, (
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (path, nameTable, keys, options))
    IMPLEMENT_OVERLOADED_METHOD(std::vector<NTabletClient::TTableReplicaId>, GetInSyncReplicas, DoGetInSyncReplicasWithoutKeys, (
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options),
        (path, options))
    IMPLEMENT_METHOD(std::vector<TTabletInfo>, GetTabletInfos, (
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options),
        (path, tabletIndexes, options))
    IMPLEMENT_METHOD(TGetTabletErrorsResult, GetTabletErrors, (
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, MountTable, (
        const NYPath::TYPath& path,
        const TMountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, UnmountTable, (
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, RemountTable, (
        const NYPath::TYPath& path,
        const TRemountTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, FreezeTable, (
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, UnfreezeTable, (
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options),
        (path, options))
    IMPLEMENT_OVERLOADED_METHOD(void, ReshardTable, DoReshardTableWithPivotKeys, (
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options),
        (path, pivotKeys, options))
    IMPLEMENT_OVERLOADED_METHOD(void, ReshardTable, DoReshardTableWithTabletCount, (
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options),
        (path, tabletCount, options))
    IMPLEMENT_METHOD(std::vector<NTabletClient::TTabletActionId>, ReshardTableAutomatic, (
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, AlterTable, (
        const NYPath::TYPath& path,
        const TAlterTableOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, TrimTable, (
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options),
        (path, tabletIndex, trimmedRowCount, options))
    IMPLEMENT_METHOD(void, AlterTableReplica, (
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options),
        (replicaId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, GetTablePivotKeys, (
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, CreateTableBackup, (
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options),
        (manifest, options))
    IMPLEMENT_METHOD(void, RestoreTableBackup, (
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options),
        (manifest, options))
    IMPLEMENT_METHOD(std::vector<NTabletClient::TTabletActionId>, BalanceTabletCells, (
        const TString& tabletCellBundle,
        const std::vector< NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options),
        (tabletCellBundle, movableTables, options))
    IMPLEMENT_METHOD(NChaosClient::TReplicationCardPtr, GetReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options = {}),
        (replicationCardId, options))
    IMPLEMENT_METHOD(void, UpdateChaosTableReplicaProgress, (
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options = {}),
        (replicaId, options))
    IMPLEMENT_METHOD(void, AlterReplicationCard, (
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options = {}),
        (replicationCardId, options))

    IMPLEMENT_METHOD(NQueueClient::IQueueRowsetPtr, PullQueue, (
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}),
        (queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    IMPLEMENT_METHOD(NQueueClient::IQueueRowsetPtr, PullQueueUnauthenticated, (
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}),
        (queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    IMPLEMENT_METHOD(NQueueClient::IQueueRowsetPtr, PullConsumer, (
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        std::optional<i64> offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullConsumerOptions& options = {}),
        (consumerPath, queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    IMPLEMENT_METHOD(void, RegisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        bool vital,
        const TRegisterQueueConsumerOptions& options = {}),
        (queuePath, consumerPath, vital, options))

    IMPLEMENT_METHOD(void, UnregisterQueueConsumer, (
        const NYPath::TRichYPath& queuePath,
        const NYPath::TRichYPath& consumerPath,
        const TUnregisterQueueConsumerOptions& options = {}),
        (queuePath, consumerPath, options))

    IMPLEMENT_METHOD(std::vector<TListQueueConsumerRegistrationsResult>, ListQueueConsumerRegistrations, (
        const std::optional<NYPath::TRichYPath>& queuePath,
        const std::optional<NYPath::TRichYPath>& consumerPath,
        const TListQueueConsumerRegistrationsOptions& options = {}),
        (queuePath, consumerPath, options))

    IMPLEMENT_METHOD(NQueryTrackerClient::TQueryId, StartQuery, (
        NQueryTrackerClient::EQueryEngine engine,
        const TString& query,
        const TStartQueryOptions& options = {}),
        (engine, query, options))
    IMPLEMENT_METHOD(void, AbortQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TAbortQueryOptions& options = {}),
        (queryId, options))
    IMPLEMENT_METHOD(TQueryResult, GetQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex = 0,
        const TGetQueryResultOptions& options = {}),
        (queryId, resultIndex, options))
    IMPLEMENT_METHOD(IUnversionedRowsetPtr, ReadQueryResult, (
        NQueryTrackerClient::TQueryId queryId,
        i64 resultIndex = 0,
        const TReadQueryResultOptions& options = {}),
        (queryId, resultIndex, options))
    IMPLEMENT_METHOD(TQuery, GetQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TGetQueryOptions& options = {}),
        (queryId, options))
    IMPLEMENT_METHOD(TListQueriesResult, ListQueries, (
        const TListQueriesOptions& options = {}),
        (options))
    IMPLEMENT_METHOD(void, AlterQuery, (
        NQueryTrackerClient::TQueryId queryId,
        const TAlterQueryOptions& options = {}),
        (queryId, options))
    IMPLEMENT_METHOD(TGetQueryTrackerInfoResult, GetQueryTrackerInfo, (
        const TGetQueryTrackerInfoOptions& options = {}),
        (options))

    IMPLEMENT_METHOD(NYson::TYsonString, GetNode, (
        const NYPath::TYPath& path,
        const TGetNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(void, SetNode, (
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options),
        (path, value, options))
    IMPLEMENT_METHOD(void, MultisetAttributesNode, (
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options),
        (path, attributes, options))
    IMPLEMENT_METHOD(void, RemoveNode, (
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(NYson::TYsonString, ListNode, (
        const NYPath::TYPath& path,
        const TListNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(NCypressClient::TNodeId, CreateNode, (
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options),
        (path, type, options))
    IMPLEMENT_METHOD(TLockNodeResult, LockNode, (
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options),
        (path, mode, options))
    IMPLEMENT_METHOD(void, UnlockNode, (
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(NCypressClient::TNodeId, CopyNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(NCypressClient::TNodeId, MoveNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(NCypressClient::TNodeId, LinkNode, (
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options),
        (srcPath, dstPath, options))
    IMPLEMENT_METHOD(void, ConcatenateNodes, (
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options),
        (srcPaths, dstPath, options))
    IMPLEMENT_METHOD(void, ExternalizeNode, (
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options),
        (path, cellTag, options))
    IMPLEMENT_METHOD(void, InternalizeNode, (
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options),
        (path, options))
    IMPLEMENT_METHOD(bool, NodeExists, (
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options),
        (path, options))
    IMPLEMENT_METHOD(NObjectClient::TObjectId, CreateObject, (
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options),
        (type, options))

    TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options) override;
    IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options) override;

    IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options) override;
    IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options) override;

    TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) override;
    TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;

    TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options) override;

    IMPLEMENT_METHOD(std::vector<NTableClient::TColumnarStatistics>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options),
        (paths, options))

    IMPLEMENT_METHOD(TDisableChunkLocationsResult, DisableChunkLocations, (
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TDisableChunkLocationsOptions& options),
        (nodeAddress, locationUuids, options))

    IMPLEMENT_METHOD(TDestroyChunkLocationsResult, DestroyChunkLocations, (
        const TString& nodeAddress,
        bool recoverUnlinkedDisks,
        const std::vector<TGuid>& locationUuids,
        const TDestroyChunkLocationsOptions& options),
        (nodeAddress, recoverUnlinkedDisks, locationUuids, options))

    IMPLEMENT_METHOD(TResurrectChunkLocationsResult, ResurrectChunkLocations, (
        const TString& nodeAddress,
        const std::vector<TGuid>& locationUuids,
        const TResurrectChunkLocationsOptions& options),
        (nodeAddress, locationUuids, options))

    IMPLEMENT_METHOD(TRequestRestartResult, RequestRestart, (
        const TString& nodeAddress,
        const TRequestRestartOptions& options),
        (nodeAddress, options))

    IMPLEMENT_METHOD(TMultiTablePartitions, PartitionTables, (
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options),
        (paths, options))

    IMPLEMENT_METHOD(void, TruncateJournal, (
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options),
        (path, rowCount, options))

    IMPLEMENT_METHOD(TGetFileFromCacheResult, GetFileFromCache, (
        const TString& md5,
        const TGetFileFromCacheOptions& options),
        (md5, options))

    IMPLEMENT_METHOD(TPutFileToCacheResult, PutFileToCache, (
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options),
        (path, expectedMD5, options))

    IMPLEMENT_METHOD(void, AddMember, (
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(void, RemoveMember, (
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options),
        (group, member, options))
    IMPLEMENT_METHOD(TCheckPermissionResponse, CheckPermission, (
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options),
        (user, path, permission, options))
    IMPLEMENT_METHOD(TCheckPermissionByAclResult, CheckPermissionByAcl, (
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options),
        (user, permission, acl, options))
    IMPLEMENT_METHOD(void, TransferAccountResources, (
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options),
        (srcAccount, dstAccount, resourceDelta, options))

    IMPLEMENT_METHOD(void, TransferPoolResources, (
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options),
        (srcPool, dstPool, poolTree, resourceDelta, options))

    IMPLEMENT_METHOD(NScheduler::TOperationId, StartOperation, (
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options),
        (type, spec, options))
    IMPLEMENT_METHOD(void, AbortOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(void, SuspendOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(void, ResumeOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(void, CompleteOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(void, UpdateOperationParameters, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options),
        (operationIdOrAlias, parameters, options))
    IMPLEMENT_METHOD(TOperation, GetOperation, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(void, DumpJobContext, (
        NScheduler::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options),
        (jobId, path, options))
    IMPLEMENT_METHOD(NConcurrency::IAsyncZeroCopyInputStreamPtr, GetJobInput, (
        NScheduler::TJobId jobId,
        const TGetJobInputOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, GetJobInputPaths, (
        NScheduler::TJobId jobId,
        const TGetJobInputPathsOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, GetJobSpec, (
        NScheduler::TJobId jobId,
        const TGetJobSpecOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobStderr, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobStderrOptions& options),
        (operationIdOrAlias, jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobFailContext, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobFailContextOptions& options),
        (operationIdOrAlias, jobId, options))
    // XXX(levysotsky): The counters may be incorrect if |options.IncludeArchive| is |true|
    // and an operation is in both Cypress and archive.
    // XXX(levysotsky): The "failed_jobs_count" counter is incorrect if corresponding failed operations
    // are in archive and outside of queried range.
    IMPLEMENT_METHOD(TListOperationsResult, ListOperations, (
        const TListOperationsOptions& options),
        (options))
    IMPLEMENT_METHOD(TListJobsResult, ListJobs, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options),
        (operationIdOrAlias, options))
    IMPLEMENT_METHOD(NYson::TYsonString, GetJob, (
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobOptions& options),
        (operationIdOrAlias, jobId, options))
    IMPLEMENT_METHOD(void, AbandonJob, (
        NScheduler::TJobId jobId,
        const TAbandonJobOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(TPollJobShellResponse, PollJobShell, (
        NScheduler::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options),
        (jobId, shellName, parameters, options))
    IMPLEMENT_METHOD(void, AbortJob, (
        NScheduler::TJobId jobId,
        const TAbortJobOptions& options),
        (jobId, options))

    IMPLEMENT_METHOD(TClusterMeta, GetClusterMeta, (
        const TGetClusterMetaOptions& options),
        (options))
    IMPLEMENT_METHOD(void, CheckClusterLiveness, (
        const TCheckClusterLivenessOptions& options),
        (options))

    IMPLEMENT_METHOD(int, BuildSnapshot, (
        const TBuildSnapshotOptions& options),
        (options))
    IMPLEMENT_METHOD(TCellIdToSnapshotIdMap, BuildMasterSnapshots, (
        const TBuildMasterSnapshotsOptions& options),
        (options))
    IMPLEMENT_METHOD(TCellIdToSequenceNumberMap, GetMasterConsistentState, (
        const TGetMasterConsistentStateOptions& options),
        (options))
    IMPLEMENT_METHOD(void, ExitReadOnly, (
        NObjectClient::TCellId cellId,
        const TExitReadOnlyOptions& options),
        (cellId, options))
    IMPLEMENT_METHOD(void, MasterExitReadOnly, (
        const TMasterExitReadOnlyOptions& options),
        (options))
    IMPLEMENT_METHOD(void, DiscombobulateNonvotingPeers, (
        NObjectClient::TCellId cellId,
        const TDiscombobulateNonvotingPeersOptions& options),
        (cellId, options))
    IMPLEMENT_METHOD(void, SwitchLeader, (
        NObjectClient::TCellId cellId,
        const TString& newLeaderAddress,
        const TSwitchLeaderOptions& options),
        (cellId, newLeaderAddress, options))
    IMPLEMENT_METHOD(void, ResetStateHash, (
        NObjectClient::TCellId cellId,
        const TResetStateHashOptions& options),
        (cellId, options))
    IMPLEMENT_METHOD(void, GCCollect, (
        const TGCCollectOptions& options),
        (options))
    IMPLEMENT_METHOD(void, KillProcess, (
        const TString& address,
        const TKillProcessOptions& options),
        (address, options))
    IMPLEMENT_METHOD(TString, WriteCoreDump, (
        const TString& address,
        const TWriteCoreDumpOptions& options),
        (address, options))
    IMPLEMENT_METHOD(TGuid, WriteLogBarrier, (
        const TString& address,
        const TWriteLogBarrierOptions& options),
        (address, options))
    IMPLEMENT_METHOD(TString, WriteOperationControllerCoreDump, (
        NScheduler::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(void, HealExecNode, (
        const TString& address,
        const THealExecNodeOptions& options),
        (address, options))
    IMPLEMENT_METHOD(void, SuspendCoordinator, (
        NObjectClient::TCellId coordinatorCellId,
        const TSuspendCoordinatorOptions& options),
        (coordinatorCellId, options))
    IMPLEMENT_METHOD(void, ResumeCoordinator, (
        NObjectClient::TCellId coordinatorCellId,
        const TResumeCoordinatorOptions& options),
        (coordinatorCellId, options))
    IMPLEMENT_METHOD(void, MigrateReplicationCards, (
        NObjectClient::TCellId chaosCellId,
        const TMigrateReplicationCardsOptions& options),
        (chaosCellId, options))
    IMPLEMENT_METHOD(void, SuspendChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendChaosCellsOptions& options),
        (cellIds, options))
    IMPLEMENT_METHOD(void, ResumeChaosCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeChaosCellsOptions& options),
        (cellIds, options))
    IMPLEMENT_METHOD(void, SuspendTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TSuspendTabletCellsOptions& options),
        (cellIds, options))
    IMPLEMENT_METHOD(void, ResumeTabletCells, (
        const std::vector<NObjectClient::TCellId>& cellIds,
        const TResumeTabletCellsOptions& options),
        (cellIds, options))
    IMPLEMENT_METHOD(TMaintenanceIdPerTarget, AddMaintenance, (
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment,
        const TAddMaintenanceOptions& options),
        (component, address, type, comment, options))
    IMPLEMENT_METHOD(TMaintenanceCountsPerTarget, RemoveMaintenance, (
        EMaintenanceComponent component,
        const TString& address,
        const TMaintenanceFilter& target,
        const TRemoveMaintenanceOptions& options),
        (component, address, target, options))


    IMPLEMENT_METHOD(TSyncAlienCellsResult, SyncAlienCells, (
        const std::vector<NChaosClient::TAlienCellDescriptorLite>& alienCellDescriptors,
        const TSyncAlienCellOptions& options),
        (alienCellDescriptors, options))

    IMPLEMENT_METHOD(std::vector<TSharedRef>, ReadHunks, (
        const std::vector<THunkDescriptor>& descriptors,
        const TReadHunksOptions& options),
        (descriptors, options))
    IMPLEMENT_METHOD(std::vector<THunkDescriptor>, WriteHunks, (
        const NYTree::TYPath& path,
        int tabletIndex,
        const std::vector<TSharedRef>& payloads,
        const TWriteHunksOptions& options),
        (path, tabletIndex, payloads, options))
    IMPLEMENT_METHOD(void, LockHunkStore, (
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TLockHunkStoreOptions& options),
        (path, tabletIndex, storeId, lockerTabletId, options))
    IMPLEMENT_METHOD(void, UnlockHunkStore, (
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TUnlockHunkStoreOptions& options),
        (path, tabletIndex, storeId, lockerTabletId, options))
    IMPLEMENT_METHOD(std::vector<TErrorOr<i64>>, GetOrderedTabletSafeTrimRowCount, (
        const std::vector<TGetOrderedTabletSafeTrimRowCountRequest>& requests,
        const TGetOrderedTabletSafeTrimRowCountOptions& options),
        (requests, options))

    IMPLEMENT_METHOD(void, IssueLease, (
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        const TIssueLeaseOptions& options),
        (cellId, leaseId, options))
    IMPLEMENT_METHOD(void, RevokeLease, (
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool force,
        const TRevokeLeaseOptions& options),
        (cellId, leaseId, force, options))

    IMPLEMENT_METHOD(void, ReferenceLease, (
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool persistent,
        bool force,
        const TReferenceLeaseOptions& options),
        (cellId, leaseId, persistent, force, options))
    IMPLEMENT_METHOD(void, UnreferenceLease, (
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool persistent,
        const TUnreferenceLeaseOptions& options),
        (cellId, leaseId, persistent, options))

    IMPLEMENT_METHOD(void, SetUserPassword, (
        const TString& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options),
        (user, currentPasswordSha256, newPasswordSha256, options))
    IMPLEMENT_METHOD(TIssueTokenResult, IssueToken, (
        const TString& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options),
        (user, passwordSha256, options))
    IMPLEMENT_METHOD(TIssueTokenResult, IssueTemporaryToken, (
        const TString& user,
        const NYTree::IAttributeDictionaryPtr& attributes,
        const TIssueTemporaryTokenOptions& options),
        (user, attributes, options))
    IMPLEMENT_METHOD(void, RefreshTemporaryToken, (
        const TString& user,
        const TString& token,
        const TRefreshTemporaryTokenOptions& options),
        (user, token, options))
    IMPLEMENT_METHOD(void, RevokeToken, (
        const TString& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options),
        (user, passwordSha256, tokenSha256, options))
    IMPLEMENT_METHOD(TListUserTokensResult, ListUserTokens, (
        const TString& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options),
        (user, passwordSha256, options))

    IMPLEMENT_METHOD(NBundleControllerClient::TBundleConfigDescriptorPtr, GetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TGetBundleConfigOptions& options),
        (bundleName, options))
    IMPLEMENT_METHOD(void, SetBundleConfig, (
        const TString& bundleName,
        const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
        const NBundleControllerClient::TSetBundleConfigOptions& options),
        (bundleName, bundleConfig, options))

    IMPLEMENT_METHOD(TGetPipelineSpecResult, GetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineSpecOptions& options),
        (pipelinePath, options))
    IMPLEMENT_METHOD(TSetPipelineSpecResult, SetPipelineSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineSpecOptions& options),
        (pipelinePath, spec, options))
    IMPLEMENT_METHOD(TGetPipelineDynamicSpecResult, GetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineDynamicSpecOptions& options),
        (pipelinePath, options))
    IMPLEMENT_METHOD(TSetPipelineDynamicSpecResult, SetPipelineDynamicSpec, (
        const NYPath::TYPath& pipelinePath,
        const NYson::TYsonString& spec,
        const TSetPipelineDynamicSpecOptions& options),
        (pipelinePath, spec, options))
    IMPLEMENT_METHOD(void, StartPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStartPipelineOptions& options),
        (pipelinePath, options))
    IMPLEMENT_METHOD(void, StopPipeline, (
        const NYPath::TYPath& pipelinePath,
        const TStopPipelineOptions& options),
        (pipelinePath, options))
    IMPLEMENT_METHOD(void, PausePipeline, (
        const NYPath::TYPath& pipelinePath,
        const TPausePipelineOptions& options),
        (pipelinePath, options))
    IMPLEMENT_METHOD(TPipelineStatus, GetPipelineStatus, (
        const NYPath::TYPath& pipelinePath,
        const TGetPipelineStatusOptions& options),
        (pipelinePath, options))

#undef DROP_BRACES
#undef IMPLEMENT_METHOD

private:
    friend class TTransaction;
    friend class TNodeConcatenator;
    friend class TReplicatedTableReplicaTypeHandler;
    friend class TReplicationCardTypeHandler;
    friend class TReplicationCardCollocationTypeHandler;
    friend class TChaosTableReplicaTypeHandler;
    friend class TSecondaryIndexTypeHandler;
    friend class TTableCollocationTypeHandler;
    friend class TTabletActionTypeHandler;
    friend class TChaosReplicatedTableTypeHandler;
    friend class TDefaultTypeHandler;

    const IConnectionPtr Connection_;
    const TClientOptions Options_;

    const NLogging::TLogger Logger;

    const NProfiling::TProfiler Profiler_;
    const TClientCounters Counters_;

    const std::vector<ITypeHandlerPtr> TypeHandlers_;

    TEnumIndexedArray<EMasterChannelKind, THashMap<NObjectClient::TCellTag, NRpc::IChannelPtr>> MasterChannels_;
    TEnumIndexedArray<EMasterChannelKind, THashMap<NObjectClient::TCellTag, NRpc::IChannelPtr>> CypressChannels_;
    NRpc::IChannelPtr SchedulerChannel_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, OperationsArchiveClientLock_);
    IClientPtr OperationsArchiveClient_;
    NNodeTrackerClient::INodeChannelFactoryPtr ChannelFactory_;
    NTransactionClient::TTransactionManagerPtr TransactionManager_;
    TLazyIntrusivePtr<NQueryClient::TFunctionImplCache> FunctionImplCache_;
    TLazyIntrusivePtr<NQueryClient::IFunctionRegistry> FunctionRegistry_;
    std::unique_ptr<NScheduler::TOperationServiceProxy> SchedulerOperationProxy_;
    std::unique_ptr<NBundleController::TBundleControllerServiceProxy> BundleControllerProxy_;
    const ITypedNodeMemoryTrackerPtr LookupMemoryTracker_;
    const ITypedNodeMemoryTrackerPtr QueryMemoryTracker_;
    const NQueryClient::TMemoryProviderMapByTagPtr MemoryProvider_ = New<NQueryClient::TMemoryProviderMapByTag>();

    struct TReplicaClient final
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
        NApi::NNative::IClientPtr Client;
        TFuture<NApi::NNative::IClientPtr> AsyncClient;

        bool IsTerminated() const;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ReplicaClientsLock_);
    THashMap<TString, TIntrusivePtr<TReplicaClient>> ReplicaClients_;

    const IClientPtr& GetOperationsArchiveClient();

    template <class T>
    TFuture<T> Execute(
        TStringBuf commandName,
        const TTimeoutOptions& options,
        TCallback<T()> callback);

    template <class T>
    auto CallAndRetryIfMetadataCacheIsInconsistent(
        const TDetailedProfilingInfoPtr& profilingInfo,
        T&& callback) -> decltype(callback());

    void SetMutationId(
        const NRpc::IClientRequestPtr& request,
        const TMutatingOptions& options);
    NTransactionClient::TTransactionId GetTransactionId(
        const TTransactionalOptions& options,
        bool allowNullTransaction);
    void SetTransactionId(
        const NRpc::IClientRequestPtr& request,
        const TTransactionalOptions& options,
        bool allowNullTransaction);
    void SetSuppressAccessTracking(
        const NRpc::IClientRequestPtr& request,
        const TSuppressableAccessTrackingOptions& commandOptions);
    void SetCachingHeader(
        const NRpc::IClientRequestPtr& request,
        const TMasterReadOptions& options);
    void SetBalancingHeader(
        const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& request,
        const TMasterReadOptions& options);

    NObjectClient::TObjectServiceProxy CreateObjectServiceReadProxy(
        const TMasterReadOptions& options,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel);
    NObjectClient::TObjectServiceProxy CreateObjectServiceWriteProxy(
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel);

    template <class TProxy>
    TProxy CreateWriteProxy(NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel);
    NRpc::IChannelPtr GetReadCellChannelOrThrow(NObjectClient::TCellId cellId);
    NRpc::IChannelPtr GetReadCellChannelOrThrow(const NHiveClient::TCellDescriptorPtr& cellDescriptor);
    NRpc::IChannelPtr GetHydraAdminChannelOrThrow(NObjectClient::TCellId cellId);
    NHiveClient::TCellDescriptorPtr GetCellDescriptorOrThrow(NObjectClient::TCellId cellId);
    std::vector<TString> GetCellAddressesOrThrow(NObjectClient::TCellId cellId);

    NApi::IClientPtr CreateRootClient();

    void ValidateSuperuserPermissions();
    void ValidatePermissionsWithAcn(
        NSecurityClient::EAccessControlObject accessControlObject,
        NYTree::EPermission permission);

    NObjectClient::TObjectId CreateObjectImpl(
        NObjectClient::EObjectType type,
        NObjectClient::TCellTag cellTag,
        const NYTree::IAttributeDictionary& attributes,
        const TCreateObjectOptions& options);
    NCypressClient::TNodeId CreateNodeImpl(
        NCypressClient::EObjectType type,
        const NYPath::TYPath& path,
        const NYTree::IAttributeDictionary& attributes,
        const TCreateNodeOptions& options);

    using TEncoderWithMapping = std::function<std::vector<TSharedRef>(
        const NTableClient::TColumnFilter&,
        const std::vector<NTableClient::TUnversionedRow>&)>;
    TEncoderWithMapping GetLookupRowsEncoder() const;

    using TDecoderWithMapping = std::function<NTableClient::TTypeErasedRow(
        const NTableClient::TSchemaData&,
        NTableClient::IWireProtocolReader*)>;
    TDecoderWithMapping GetLookupRowsDecoder() const;

    struct TReplicaFallbackInfo
    {
        NApi::IClientPtr Client;
        NYPath::TYPath Path;
        NTabletClient::TTableReplicaId ReplicaId;
        NTableClient::TTableSchemaPtr OriginalTableSchema;
    };

    TReplicaFallbackInfo GetReplicaFallbackInfo(
        const TTableReplicaInfoPtrList& replicas);

    template <class TResult>
    using TReplicaFallbackHandler = std::function<TFuture<TResult>(
        const TReplicaFallbackInfo& replicaFallbackInfo)>;

    template <class IRowset, class TRow>
    TLookupRowsResult<IRowset> DoLookupRowsOnce(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptionsBase& options,
        const std::optional<TString>& retentionConfig,
        TEncoderWithMapping encoderWithMapping,
        TDecoderWithMapping decoderWithMapping,
        TReplicaFallbackHandler<TLookupRowsResult<IRowset>> replicaFallbackHandler);

    static NTabletClient::TTableReplicaInfoPtr PickRandomReplica(
        const TTableReplicaInfoPtrList& replicas);
    static TString PickRandomCluster(
        const std::vector<TString>& clusterNames);

    TTableReplicaInfoPtrList OnTabletInfosReceived(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        int totalTabletCount,
        std::optional<TInstant> cachedSyncReplicasAt,
        THashMap<NTabletClient::TTableReplicaId, int> replicaIdToCount,
        const std::vector<NQueryClient::TQueryServiceProxy::TRspGetTabletInfoPtr>& responses);

    std::pair<std::vector<NTabletClient::TTableMountInfoPtr>, std::vector<TTableReplicaInfoPtrList>> PrepareInSyncReplicaCandidates(
        const TTabletReadOptions& options,
        const std::vector<NTabletClient::TTableMountInfoPtr>& tableInfos);

    std::pair<TString, TSelectRowsOptions::TExpectedTableSchemas> PickInSyncClusterAndPatchQuery(
        const std::vector<NTabletClient::TTableMountInfoPtr>& tableInfos,
        const std::vector<TTableReplicaInfoPtrList>& candidates,
        NQueryClient::NAst::TQuery* query);

    NApi::NNative::IConnectionPtr GetReplicaConnectionOrThrow(const TString& clusterName);
    NApi::IClientPtr GetOrCreateReplicaClient(const TString& clusterName);

    TSelectRowsResult DoSelectRowsOnce(
        const TString& queryString,
        const TSelectRowsOptions& options);

    static bool IsReplicaInSync(
        const NQueryClient::NProto::TReplicaInfo& replicaInfo,
        const NQueryClient::NProto::TTabletInfo& tabletInfo,
        NTransactionClient::TTimestamp timestamp);

    std::vector<NTabletClient::TTableReplicaId> DoGetInSyncReplicas(
        const NYPath::TYPath& path,
        bool allKeys,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options);

    //
    // Dynamic tables
    //

    std::vector<TTabletInfo> GetTabletInfosByTabletIds(
        const NYPath::TYPath& path,
        const std::vector<NTabletClient::TTabletId>& tabletIds,
        const TGetTabletInfosOptions& options);

    std::vector<TTabletInfo> GetTabletInfosImpl(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options);

    template <class TReq>
    void ExecuteTabletServiceRequest(
        const NYPath::TYPath& path,
        TStringBuf action,
        TReq* req);

    NTabletClient::NProto::TReqReshard MakeReshardRequest(
        const TReshardTableOptions& options);
    NTableClient::TTableYPathProxy::TReqReshardPtr MakeYPathReshardRequest(
        const NYPath::TYPath& path,
        const TReshardTableOptions& options);

    std::vector<NTableClient::TLegacyOwningKey> PickUniformPivotKeys(
        const NYPath::TYPath& path,
        int tabletCount);

    friend class TClusterBackupSession;
    friend class TBackupSession;

    TSharedRange<NTableClient::TUnversionedRow> PermuteAndEvaluateKeys(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys);

    std::vector<NTabletClient::TTableReplicaId> GetReplicatedTableInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        bool allKeys,
        const TGetInSyncReplicasOptions& options);

    std::vector<NTabletClient::TTableReplicaId> GetChaosTableInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const NChaosClient::TReplicationCardPtr& replicationCard,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        bool allKeys,
        NTransactionClient::TTimestamp userTimestamp = NTransactionClient::NullTimestamp);

    //
    // Queues
    //

    NQueueClient::IQueueRowsetPtr DoPullQueueImpl(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options,
        bool checkPermissions);

    IUnversionedRowsetPtr DoPullQueueViaSelectRows(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options);

    IUnversionedRowsetPtr DoPullQueueViaTabletNodeApi(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options,
        bool checkPermissions);

    //
    // Chaos
    //

    NRpc::IChannelPtr GetChaosChannelByCellId(
        NObjectClient::TCellId cellId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);
    NRpc::IChannelPtr GetChaosChannelByCellTag(
        NObjectClient::TCellTag cellTag,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);
    NRpc::IChannelPtr GetChaosChannelByCardId(
        NChaosClient::TReplicationCardId replicationCardId,
        NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);
    NChaosClient::TReplicationCardPtr GetSyncReplicationCard(const NTabletClient::TTableMountInfoPtr& tableInfo);

    //
    // Cypress
    //

    template <class TOptions>
    NCypressClient::TNodeId DoCloneNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TOptions& options);

    //
    // File cache
    //

    void SetTouchedAttribute(
        const TString& destination,
        const TPrerequisiteOptions& options = TPrerequisiteOptions(),
        NTransactionClient::TTransactionId transactionId = {});
    TPutFileToCacheResult DoAttemptPutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options,
        NLogging::TLogger logger);

    //
    // Security
    //

    TCheckPermissionResult CheckPermissionImpl(
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});
    void ValidatePermissionImpl(
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});
    void MaybeValidateExternalObjectPermission(
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});
    NYPath::TYPath GetReplicaTablePath(
        NTabletClient::TTableReplicaId replicaId);
    void ValidateTableReplicaPermission(
        NTabletClient::TTableReplicaId replicaId,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});

    //
    // Operation info
    //

    int DoGetOperationsArchiveVersion();

    struct TGetOperationFromCypressResult
    {
        std::optional<TOperation> Operation;
        TInstant NodeModificationTime;
    };

    TGetOperationFromCypressResult DoGetOperationFromCypress(
        NScheduler::TOperationId operationId,
        const TGetOperationOptions& options);
    std::optional<TOperation> DoGetOperationFromArchive(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);
    TOperation DoGetOperationImpl(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);

    NScheduler::TOperationId ResolveOperationAlias(
        const TString& alias,
        const TMasterReadOptions& options,
        TInstant deadline);

    // Searches in Cypress for operations satisfying given filters.
    // Adds found operations to |idToOperation| map.
    // The operations are returned with requested fields plus necessarily "start_time" and "id".
    void DoListOperationsFromCypress(
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options,
        THashMap<NScheduler::TOperationId, TOperation>* idToOperation,
        const NLogging::TLogger& Logger);

    THashMap<NScheduler::TOperationId, TOperation> LookupOperationsInArchiveTyped(
        const std::vector<NScheduler::TOperationId>& ids,
        const THashSet<TString>& attributes,
        std::optional<TDuration> timeout,
        const NLogging::TLogger& Logger);

    // Searches in archive for operations satisfying given filters.
    // Returns operations with requested fields plus necessarily "start_time" and "id".
    THashMap<NScheduler::TOperationId, TOperation> DoListOperationsFromArchive(
        TInstant deadline,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options,
        const NLogging::TLogger& Logger);

    //
    // Job artifacts and info
    //

    // Get job node descriptor from scheduler and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NNodeTrackerClient::TNodeDescriptor> TryGetJobNodeDescriptor(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    TErrorOr<NRpc::IChannelPtr> TryCreateChannelToJobNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    TErrorOr<NControllerAgent::NProto::TJobSpec> TryFetchJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        NRpc::IChannelPtr nodeChannel);
    // Fetch job spec from node and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NControllerAgent::NProto::TJobSpec> TryFetchJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    // Returns zero id if operation is missing in corresponding table.
    NScheduler::TOperationId TryGetOperationId(NScheduler::TJobId jobId);

    void ValidateOperationAccess(
        NScheduler::TJobId jobId,
        const NControllerAgent::NProto::TJobSpec& jobSpec,
        NYTree::EPermissionSet permissions);
    void ValidateOperationAccess(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet permissions);
    void ValidateOperationAccess(
        NScheduler::TOperationId operationId,
        const NSecurityClient::TSerializableAccessControlList& operationAcl,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet permissions);

    NControllerAgent::NProto::TJobSpec FetchJobSpecFromArchive(
        NScheduler::TJobId jobId);
    // Tries to fetch job spec from both node and job archive and checks
    // that user has |requiredPermissions| for accessing the corresponding operation.
    // Throws if spec could not be fetched.
    NControllerAgent::NProto::TJobSpec FetchJobSpec(
        NScheduler::TJobId jobId,
        NApi::EJobSpecSource specSource,
        NYTree::EPermissionSet requiredPermissions);

    TSharedRef DoGetJobStderrFromNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobStderrFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);

    TSharedRef DoGetJobFailContextFromNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobFailContextFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);

    // Get statistics for jobs.
    TFuture<TListJobsStatistics> ListJobsStatisticsFromArchiveAsync(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TListJobsOptions& options);

    // Retrieve:
    // 1) Filtered finished jobs (with limit).
    // 2) All (non-filtered and without limit) in-progress jobs (if |includeInProgressJobs == true|).
    TFuture<std::vector<TJob>> DoListJobsFromArchiveAsync(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TListJobsOptions& options);

    TFuture<TListJobsFromControllerAgentResult> DoListJobsFromControllerAgentAsync(
        NScheduler::TOperationId operationId,
        const std::optional<TString>& controllerAgentAddress,
        TInstant deadline,
        const TListJobsOptions& options);

    std::optional<TJob> DoGetJobFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        TInstant deadline,
        const THashSet<TString>& attributes);

    std::optional<TJob> DoGetJobFromControllerAgent(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        TInstant deadline,
        const THashSet<TString>& attributes);

    //
    // Administration
    //

    void SyncCellsIfNeeded(const std::vector<NObjectClient::TCellId>& cellIds);

    //
    // Internal
    //

    void DoToggleHunkStoreLock(
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        bool lock,
        const TTimeoutOptions& options);

    //
    // Authentication
    //

    TIssueTokenResult DoIssueTokenImpl(
        const TString& user,
        const NYTree::IAttributeDictionaryPtr& attributes,
        const TIssueTokenOptions& options);

    //! Checks whether authenticated user can execute
    //! authentication-related commands affecting |user|.
    //! Always allows execution for superusers.
    //! Allows execution if |user| coincides with authenticated user
    //! and valid password is provided.
    //! Always denies execution for other users.
    void ValidateAuthenticationCommandPermissions(
        TStringBuf action,
        const TString& user,
        const TString& passwordSha256,
        const TTimeoutOptions& options);

    //
    // Flow
    //

    TString DiscoverPipelineControllerLeader(const NYPath::TYPath& pipelinePath);

    NFlow::NController::TControllerServiceProxy CreatePipelineControllerLeaderProxy(const NYPath::TYPath& pipelinePath);
};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
