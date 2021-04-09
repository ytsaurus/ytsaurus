#pragma once

#include "client.h"
#include "private.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>
#include <yt/yt/ytlib/query_client/query_builder.h>
#include <yt/yt/ytlib/query_client/ast.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/yt/ytlib/scheduler/job_prober_service_proxy.h>
#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/table_replica_ypath.h>
#include <yt/yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using TTableReplicaInfoPtrList = SmallVector<
    NTabletClient::TTableReplicaInfoPtr,
    NChunkClient::TypicalReplicaCount>;

DECLARE_REFCOUNTED_CLASS(TClient)

class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        const TClientOptions& options);

    virtual NApi::IConnectionPtr GetConnection() override;
    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;
    virtual const IConnectionPtr& GetNativeConnection() override;
    virtual NQueryClient::IFunctionRegistryPtr GetFunctionRegistry() override;
    virtual NQueryClient::TFunctionImplCachePtr GetFunctionImplCache() override;

    virtual const TClientOptions& GetOptions() override;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) override;
    virtual NRpc::IChannelPtr GetCellChannelOrThrow(NObjectClient::TCellId cellId) override;
    virtual NRpc::IChannelPtr GetSchedulerChannel() override;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() override;

    virtual void Terminate() override;

    // Transactions
    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options) override;
    virtual ITransactionPtr AttachNativeTransaction(
        NCypressClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options) override;
    virtual TFuture<NApi::ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options) override;
    virtual NApi::ITransactionPtr AttachTransaction(
        NCypressClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options) override;

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_OVERLOADED_METHOD(returnType, method, doMethod, signature, args) \
    virtual TFuture<returnType> method signature override \
    { \
        return Execute( \
            AsStringBuf(#method), \
            options, \
            BIND( \
                &TClient::doMethod, \
                Unretained(this), \
                DROP_BRACES args)); \
    }

#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    IMPLEMENT_OVERLOADED_METHOD(returnType, method, Do##method, signature, args)

    IMPLEMENT_METHOD(IUnversionedRowsetPtr, LookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(IVersionedRowsetPtr, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(std::vector<IUnversionedRowsetPtr>, MultiLookup, (
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options),
        (subrequests, options))
    IMPLEMENT_METHOD(TSelectRowsResult, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))
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
        const TGetTabletsInfoOptions& options),
        (path, tabletIndexes, options))
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
    IMPLEMENT_METHOD(std::vector<NTabletClient::TTabletActionId>, BalanceTabletCells, (
        const TString& tabletCellBundle,
        const std::vector< NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options),
        (tabletCellBundle, movableTables, options))

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

    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options) override;
    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options) override;

    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options) override;
    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options) override;

    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) override;
    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const NApi::TTableWriterOptions& options) override;

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options) override;

    IMPLEMENT_METHOD(std::vector<NTableClient::TColumnarStatistics>, GetColumnarStatistics, (
        const std::vector<NYPath::TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options),
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
    IMPLEMENT_METHOD(NYson::TYsonString, GetOperation, (
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
    IMPLEMENT_METHOD(NYson::TYsonString, PollJobShell, (
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
    IMPLEMENT_METHOD(void, SwitchLeader, (
        NObjectClient::TCellId cellId,
        const TString& newLeaderAddress,
        const TSwitchLeaderOptions& options),
        (cellId, newLeaderAddress, options))
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
    IMPLEMENT_METHOD(TString, WriteOperationControllerCoreDump, (
        NScheduler::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options),
        (operationId, options))
#undef DROP_BRACES
#undef IMPLEMENT_METHOD

private:
    friend class TTransaction;
    friend class TNodeConcatenator;

    const IConnectionPtr Connection_;
    const TClientOptions Options_;
    const NConcurrency::TAsyncSemaphorePtr ConcurrentRequestsSemaphore_;
    const NLogging::TLogger Logger;

    TEnumIndexedVector<EMasterChannelKind, THashMap<NObjectClient::TCellTag, NRpc::IChannelPtr>> MasterChannels_;
    NRpc::IChannelPtr SchedulerChannel_;
    YT_DECLARE_SPINLOCK(TAdaptiveLock, OperationsArchiveChannelsLock_);
    std::optional<TEnumIndexedVector<EMasterChannelKind, NRpc::IChannelPtr>> OperationsArchiveChannels_;
    NNodeTrackerClient::INodeChannelFactoryPtr ChannelFactory_;
    NTransactionClient::TTransactionManagerPtr TransactionManager_;
    NQueryClient::TFunctionImplCachePtr FunctionImplCache_;
    NQueryClient::IFunctionRegistryPtr FunctionRegistry_;
    std::unique_ptr<NScheduler::TSchedulerServiceProxy> SchedulerProxy_;
    std::unique_ptr<NScheduler::TJobProberServiceProxy> JobProberProxy_;

    const NRpc::IChannelPtr& GetOperationArchiveChannel(EMasterChannelKind kind);

    template <class T>
    TFuture<T> Execute(
        TStringBuf commandName,
        const TTimeoutOptions& options,
        TCallback<T()> callback);

    template <class T>
    auto CallAndRetryIfMetadataCacheIsInconsistent(T&& callback) -> decltype(callback());

    static void SetMutationId(
        const NRpc::IClientRequestPtr& request,
        const TMutatingOptions& options);
    NTransactionClient::TTransactionId GetTransactionId(
        const TTransactionalOptions& options,
        bool allowNullTransaction);
    void SetTransactionId(
        const NRpc::IClientRequestPtr& request,
        const TTransactionalOptions& options,
        bool allowNullTransaction);
    void SetPrerequisites(
        const NRpc::IClientRequestPtr& request,
        const TPrerequisiteOptions& options);
    static void SetSuppressAccessTracking(
        const NRpc::IClientRequestPtr& request,
        const TSuppressableAccessTrackingOptions& commandOptions);

    void SetCachingHeader(
        const NRpc::IClientRequestPtr& request,
        const TMasterReadOptions& options);
    void SetBalancingHeader(
        const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& request,
        const TMasterReadOptions& options);

    template <class TProxy>
    std::unique_ptr<TProxy> CreateReadProxy(
        const TMasterReadOptions& options,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag);
    template <class TProxy>
    std::unique_ptr<TProxy> CreateWriteProxy(
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag);
    NRpc::IChannelPtr GetReadCellChannelOrThrow(NObjectClient::TCellId cellId);
    NRpc::IChannelPtr GetLeaderCellChannelOrThrow(NObjectClient::TCellId cellId);
    NHiveClient::TCellDescriptor GetCellDescriptorOrThrow(NObjectClient::TCellId cellId);
    std::vector<TString> GetCellAddressesOrThrow(NObjectClient::TCellId cellId);

    void ValidateSuperuserPermissions();

    IUnversionedRowsetPtr DoLookupRows(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptions& options);
    IVersionedRowsetPtr DoVersionedLookupRows(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TVersionedLookupRowsOptions& options);
    std::vector<IUnversionedRowsetPtr> DoMultiLookup(
        const std::vector<TMultiLookupSubrequest>& subrequests,
        const TMultiLookupOptions& options);

    using TEncoderWithMapping = std::function<std::vector<TSharedRef>(
        const NTableClient::TColumnFilter&,
        const std::vector<NTableClient::TUnversionedRow>&)>;
    TEncoderWithMapping GetLookupRowsEncoder() const;

    using TDecoderWithMapping = std::function<NTableClient::TTypeErasedRow(
        const NTableClient::TSchemaData&,
        NTableClient::TWireProtocolReader*)>;
    TDecoderWithMapping GetLookupRowsDecoder() const;

    template <class TResult>
    using TReplicaFallbackHandler = std::function<TFuture<TResult>(
        const NApi::IClientPtr&,
        const NTabletClient::TTableReplicaInfoPtr&)>;

    template <class TRowset, class TRow>
    TRowset DoLookupRowsOnce(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TLookupRowsOptionsBase& options,
        const std::optional<TString>& retentionConfig,
        TEncoderWithMapping encoderWithMapping,
        TDecoderWithMapping decoderWithMapping,
        TReplicaFallbackHandler<TRowset> replicaFallbackHandler);

    static NTabletClient::TTableReplicaInfoPtr PickRandomReplica(
        const TTableReplicaInfoPtrList& replicas);
    static TString PickRandomCluster(
        const std::vector<TString>& clusterNames);

    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options,
        const std::vector<std::pair<NTableClient::TLegacyKey, int>>& keys);
    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options);
    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options,
        const THashMap<NObjectClient::TCellId, std::vector<NTabletClient::TTabletId>>& cellIdToTabletIds);

    std::optional<TString> PickInSyncClusterAndPatchQuery(
        const TTabletReadOptions& options,
        NQueryClient::NAst::TQuery* query);

    NApi::IConnectionPtr GetReplicaConnectionOrThrow(const TString& clusterName);
    NApi::IClientPtr CreateReplicaClient(const TString& clusterName);

    TSelectRowsResult DoSelectRows(
        const TString& queryString,
        const TSelectRowsOptions& options);
    TSelectRowsResult DoSelectRowsOnce(
        const TString& queryString,
        const TSelectRowsOptions& options);
    NYson::TYsonString DoExplainQuery(
        const TString& queryString,
        const TExplainQueryOptions& options);

    static bool IsReplicaInSync(
        const NQueryClient::NProto::TReplicaInfo& replicaInfo,
        const NQueryClient::NProto::TTabletInfo& tabletInfo);
    static bool IsReplicaInSync(
        const NQueryClient::NProto::TReplicaInfo& replicaInfo,
        const NQueryClient::NProto::TTabletInfo& tabletInfo,
        NTransactionClient::TTimestamp timestamp);

    std::vector<NTabletClient::TTableReplicaId> DoGetInSyncReplicasWithKeys(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options);
    std::vector<NTabletClient::TTableReplicaId> DoGetInSyncReplicasWithoutKeys(
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options);
    std::vector<NTabletClient::TTableReplicaId> DoGetInSyncReplicas(
        const NYPath::TYPath& path,
        bool allKeys,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options);

    std::vector<NTableClient::TColumnarStatistics> DoGetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options);

    void DoTruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options);

    //
    // Dynamic tables.
    //

    std::vector<TTabletInfo> DoGetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options);

    NYTree::IAttributeDictionaryPtr ResolveExternalTable(
        const NYPath::TYPath& path,
        NTableClient::TTableId* tableId,
        NObjectClient::TCellTag* externalCellTag,
        const std::vector<TString>& extraAttributeKeys = {});

    template <class TReq>
    void ExecuteTabletServiceRequest(
        const NYPath::TYPath& path,
        TStringBuf action,
        TReq* req);

    void DoMountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options);
    void DoUnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options);
    void DoRemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options);
    void DoFreezeTable(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options);
    void DoUnfreezeTable(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options);

    NTabletClient::NProto::TReqReshard MakeReshardRequest(
        const TReshardTableOptions& options);
    NTableClient::TTableYPathProxy::TReqReshardPtr MakeYPathReshardRequest(
        const NYPath::TYPath& path,
        const TReshardTableOptions& options);

    std::vector<NTableClient::TLegacyOwningKey> PickUniformPivotKeys(
        const NYPath::TYPath& path,
        int tabletCount);

    void DoReshardTableWithPivotKeys(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotTKeys,
        const TReshardTableOptions& options);
    void DoReshardTableWithTabletCount(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options);
    std::vector<NTabletClient::TTabletActionId> DoReshardTableAutomatic(
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options);

    void DoAlterTable(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options);

    void DoTrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options);

    void DoAlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options);

    NYson::TYsonString DoGetTablePivotKeys(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options);

    std::vector<NTabletClient::TTabletActionId> DoBalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector< NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options);

    //
    // Cypress.
    //

    NYson::TYsonString DoGetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options);
    void DoSetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options);
    void DoMultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options);
    void DoRemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options);
    NYson::TYsonString DoListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options);
    NCypressClient::TNodeId DoCreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options);

    TLockNodeResult DoLockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options);
    void DoUnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options);

    NCypressClient::TNodeId DoCopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options);
    NCypressClient::TNodeId DoMoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options);

    template <class TOptions>
    NCypressClient::TNodeId DoCloneNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TOptions& options);

    NCypressClient::TNodeId DoLinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options);
    void DoConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        TConcatenateNodesOptions options);
    void DoExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        TExternalizeNodeOptions options);
    void DoInternalizeNode(
        const NYPath::TYPath& path,
        TInternalizeNodeOptions options);
    bool DoNodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options);
    NObjectClient::TObjectId DoCreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options);

    //
    // File cache.
    //

    void SetTouchedAttribute(
        const TString& destination,
        const TPrerequisiteOptions& options = TPrerequisiteOptions(),
        NTransactionClient::TTransactionId transactionId = {});
    TGetFileFromCacheResult DoGetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options);
    TPutFileToCacheResult DoAttemptPutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options,
        NLogging::TLogger logger);
    TPutFileToCacheResult DoPutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options);

    //
    // Security.
    //

    void DoAddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options);
    void DoRemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options);
    TCheckPermissionResponse DoCheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options);
    TCheckPermissionByAclResult DoCheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        const NYTree::INodePtr& acl,
        const TCheckPermissionByAclOptions& options);
        TCheckPermissionResult InternalCheckPermission(
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});
    void InternalValidatePermission(
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
    void DoTransferAccountResources(
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options);

    //
    // Operations.
    //

    NScheduler::TOperationId DoStartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options);
    void DoAbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options);
    void DoSuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options);
    void DoResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options);
    void DoCompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options);
    void DoUpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options);

    //
    // Operation info.
    //

    bool DoesOperationsArchiveExist();
    int DoGetOperationsArchiveVersion();

    struct TGetOperationFromCypressResult
    {
        NYson::TYsonString Operation;
        TInstant NodeModificationTime;
    };

    TGetOperationFromCypressResult DoGetOperationFromCypress(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);
    NYson::TYsonString DoGetOperationFromArchive(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);
    NYson::TYsonString DoGetOperationImpl(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);
    NYson::TYsonString DoGetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options);

    NScheduler::TOperationId ResolveOperationAlias(
        const TString& alias,
        const TMasterReadOptions& options,
        TInstant deadline);

    // Searches in Cypress for operations satisfying given filters.
    // Adds found operations to |idToOperation| map.
    // The operations are returned with requested fields plus necessarily "start_time" and "id".
    void DoListOperationsFromCypress(
        TInstant deadline,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options,
        THashMap<NScheduler::TOperationId, TOperation>* idToOperation);

    THashMap<NScheduler::TOperationId, TOperation> LookupOperationsInArchiveTyped(
        const std::vector<NScheduler::TOperationId>& ids,
        const std::optional<THashSet<TString>>& attributes,
        std::optional<TDuration> timeout);

    // Searches in archive for operations satisfying given filters.
    // Returns operations with requested fields plus necessarily "start_time" and "id".
    THashMap<NScheduler::TOperationId, TOperation> DoListOperationsFromArchive(
        TInstant deadline,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options);

    // XXX(levysotsky): The counters may be incorrect if |options.IncludeArchive| is |true|
    // and an operation is in both Cypress and archive.
    // XXX(levysotsky): The "failed_jobs_count" counter is incorrect if corresponding failed operations
    // are in archive and outside of queried range.
    TListOperationsResult DoListOperations(const TListOperationsOptions& options);

    //
    // Jobs.
    //

    void DoAbandonJob(
        NScheduler::TJobId jobId,
        const TAbandonJobOptions& options);
    NYson::TYsonString DoPollJobShell(
        NScheduler::TJobId jobId,
        const std::optional<TString>& shellName,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options);
    void DoAbortJob(
        NScheduler::TJobId jobId,
        const TAbortJobOptions& options);

    //
    // Job artifacts and info.
    //

    void DoDumpJobContext(
        NScheduler::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options);

    // Get job node descriptor from scheduler and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NNodeTrackerClient::TNodeDescriptor> TryGetJobNodeDescriptor(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    TErrorOr<NRpc::IChannelPtr> TryCreateChannelToJobNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    TErrorOr<NJobTrackerClient::NProto::TJobSpec> TryFetchJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        NRpc::IChannelPtr nodeChannel);
    // Fetch job spec from node and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NJobTrackerClient::NProto::TJobSpec> TryFetchJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    // Returns zero id if operation is missing in corresponding table.
    NScheduler::TOperationId TryGetOperationId(NScheduler::TJobId);

    void ValidateOperationAccess(
        NScheduler::TJobId jobId,
        const NJobTrackerClient::NProto::TJobSpec& jobSpec,
        NYTree::EPermissionSet permissions);
    void ValidateOperationAccess(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet permissions);

    NJobTrackerClient::NProto::TJobSpec FetchJobSpecFromArchive(
        NScheduler::TJobId jobId);
    // Tries to fetch job spec from both node and job archive and checks
    // that user has |requiredPermissions| for accessing the corresponding operation.
    // Throws if spec could not be fetched.
    NJobTrackerClient::NProto::TJobSpec FetchJobSpec(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    NConcurrency::IAsyncZeroCopyInputStreamPtr DoGetJobInput(
        NScheduler::TJobId jobId,
        const TGetJobInputOptions& options);
    NYson::TYsonString DoGetJobInputPaths(
        NScheduler::TJobId jobId,
        const TGetJobInputPathsOptions& options);
    NYson::TYsonString DoGetJobSpec(
        NScheduler::TJobId jobId,
        const TGetJobSpecOptions& options);
    TSharedRef DoGetJobStderrFromNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobStderrFromCypress(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobStderrFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobStderr(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobStderrOptions& options);

    TSharedRef DoGetJobFailContextFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobFailContextFromCypress(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobFailContext(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobFailContextOptions& options);

    // Asynchronously perform "select_rows" from job archive and parse result.
    // |Offset| and |Limit| fields in |options| are ignored, |limit| is used instead.
    // Jobs are additionally filtered by |states|.
    TFuture<std::vector<TJob>> DoListJobsFromArchiveAsyncImpl(
        NScheduler::TOperationId operationId,
        const std::vector<NJobTrackerClient::EJobState>& states,
        i64 limit,
        const TSelectRowsOptions& selectRowsOptions,
        const TListJobsOptions& options);

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

    struct TListJobsFromControllerAgentResult
    {
        std::vector<TJob> FinishedJobs;
        int TotalFinishedJobCount = 0;
        std::vector<TJob> InProgressJobs;
        int TotalInProgressJobCount = 0;
    };

    TFuture<TListJobsFromControllerAgentResult> DoListJobsFromControllerAgentAsync(
        NScheduler::TOperationId operationId,
        const std::optional<TString>& controllerAgentAddress,
        TInstant deadline,
        const TListJobsOptions& options);

    TListJobsResult DoListJobs(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TListJobsOptions& options);

    std::optional<TJob> DoGetJobFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        TInstant deadline,
        const THashSet<TString>& attributes,
        const TGetJobOptions& options);
    std::optional<TJob> DoGetJobFromControllerAgent(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        TInstant deadline,
        const THashSet<TString>& attributes,
        const TGetJobOptions& options);
    NYson::TYsonString DoGetJob(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        NScheduler::TJobId jobId,
        const TGetJobOptions& options);


    // Misc.
    TClusterMeta DoGetClusterMeta(
        const TGetClusterMetaOptions& options);
    void DoCheckClusterLiveness(
        const TCheckClusterLivenessOptions& options);


    // Administration.
    int DoBuildSnapshot(
        const TBuildSnapshotOptions& options);
    TCellIdToSnapshotIdMap DoBuildMasterSnapshots(
        const TBuildMasterSnapshotsOptions& options);
    void DoSwitchLeader(
        NObjectClient::TCellId cellId,
        const TString& newLeaderAddress,
        const TSwitchLeaderOptions& options);
    void DoGCCollect(
        const TGCCollectOptions& options);
    void DoKillProcess(
        const TString& address,
        const TKillProcessOptions& options);
    TString DoWriteCoreDump(
        const TString& address,
        const TWriteCoreDumpOptions& options);
    TString DoWriteOperationControllerCoreDump(
        NScheduler::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options);
};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

