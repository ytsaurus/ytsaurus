#pragma once

#include "client.h"

#include "private.h"

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/query_builder.h>
#include <yt/ytlib/query_client/ast.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/scheduler/job_prober_service_proxy.h>
#include <yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/ytlib/tablet_client/table_replica_ypath.h>
#include <yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ypath/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

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

    virtual TFuture<void> Terminate() override;

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
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(IVersionedRowsetPtr, VersionedLookupRows, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options),
        (path, std::move(nameTable), std::move(keys), options))
    IMPLEMENT_METHOD(TSelectRowsResult, SelectRows, (
        const TString& query,
        const TSelectRowsOptions& options),
        (query, options))
    IMPLEMENT_METHOD(NYson::TYsonString, ExplainQuery, (
        const TString& query,
        const TExplainQueryOptions& options),
        (query, options))
    IMPLEMENT_METHOD(std::vector<NTabletClient::TTableReplicaId>, GetInSyncReplicas, (
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options),
        (path, nameTable, keys, options))
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
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
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
    IMPLEMENT_METHOD(TSharedRef, GetJobStderr, (
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobStderrOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TSharedRef, GetJobFailContext, (
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobFailContextOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(TListOperationsResult, ListOperations, (
        const TListOperationsOptions& options),
        (options))
    IMPLEMENT_METHOD(TListJobsResult, ListJobs, (
        NScheduler::TOperationId operationId,
        const TListJobsOptions& options),
        (operationId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, GetJob, (
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobOptions& options),
        (operationId, jobId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, StraceJob, (
        NScheduler::TJobId jobId,
        const TStraceJobOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(void, SignalJob, (
        NScheduler::TJobId jobId,
        const TString& signalName,
        const TSignalJobOptions& options),
        (jobId, signalName, options))
    IMPLEMENT_METHOD(void, AbandonJob, (
        NScheduler::TJobId jobId,
        const TAbandonJobOptions& options),
        (jobId, options))
    IMPLEMENT_METHOD(NYson::TYsonString, PollJobShell, (
        NScheduler::TJobId jobId,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options),
        (jobId, parameters, options))
    IMPLEMENT_METHOD(void, AbortJob, (
        NScheduler::TJobId jobId,
        const TAbortJobOptions& options),
        (jobId, options))


    IMPLEMENT_METHOD(TClusterMeta, GetClusterMeta, (
        const TGetClusterMetaOptions& options),
        (options))

#undef DROP_BRACES
#undef IMPLEMENT_METHOD

private:
    friend class TTransaction;

    const IConnectionPtr Connection_;
    const TClientOptions Options_;
    const NConcurrency::TAsyncSemaphorePtr ConcurrentRequestsSemaphore_;
    const NLogging::TLogger Logger;

    TEnumIndexedVector<EMasterChannelKind, THashMap<NObjectClient::TCellTag, NRpc::IChannelPtr>> MasterChannels_;
    NRpc::IChannelPtr SchedulerChannel_;
    TSpinLock OperationsArchiveChannelsLock_;
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
        const NRpc::IClientRequestPtr& request,
        const TMasterReadOptions& options);

    template <class TProxy>
    std::unique_ptr<TProxy> CreateReadProxy(
        const TMasterReadOptions& options,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag);
    template <class TProxy>
    std::unique_ptr<TProxy> CreateWriteProxy(
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag);
    NRpc::IChannelPtr GetReadCellChannelOrThrow(NObjectClient::TCellId cellId);

    using TEncoderWithMapping = std::function<std::vector<TSharedRef>(
        const NTableClient::TColumnFilter&,
        const std::vector<NTableClient::TUnversionedRow>&)>;
    using TDecoderWithMapping = std::function<NTableClient::TTypeErasedRow(
        const NTableClient::TSchemaData&,
        NTableClient::TWireProtocolReader*)>;
    template <class TResult>
    using TReplicaFallbackHandler = std::function<TFuture<TResult>(
        const NApi::IClientPtr&,
        const NTabletClient::TTableReplicaInfoPtr&)>;

    IUnversionedRowsetPtr DoLookupRows(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options);
    IVersionedRowsetPtr DoVersionedLookupRows(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options);

    TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
        const NTabletClient::TTableMountInfoPtr& tableInfo,
        const TTabletReadOptions& options,
        const std::vector<std::pair<NTableClient::TKey, int>>& keys);
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

    template <class TRowset, class TRow>
    TRowset DoLookupRowsOnce(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptionsBase& options,
        const std::optional<TString>& retentionConfig,
        TEncoderWithMapping encoderWithMapping,
        TDecoderWithMapping decoderWithMapping,
        TReplicaFallbackHandler<TRowset> replicaFallbackHandler);

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

    std::vector<NTabletClient::TTableReplicaId> DoGetInSyncReplicas(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options);

    std::vector<NTableClient::TColumnarStatistics> DoGetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& paths,
        const TGetColumnarStatisticsOptions& options);

    void DoTruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options);

    // Dynamic tables
    std::vector<TTabletInfo> DoGetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options);

    std::unique_ptr<NYTree::IAttributeDictionary> ResolveExternalTable(
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

    void DoReshardTableWithPivotKeys(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotTKeys,
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

    // Cypress
    NYson::TYsonString DoGetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options);
    void DoSetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options);
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

    // File Cache
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

    // Security
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
    void InternalValidateTableReplicaPermission(
        NTabletClient::TTableReplicaId replicaId,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {});
    void ValidateOperationAccess(
        NScheduler::TJobId jobId,
        const NJobTrackerClient::NProto::TJobSpec& jobSpec,
        NYTree::EPermissionSet permissions);

    // Operations
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

    bool DoesOperationsArchiveExist();
    int DoGetOperationsArchiveVersion();

    // Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
    // commands to Cypress node attribute names.
    static std::vector<TString> MakeCypressOperationAttributes(const THashSet<TString>& attributes);
    // Map operation attribute names as they are requested in 'get_operation' or 'list_operations'
    // commands to operations archive column names.
    std::vector<TString> MakeArchiveOperationAttributes(const THashSet<TString>& attributes);

    NYson::TYsonString DoGetOperationFromCypress(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);
    NYson::TYsonString DoGetOperationFromArchive(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        const TGetOperationOptions& options);

    NScheduler::TOperationId ResolveOperationAlias(
        const TString& alias,
        const TGetOperationOptions& options,
        TInstant deadline);

    NYson::TYsonString DoGetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options);

    void DoDumpJobContext(
        NScheduler::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options);

    static void ValidateJobSpecVersion(
        NScheduler::TJobId jobId,
        const NYT::NJobTrackerClient::NProto::TJobSpec& jobSpec);

    static bool IsNoSuchJobOrOperationError(const TError& error);

    // Get job node descriptor from scheduler and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NNodeTrackerClient::TNodeDescriptor> TryGetJobNodeDescriptor(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    NRpc::IChannelPtr TryCreateChannelToJobNode(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    TErrorOr<NJobTrackerClient::NProto::TJobSpec> TryGetJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        const NRpc::IChannelPtr& nodeChannel);
    // Get job spec from node and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    TErrorOr<NJobTrackerClient::NProto::TJobSpec> TryGetJobSpecFromJobNode(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);
    // Get job spec from job archive and check that user has |requiredPermissions|
    // for accessing the corresponding operation.
    NJobTrackerClient::NProto::TJobSpec GetJobSpecFromArchive(
        NScheduler::TJobId jobId,
        NYTree::EPermissionSet requiredPermissions);

    NConcurrency::IAsyncZeroCopyInputStreamPtr DoGetJobInput(
        NScheduler::TJobId jobId,
        const TGetJobInputOptions& options);
    NYson::TYsonString DoGetJobInputPaths(
        NScheduler::TJobId jobId,
        const TGetJobInputPathsOptions& options);
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
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobStderrOptions& options);

    TSharedRef DoGetJobFailContextFromArchive(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobFailContextFromCypress(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId);
    TSharedRef DoGetJobFailContext(
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobFailContextOptions& options);

    static TString ExtractTextFactorForCypressItem(const TOperation& operation);
    static std::vector<TString> GetPoolsFromRuntimeParameters(const NYTree::INodePtr& runtimeParameters);

    TOperation CreateOperationFromNode(
        const NYTree::INodePtr& node,
        const std::optional<THashSet<TString>>& attributes = std::nullopt);

    // XXX(babenko): rename
    THashSet<TString> MakeFinalAttributeSet(
        const std::optional<THashSet<TString>>& originalAttributes,
        const THashSet<TString>& requiredAttributes,
        const THashSet<TString>& defaultAttributes,
        const THashSet<TString>& ignoredAttributes);

    // Searches in Cypress for operations satisfying given filters.
    // Adds found operations to |idToOperation| map.
    // The operations are returned with requested fields plus necessarily "start_time" and "id".
    void DoListOperationsFromCypress(
        TInstant deadline,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options,
        THashMap<NScheduler::TOperationId, TOperation>* idToOperation);

    // Searches in archive for operations satisfying given filters.
    // Returns operations with requested fields plus necessarily "start_time" and "id".
    THashMap<NScheduler::TOperationId, TOperation> DoListOperationsFromArchive(
        TInstant deadline,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options);

    THashSet<TString> GetSubjectClosure(
        const TString& subject,
        NObjectClient::TObjectServiceProxy& proxy,
        const TMasterReadOptions& options);

    // XXX(levysotsky): The counters may be incorrect if |options.IncludeArchive| is |true|
    // and an operation is in both Cypress and archive.
    // XXX(levysotsky): The "failed_jobs_count" counter is incorrect if corresponding failed operations
    // are in archive and outside of queried range.
    TListOperationsResult DoListOperations(const TListOperationsOptions& options);

    // XXX(babenko): rename
    static void ValidateNotNull(
        const NTableClient::TUnversionedValue& value,
        TStringBuf name,
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId = {});

    NQueryClient::TQueryBuilder GetListJobsQueryBuilder(
        NScheduler::TOperationId operationId,
        const std::vector<NJobTrackerClient::EJobState>& states,
        const TListJobsOptions& options);

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
    // Jobs are additionally filtered by |states|.
    TFuture<TListJobsStatistics> ListJobsStatisticsFromArchiveAsync(
        NScheduler::TOperationId operationId,
        const std::vector<NJobTrackerClient::EJobState>& states,
        const TSelectRowsOptions& selectRowsOptions,
        const TListJobsOptions& options);

    struct TListJobsFromArchiveResult
    {
        std::vector<TJob> FinishedJobs;
        std::vector<TJob> InProgressJobs;
        TListJobsStatistics FinishedJobsStatistics;
    };

    // Retrieves:
    // 1) Filtered finished jobs (with limit).
    // 2) All (non-filtered and without limit) in-progress jobs (if |includeInProgressJobs == true|).
    // 3) Statistics for finished jobs.
    TFuture<TListJobsFromArchiveResult> DoListJobsFromArchiveAsync(
        NScheduler::TOperationId operationId,
        TInstant deadline,
        bool includeInProgressJobs,
        const TListJobsOptions& options);
    TFuture<std::pair<std::vector<TJob>, int>> DoListJobsFromCypressAsync(
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
        NScheduler::TOperationId operationId,
        const TListJobsOptions& options);

    template <typename TValue>
    static void TryAddFluentItem(
        NYTree::TFluentMap fluent,
        TStringBuf key,
        NTableClient::TUnversionedRow row,
        const NTableClient::TColumnFilter& columnFilter,
        int columnIndex);

    static std::vector<TString> MakeJobArchiveAttributes(const THashSet<TString>& attributes);

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
        NScheduler::TOperationId operationId,
        NScheduler::TJobId jobId,
        const TGetJobOptions& options);

    NYson::TYsonString DoStraceJob(
        NScheduler::TJobId jobId,
        const TStraceJobOptions& options);
    void DoSignalJob(
        NScheduler::TJobId jobId,
        const TString& signalName,
        const TSignalJobOptions& options);
    void DoAbandonJob(
        NScheduler::TJobId jobId,
        const TAbandonJobOptions& options);
    NYson::TYsonString DoPollJobShell(
        NScheduler::TJobId jobId,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options);
    void DoAbortJob(
        NScheduler::TJobId jobId,
        const TAbortJobOptions& options);

    TClusterMeta DoGetClusterMeta(
        const TGetClusterMetaOptions& options);

};

DEFINE_REFCOUNTED_TYPE(TClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

