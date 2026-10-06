#pragma once

#include "api_service.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/non_null_ptr.h>

#include <library/cpp/yt/string/stream.h>

#include <algorithm>

namespace NYT::NQueryTrackerClient {

class TQueryTrackerServiceProxy;

} // namespace NYT::NQueryTrackerClient

namespace NYT::NRpcProxy {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TError MakeCanceledError();

void SetTimeoutOptions(
    NApi::TTimeoutOptions* options,
    const NRpc::IServiceContext* context);

template <class TRequest>
void SetMutatingOptions(
    NApi::TMutatingOptions* options,
    const TRequest* request,
    const NRpc::IServiceContext* context);

void FromProto(
    NApi::TPrerequisiteOptions* options,
    const NApi::NRpcProxy::NProto::TPrerequisiteOptions& proto);

void FromProto(
    NApi::TMasterReadOptions* options,
    const NApi::NRpcProxy::NProto::TMasterReadOptions& proto);

void FromProto(
    NApi::TMutatingOptions* options,
    const NApi::NRpcProxy::NProto::TMutatingOptions& proto);

void FromProto(
    NApi::TTabletRangeOptions* options,
    const NApi::NRpcProxy::NProto::TTabletRangeOptions& proto);

void FromProto(
    NApi::TTabletReadOptionsBase* options,
    const NApi::NRpcProxy::NProto::TTabletReadOptions& proto);

NApi::IUnversionedRowsetPtr DeserializeRowset(
    const NApi::NRpcProxy::NProto::TRowsetDescriptor& descriptor,
    NTableClient::TTableSchemaPtr schema,
    const std::optional<NFormats::TFormat>& format,
    const TSharedRef& data,
    const NLogging::TLogger& logger);

template <class TResponse, class TRow>
std::vector<TSharedRef> PrepareRowsetForAttachment(
    TResponse* response,
    const TIntrusivePtr<NApi::IRowset<TRow>>& rowset,
    const IMemoryUsageTrackerPtr& memoryTracker = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TDetailedProfilingCounters
    : public TRefCounted
{
public:
    explicit TDetailedProfilingCounters(NProfiling::TProfiler profiler);

    const NProfiling::TEventTimer& LookupDurationTimer() const;
    const NProfiling::TEventTimer& SelectDurationTimer() const;
    const NProfiling::TEventTimer& PullQueueDurationTimer() const;
    const NProfiling::TEventTimer& LookupMountCacheWaitTimer() const;
    const NProfiling::TEventTimer& SelectMountCacheWaitTimer() const;
    const NProfiling::TEventTimer& PullQueueMountCacheWaitTimer() const;
    const NProfiling::TEventTimer& LookupPermissionCacheWaitTimer() const;
    const NProfiling::TEventTimer& SelectPermissionCacheWaitTimer() const;
    const NProfiling::TEventTimer& PullQueuePermissionCacheWaitTimer() const;
    const NProfiling::TCounter& WastedLookupSubrequestCount() const;
    NProfiling::TCounter* GetRetryCounterByReason(TErrorCode reason);

private:
    const NProfiling::TProfiler Profiler_;

    //! Histograms.
    NProfiling::TEventTimer LookupDuration_;
    NProfiling::TEventTimer SelectDuration_;
    NProfiling::TEventTimer PullQueueDuration_;

    //! Timers.
    NProfiling::TEventTimer LookupMountCacheWaitTime_;
    NProfiling::TEventTimer SelectMountCacheWaitTime_;
    NProfiling::TEventTimer PullQueueMountCacheWaitTime_;
    NProfiling::TEventTimer LookupPermissionCacheWaitTime_;
    NProfiling::TEventTimer SelectPermissionCacheWaitTime_;
    NProfiling::TEventTimer PullQueuePermissionCacheWaitTime_;

    NProfiling::TCounter WastedLookupSubrequestCount_;

    //! Retryable error code to counter map.
    NConcurrency::TSyncMap<TErrorCode, NProfiling::TCounter> RetryCounters_;
};

DEFINE_REFCOUNTED_TYPE(TDetailedProfilingCounters)

////////////////////////////////////////////////////////////////////////////////

struct IApiServiceContext
    : public virtual NRpc::IServiceContext
{
    virtual void SetClient(std::optional<std::string> clientClusterName, NApi::NNative::IClientPtr client) = 0;
};

DEFINE_REFCOUNTED_TYPE(IApiServiceContext)

template <class TRequestMessage, class TResponseMessage>
class TApiServiceContext
    : public NRpc::TTypedServiceContext<TRequestMessage, TResponseMessage>
    , public IApiServiceContext
{
    using TBase = NRpc::TTypedServiceContext<TRequestMessage, TResponseMessage>;

public:
    // For most cases the most important request field is "path". If it is present in request message,
    // we want to see it in the structured log.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, RequestPath);

public:
    using NRpc::TTypedServiceContext<TRequestMessage, TResponseMessage>::TTypedServiceContext;

    void Reply(const TError& error = {}) override;
    void SetLogger(NLogging::TLogger logger);
    void SetClient(std::optional<std::string> clientClusterName, NApi::NNative::IClientPtr client) override;
    void SetupMainMessage(NYson::TYsonString requestYson);
    void SetupErrorMessage();
    void LogStructuredError(const TError& error) const;
    void LogStructured() const;

private:
    NLogging::TLogger Logger;

    std::optional<std::string> ClientClusterName_;
    NApi::NNative::IClientPtr Client_;

    //! True if message should be emitted to main topic.
    bool EmitMain_ = false;
    // YSON-serialized request body. This field may be really heavy.
    std::optional<NYson::TYsonString> RequestYson_;

    //! True if message should be emitted to error topic provided that request indeed resulted in error.
    bool EmitError_ = false;

    TError GetFinalError() const;
    void DoEmitMain() const;
    void DoEmitError() const;
};

////////////////////////////////////////////////////////////////////////////////

class TApiService
    : public NRpc::TServiceBase
    , public IApiService
{
public:
    template <class TRequestMessage, class TResponseMessage>
    using TTypedServiceContextImpl = TApiServiceContext<TRequestMessage, TResponseMessage>;

    TApiService(
        TApiServiceConfigPtr config,
        IInvokerPtr defaultInvoker,
        TPooledInvokerProvider workerInvokerProvider,
        NApi::NNative::IConnectionPtr connection,
        NRpc::IAuthenticatorPtr authenticator,
        IProxyCoordinatorPtr proxyCoordinator,
        IAccessCheckerPtr accessChecker,
        NTracing::TSamplerPtr traceSampler,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        INodeMemoryTrackerPtr memoryTracker,
        NApi::IStickyTransactionPoolPtr stickyTransactionPool,
        NSignature::ISignatureValidatorPtr signatureValidator,
        IQueryCorpusReporterPtr queryCorpusReporter);

private:
    TApiServiceConfigPtr ApiServiceConfig_;
    const NProfiling::TProfiler Profiler_;
    TAtomicIntrusivePtr<TApiServiceDynamicConfig> Config_{New<TApiServiceDynamicConfig>()};
    const NApi::NNative::IConnectionPtr LocalConnection_;
    const IProxyCoordinatorPtr ProxyCoordinator_;
    const IAccessCheckerPtr AccessChecker_;
    const NTracing::TSamplerPtr TraceSampler_;
    const NApi::IStickyTransactionPoolPtr StickyTransactionPool_;
    const TMulticonnectionClientCachePtr AuthenticatedClientCache_;
    const NServer::THeapProfilerTestingOptionsPtr HeapProfilerTestingOptions_;
    const IMemoryUsageTrackerPtr HeavyRequestMemoryUsageTracker_;
    const NSignature::ISignatureValidatorPtr SignatureValidator_;
    const IQueryCorpusReporterPtr QueryCorpusReporter_;
    const NSecurityServer::IUserAccessValidatorPtr UserAccessValidator_;
    const TPooledInvokerProvider WorkerInvokerProvider_;

    static const TStructuredLoggingMethodDynamicConfigPtr DefaultMethodConfig;

    IMultiproxyAccessValidatorPtr MultiproxyAccessValidator_;

    NProfiling::TCounter SelectConsumeDataWeight_;
    NProfiling::TCounter SelectConsumeRowCount_;

    NProfiling::TCounter SelectOutputDataWeight_;
    NProfiling::TCounter SelectOutputRowCount_;

    struct TDetailedProfilingCountersKey
    {
        std::optional<std::string> UserTag;
        std::optional<NYPath::TYPath> TablePath;

        operator size_t() const
        {
            return MultiHash(
                UserTag,
                TablePath);
        }
    };

    using TDetailedProfilingCountersMap = NConcurrency::TSyncMap<
        TDetailedProfilingCountersKey,
        TDetailedProfilingCountersPtr
    >;
    TDetailedProfilingCountersMap DetailedProfilingCountersMap_;

    std::atomic<i64> NextSequenceNumberSourceId_ = 0;

    template <class TContext, class TExecutor, class TResultHandler>
    class TExecuteCallSession;

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GenerateTimestamps);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CommitTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FlushTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AttachTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DetachTransaction);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateObject);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTableMountInfo);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTablePivotKeys);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExistsNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultisetAttributesNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LockNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnlockNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CopyNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MoveNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LinkNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ConcatenateNodes);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExternalizeNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, InternalizeNode);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MountTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnmountTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemountTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FreezeTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnfreezeTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTableAutomatic);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TrimTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTableReplica);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterReplicationCard);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingChaosLease);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BalanceTabletCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateTableBackup);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RestoreTableBackup);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TransferBundleResources);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CompleteOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UpdateOperationParameters);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PatchOperationSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetOperation);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListOperationEvents);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListOperations);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListJobs);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DumpJobContext);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInput);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInputPaths);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobStderr);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobTrace);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListJobTraces);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckOperationPermission);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobFailContext);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJob);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbandonJob);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PollJobShell);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RunJobShellCommand);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortJob);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DumpJobProxyLog);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LookupRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, VersionedLookupRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultiLookup);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SelectRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExplainQuery);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetInSyncReplicas);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTabletInfos);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTabletErrors);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PushQueueProducer);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AdvanceQueueConsumer);
    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        NApi::NRpcProxy::NProto::TReqAdvanceQueueConsumer,
        NApi::NRpcProxy::NProto::TRspAdvanceQueueConsumer,
        AdvanceConsumer);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullQueue);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PullQueueConsumer);
    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        NApi::NRpcProxy::NProto::TReqPullQueueConsumer,
        NApi::NRpcProxy::NProto::TRspPullQueueConsumer,
        PullConsumer);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RegisterQueueConsumer);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnregisterQueueConsumer);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListQueueConsumerRegistrations);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateQueueProducerSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveQueueProducerSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ModifyRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BatchModifyRows);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BuildSnapshot);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExitReadOnly);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MasterExitReadOnly);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DiscombobulateNonvotingPeers);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResetDynamicallyPropagatedMasterCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GCCollect);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendCoordinator);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeCoordinator);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MigrateReplicationCards);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendChaosCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeChaosCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendTabletCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeTabletCells);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AddMaintenance);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMaintenance);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DisableChunkLocations);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DestroyChunkLocations);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResurrectChunkLocations);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RequestRestart);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetCurrentUser);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AddMember);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMember);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermission);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermissionByAcl);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TransferAccountResources);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadFile);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteFile);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PartitionFile);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadFilePartition);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadJournal);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteJournal);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TruncateJournal);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteTable);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetColumnarStatistics);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PartitionTables);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadTablePartition);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartDistributedWriteSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingDistributedWriteSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FinishDistributedWriteSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteTableFragment);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartDistributedWriteFileSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingDistributedWriteFileSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FinishDistributedWriteFileSession);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteFileFragment);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetFileFromCache);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PutFileToCache);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetPipelineSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineDynamicSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetPipelineDynamicSpec);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartPipeline);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StopPipeline);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PausePipeline);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetPipelineState);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetFlowView);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FlowExecute);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartQuery);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortQuery);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQueryResult);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadQueryResult);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQuery);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListQueries);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterQuery);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQueryTrackerInfo);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetQueryDeclaredParametersInfo);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartShuffle);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadShuffleData);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteShuffleData);
    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckClusterLiveness);

    void RegisterMethodForMultiproxy(
        TMultiproxyMethodList* methodList,
        EMultiproxyMethodKind methodKind,
        const TMethodDescriptor& descriptor);

    void RegisterTransactionMethods(TMultiproxyMethodList* methodList);
    void RegisterCypressMethods(TMultiproxyMethodList* methodList);
    void RegisterDynamicTableMethods(TMultiproxyMethodList* methodList);
    void RegisterReplicatedTableMethods(TMultiproxyMethodList* methodList);
    void RegisterOperationMethods(TMultiproxyMethodList* methodList);
    void RegisterOperationInfoMethods(TMultiproxyMethodList* methodList);
    void RegisterJobInfoMethods(TMultiproxyMethodList* methodList);
    void RegisterJobMethods(TMultiproxyMethodList* methodList);
    void RegisterQueueMethods(TMultiproxyMethodList* methodList);
    void RegisterAdminMethods(TMultiproxyMethodList* methodList);
    void RegisterSecurityMethods(TMultiproxyMethodList* methodList);
    void RegisterFileMethods(TMultiproxyMethodList* methodList);
    void RegisterJournalMethods(TMultiproxyMethodList* methodList);
    void RegisterStaticTableMethods(TMultiproxyMethodList* methodList);
    void RegisterFileCacheMethods(TMultiproxyMethodList* methodList);
    void RegisterFlowMethods(TMultiproxyMethodList* methodList);
    void RegisterQueryMethods(TMultiproxyMethodList* methodList);
    void RegisterDistributedTableMethods(TMultiproxyMethodList* methodList);
    void RegisterDistributedFileMethods(TMultiproxyMethodList* methodList);
    void RegisterShuffleMethods(TMultiproxyMethodList* methodList);

    void OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config) override;
    NYTree::IYPathServicePtr CreateOrchidService() override;
    std::optional<std::string> GetMultiproxyTargetCluster(const NRpc::IServiceContextPtr& context);
    void AllocateTestData(const NTracing::TTraceContextPtr& traceContext);
    void BuildOrchid(NYson::IYsonConsumer* consumer);
    void SetupTracing(const NRpc::IServiceContextPtr& context);

    template <class TRequestMessage, class TResponseMessage>
    void InitContext(TApiServiceContext<TRequestMessage, TResponseMessage>* context);
    // Must be called only once per request.
    NApi::NNative::IClientPtr GetAuthenticatedClientOrThrow(
        const NRpc::IServiceContextPtr& context,
        const google::protobuf::Message* request);
    NApi::ITransactionPtr FindTransaction(
        const NApi::NNative::IClientPtr& client,
        NObjectClient::TTransactionId transactionId,
        const std::optional<NApi::TTransactionAttachOptions>& options,
        bool searchInPool = true);
    NApi::ITransactionPtr GetTransactionOrThrow(
        const NApi::NNative::IClientPtr& client,
        NObjectClient::TTransactionId transactionId,
        const std::optional<NApi::TTransactionAttachOptions>& options,
        bool searchInPool = true);

    template <class TContext, class TExecutor, class TResultHandler>
    void ExecuteCall(
        TIntrusivePtr<TContext> context,
        TExecutor&& executor,
        TResultHandler&& resultHandler);

    template <class TContext, class TExecutor>
    void ExecuteCall(
        const TIntrusivePtr<TContext>& context,
        TExecutor&& executor);

    TDetailedProfilingCountersPtr GetOrCreateDetailedProfilingCounters(
        const TDetailedProfilingCountersKey& key);

    IInvokerPtr GetGenerateTimestampsInvoker(const NRpc::NProto::TRequestHeader& /*requestHeader*/) const;

    IInvokerPtr GetStartTransactionInvoker(const NRpc::NProto::TRequestHeader& /*requestHeader*/) const;

    IInvokerPtr GetWorkerInvoker(const NRpc::NProto::TRequestHeader& requestHeader) const;

    void ProcessLookupRowsDetailedProfilingInfo(
        NProfiling::TWallTimer timer,
        const std::string& userTag,
        const NApi::TDetailedProfilingInfoPtr& detailedProfilingInfo);

    void ProcessSelectRowsDetailedProfilingInfo(
        NProfiling::TWallTimer timer,
        const std::string& userTag,
        const NApi::TDetailedProfilingInfoPtr& detailedProfilingInfo);

    void ProcessPullQueueDetailedProfilingInfo(
        NProfiling::TWallTimer timer,
        const std::string& userTag,
        const NApi::TDetailedProfilingInfoPtr& detailedProfilingInfo);

    void AdvanceQueueConsumerImpl(
        NApi::NRpcProxy::NProto::TReqAdvanceQueueConsumer* request,
        NApi::NRpcProxy::NProto::TRspAdvanceQueueConsumer* /*response*/,
        const TCtxAdvanceQueueConsumerPtr& context);

    void PullQueueConsumerImpl(
        NApi::NRpcProxy::NProto::TReqPullQueueConsumer* request,
        NApi::NRpcProxy::NProto::TRspPullQueueConsumer* /*response*/,
        const TCtxPullQueueConsumerPtr& context);

    void DoModifyRows(
        const NApi::NRpcProxy::NProto::TReqModifyRows& request,
        const std::vector<TSharedRef>& attachments,
        const NApi::ITransactionPtr& transaction);

    void WriteTableImpl(
        const auto& context,
        const auto& request,
        NApi::ITableWriterPtr tableWriter,
        const auto& finalizer);

    void PatchTableWriterOptions(TNonNullPtr<NApi::TTableWriterOptions> options);
    TFuture<bool> ValidateSignature(const NSignature::TSignaturePtr& signedValue) const;

    template <class TRequest>
    NQueryTrackerClient::TQueryTrackerServiceProxy GetQueryTrackerProxy(
        const NRpc::IServiceContextPtr& context,
        const TRequest* request);

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override;

    void ValidateFormat(const std::string& user, const NYTree::INodePtr& formatNode);

    template <typename TContext, typename TRequest>
    std::optional<NFormats::TFormat> GetFormat(
        const TContext& context,
        const TRequest& request);
};

DEFINE_REFCOUNTED_TYPE(TApiService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

#define API_SERVICE_IMPL_INL_H_
#include "api_service_impl-inl.h"
#undef API_SERVICE_IMPL_INL_H_
