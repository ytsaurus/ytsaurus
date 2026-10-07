#include "api_service_impl.h"

#include "access_checker.h"
#include "multiconnection_client_cache.h"
#include "multiproxy_access_validator.h"
#include "proxy_coordinator.h"
#include "query_corpus_reporter.h"

#include <yt/yt/server/lib/misc/format_manager.h>
#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/server/lib/security_server/user_access_validator.h>

#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/client/api/rpc_proxy/protocol_version.h>

#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NFormats;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NSignature;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NServer;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

const std::string DyntableLightPoolName = "$dyntable_light";

TServiceDescriptor GetServiceDescriptor()
{
    return TServiceDescriptor(NApi::NRpcProxy::ApiServiceName)
        .SetProtocolVersion({
            YTRpcProxyProtocolVersionMajor,
            YTRpcProxyServerProtocolVersionMinor,
        });
}

IUnversionedRowsetPtr DeserializeFormatRowset(
    TTableSchemaPtr schema,
    const TFormat& format,
    const TSharedRef& data,
    const TLogger& logger)
{
    auto typeConversionConfig = ConvertTo<TTypeConversionConfigPtr>(format.Attributes());
    TBuildingValueConsumer valueConsumer(
        schema,
        logger,
        /*convertNullToEntity*/ false,
        typeConversionConfig);
    valueConsumer.SetTreatMissingAsNull(true);

    TTableOutput output(CreateParserForFormat(format, &valueConsumer));
    output.Write(data.Begin(), data.Size());
    output.Finish();

    auto rowBuffer = New<TRowBuffer>(TApiServiceBufferTag());
    auto capturedRows = rowBuffer->CaptureRows(valueConsumer.GetRows());
    auto rows = MakeSharedRange(
        std::vector<TUnversionedRow>(capturedRows.begin(), capturedRows.end()),
        std::move(rowBuffer));
    return CreateRowset(std::move(schema), std::move(rows));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TError MakeCanceledError()
{
    return TError("RPC request canceled");
}

void SetTimeoutOptions(
    TTimeoutOptions* options,
    const IServiceContext* context)
{
    options->Timeout = context->GetTimeout();
}

void FromProto(
    TPrerequisiteOptions* options,
    const NApi::NRpcProxy::NProto::TPrerequisiteOptions& proto)
{
    options->PrerequisiteTransactionIds.resize(proto.transactions_size());
    for (int i = 0; i < proto.transactions_size(); ++i) {
        const auto& protoItem = proto.transactions(i);
        auto& item = options->PrerequisiteTransactionIds[i];
        FromProto(&item, protoItem.transaction_id());
    }
    options->PrerequisiteRevisions.resize(proto.revisions_size());
    for (int i = 0; i < proto.revisions_size(); ++i) {
        const auto& protoItem = proto.revisions(i);
        options->PrerequisiteRevisions[i] = New<TPrerequisiteRevisionConfig>();
        auto& item = *options->PrerequisiteRevisions[i];
        item.Revision = FromProto<TRevision>(protoItem.revision());
        item.Path = protoItem.path();
    }
}

void FromProto(
    TMasterReadOptions* options,
    const NApi::NRpcProxy::NProto::TMasterReadOptions& proto)
{
    using NYT::FromProto;
    if (proto.has_read_from()) {
        FromProto(&options->ReadFrom, proto.read_from());
    }
    if (proto.has_expire_after_successful_update_time()) {
        FromProto(&options->ExpireAfterSuccessfulUpdateTime, proto.expire_after_successful_update_time());
    }
    if (proto.has_expire_after_failed_update_time()) {
        FromProto(&options->ExpireAfterFailedUpdateTime, proto.expire_after_failed_update_time());
    }
    if (proto.has_success_staleness_bound()) {
        FromProto(&options->SuccessStalenessBound, proto.success_staleness_bound());
    }
    if (proto.has_cache_sticky_group_size()) {
        options->CacheStickyGroupSize = proto.cache_sticky_group_size();
    }
}

void FromProto(
    TMutatingOptions* options,
    const NApi::NRpcProxy::NProto::TMutatingOptions& proto)
{
    if (proto.has_mutation_id()) {
        FromProto(&options->MutationId, proto.mutation_id());
    }
    if (proto.has_retry()) {
        options->Retry = proto.retry();
    }
}

void FromProto(
    TTabletRangeOptions* options,
    const NApi::NRpcProxy::NProto::TTabletRangeOptions& proto)
{
    if (proto.has_first_tablet_index()) {
        options->FirstTabletIndex = proto.first_tablet_index();
    }
    if (proto.has_last_tablet_index()) {
        options->LastTabletIndex = proto.last_tablet_index();
    }
}

void FromProto(
    TTabletReadOptionsBase* options,
    const NApi::NRpcProxy::NProto::TTabletReadOptions& proto)
{
    if (proto.has_read_from()) {
        options->ReadFrom = FromProto<EPeerKind>(proto.read_from());
    }
    if (proto.has_cached_sync_replicas_timeout()) {
        options->CachedSyncReplicasTimeout = FromProto<TDuration>(proto.cached_sync_replicas_timeout());
    }
}

IUnversionedRowsetPtr DeserializeRowset(
    const NApi::NRpcProxy::NProto::TRowsetDescriptor& descriptor,
    TTableSchemaPtr schema,
    const std::optional<TFormat>& format,
    const TSharedRef& data,
    const TLogger& logger)
{
    switch (descriptor.rowset_format()) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
            return NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(descriptor, data);

        case NApi::NRpcProxy::NProto::RF_FORMAT:
            if (!format) {
                THROW_ERROR_EXCEPTION("Format is missing for rowset format %Qv",
                    NApi::NRpcProxy::NProto::ERowsetFormat_Name(descriptor.rowset_format()));
            }
            return DeserializeFormatRowset(std::move(schema), *format, data, logger);

        default:
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(descriptor.rowset_format()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TDetailedProfilingCounters::TDetailedProfilingCounters(TProfiler profiler)
    : Profiler_(std::move(profiler))
    , LookupDuration_(Profiler_.TimeHistogram(
        "/lookup_duration",
        TDuration::MicroSeconds(1),
        TDuration::Seconds(10)))
    , SelectDuration_(Profiler_.TimeHistogram(
        "/select_duration",
        TDuration::MicroSeconds(1),
        TDuration::Seconds(10)))
    , PullQueueDuration_(Profiler_.TimeHistogram(
        "/pull_queue_duration",
        TDuration::MicroSeconds(1),
        TDuration::Seconds(10)))
    , LookupMountCacheWaitTime_(Profiler_.Timer("/lookup_mount_cache_wait_time"))
    , SelectMountCacheWaitTime_(Profiler_.Timer("/select_mount_cache_wait_time"))
    , PullQueueMountCacheWaitTime_(Profiler_.Timer("/pull_queue_mount_cache_wait_time"))
    , LookupPermissionCacheWaitTime_(Profiler_.Timer("/lookup_permission_cache_wait_time"))
    , SelectPermissionCacheWaitTime_(Profiler_.Timer("/select_permission_cache_wait_time"))
    , PullQueuePermissionCacheWaitTime_(Profiler_.Timer("/pull_queue_permission_cache_wait_time"))
    , WastedLookupSubrequestCount_(Profiler_.Counter("/wasted_lookup_subrequest_count"))
{ }

const TEventTimer& TDetailedProfilingCounters::LookupDurationTimer() const
{
    return LookupDuration_;
}

const TEventTimer& TDetailedProfilingCounters::SelectDurationTimer() const
{
    return SelectDuration_;
}

const TEventTimer& TDetailedProfilingCounters::PullQueueDurationTimer() const
{
    return PullQueueDuration_;
}

const TEventTimer& TDetailedProfilingCounters::LookupMountCacheWaitTimer() const
{
    return LookupMountCacheWaitTime_;
}

const TEventTimer& TDetailedProfilingCounters::SelectMountCacheWaitTimer() const
{
    return SelectMountCacheWaitTime_;
}

const TEventTimer& TDetailedProfilingCounters::PullQueueMountCacheWaitTimer() const
{
    return PullQueueMountCacheWaitTime_;
}

const TEventTimer& TDetailedProfilingCounters::LookupPermissionCacheWaitTimer() const
{
    return LookupPermissionCacheWaitTime_;
}

const TEventTimer& TDetailedProfilingCounters::SelectPermissionCacheWaitTimer() const
{
    return SelectPermissionCacheWaitTime_;
}

const TEventTimer& TDetailedProfilingCounters::PullQueuePermissionCacheWaitTimer() const
{
    return PullQueuePermissionCacheWaitTime_;
}

const TCounter& TDetailedProfilingCounters::WastedLookupSubrequestCount() const
{
    return WastedLookupSubrequestCount_;
}

TCounter* TDetailedProfilingCounters::GetRetryCounterByReason(TErrorCode reason)
{
    return RetryCounters_.FindOrInsert(
        reason,
        [&] {
            return Profiler_
                .WithTag("reason", ToString(reason))
                .Counter("/retry_count");
        })
        .first;
}

////////////////////////////////////////////////////////////////////////////////

TApiService::TApiService(
    TApiServiceConfigPtr config,
    IInvokerPtr defaultInvoker,
    TPooledInvokerProvider workerInvokerProvider,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IAuthenticatorPtr authenticator,
    IProxyCoordinatorPtr proxyCoordinator,
    IAccessCheckerPtr accessChecker,
    NTracing::TSamplerPtr traceSampler,
    NLogging::TLogger logger,
    TProfiler profiler,
    INodeMemoryTrackerPtr memoryTracker,
    IStickyTransactionPoolPtr stickyTransactionPool,
    ISignatureValidatorPtr signatureValidator,
    IQueryCorpusReporterPtr queryCorpusReporter)
    : TServiceBase(
        std::move(defaultInvoker),
        GetServiceDescriptor(),
        std::move(logger),
        TServiceOptions{
            .MemoryUsageTracker = WithCategory(memoryTracker, EMemoryCategory::Rpc),
            .Authenticator = std::move(authenticator),
        })
    , ApiServiceConfig_(config)
    , Profiler_(std::move(profiler))
    , LocalConnection_(std::move(connection))
    , ProxyCoordinator_(std::move(proxyCoordinator))
    , AccessChecker_(std::move(accessChecker))
    , TraceSampler_(std::move(traceSampler))
    , StickyTransactionPool_(stickyTransactionPool
        ? stickyTransactionPool
        : CreateStickyTransactionPool(Logger))
    , AuthenticatedClientCache_(New<TMulticonnectionClientCache>(config->ClientCache))
    , HeapProfilerTestingOptions_(config->TestingOptions
        ? config->TestingOptions->HeapProfiler
        : nullptr)
    , HeavyRequestMemoryUsageTracker_(WithCategory(memoryTracker, EMemoryCategory::HeavyRequest))
    , SignatureValidator_(std::move(signatureValidator))
    , QueryCorpusReporter_(std::move(queryCorpusReporter))
    , UserAccessValidator_(CreateUserAccessValidator(
        ApiServiceConfig_->UserAccessValidator,
        LocalConnection_,
        Logger))
    , WorkerInvokerProvider_(std::move(workerInvokerProvider))
    , SelectConsumeDataWeight_(Profiler_.Counter("/select_consume/data_weight"))
    , SelectConsumeRowCount_(Profiler_.Counter("/select_consume/row_count"))
    , SelectOutputDataWeight_(Profiler_.Counter("/select_output/data_weight"))
    , SelectOutputRowCount_(Profiler_.Counter("/select_output/row_count"))
{
    TMultiproxyMethodList methodList;

    // Read / Write markup is for multiproxy mode.
    // Rpc proxy can be configured to redirect requests for other clusters if request has corresponding header.
    // Rpc proxy can allow redirect read requests or read and write requests (or disallow redirecting completely).
    //
    // YT-24245
    RegisterTransactionMethods(&methodList);
    RegisterCypressMethods(&methodList);
    RegisterDynamicTableMethods(&methodList);
    RegisterReplicatedTableMethods(&methodList);
    RegisterOperationMethods(&methodList);
    RegisterOperationInfoMethods(&methodList);
    RegisterJobInfoMethods(&methodList);
    RegisterJobMethods(&methodList);
    RegisterQueueMethods(&methodList);
    RegisterAdminMethods(&methodList);
    RegisterSecurityMethods(&methodList);
    RegisterFileMethods(&methodList);
    RegisterJournalMethods(&methodList);
    RegisterStaticTableMethods(&methodList);
    RegisterFileCacheMethods(&methodList);
    RegisterFlowMethods(&methodList);
    RegisterQueryMethods(&methodList);
    RegisterDistributedTableMethods(&methodList);
    RegisterDistributedFileMethods(&methodList);
    RegisterShuffleMethods(&methodList);

    DeclareServerFeature(ERpcProxyFeature::GetInSyncWithoutKeys);
    DeclareServerFeature(ERpcProxyFeature::WideLocks);
    MultiproxyAccessValidator_ = CreateMultiproxyAccessValidator(std::move(methodList));
}

void TApiService::RegisterMethodForMultiproxy(
    TMultiproxyMethodList* methodList,
    EMultiproxyMethodKind methodKind,
    const TMethodDescriptor& descriptor)
{
    const auto& methodName = descriptor.Method;
    methodList->emplace_back(std::string(methodName), methodKind);
    RegisterMethod(descriptor);
}

void TApiService::OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto oldConfig = Config_.Acquire();

    YT_TLOG_DEBUG("Updating API service config")
        .With("OldConfig", ConvertToYsonString(oldConfig, EYsonFormat::Text))
        .With("NewConfig", ConvertToYsonString(config, EYsonFormat::Text));

    AuthenticatedClientCache_->Reconfigure(config->ClientCache);

    UserAccessValidator_->Reconfigure(config->UserAccessValidator);
    MultiproxyAccessValidator_->Reconfigure(config->Multiproxy);

    Config_.Store(config);
}

IYPathServicePtr TApiService::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND_NO_PROPAGATE(&TApiService::BuildOrchid, MakeStrong(this)))
        ->Via(WorkerInvokerProvider_(OrchidExecutionPoolName, DefaultExecutionTag));
}

std::optional<std::string> TApiService::GetMultiproxyTargetCluster(const IServiceContextPtr& context)
{
    const auto& header = context->GetRequestHeader();
    const auto& multiproxyTargetExt = header.GetExtension(NRpc::NProto::TMultiproxyTargetExt::multiproxy_target_ext);
    if (!multiproxyTargetExt.has_cluster()) {
        return {};
    }
    const auto& cluster = multiproxyTargetExt.cluster();
    const auto& localClusterName = LocalConnection_->GetStaticConfig()->ClusterName;
    if (cluster == localClusterName) {
        return {};
    }
    return cluster;
}

void TApiService::AllocateTestData(const TTraceContextPtr& traceContext)
{
    if (!HeapProfilerTestingOptions_ || !traceContext) {
        return;
    }

    if (HeapProfilerTestingOptions_->AllocationSize) {
        auto guard = TCurrentTraceContextGuard(traceContext);

        auto size = HeapProfilerTestingOptions_->AllocationSize.value();
        auto delay = HeapProfilerTestingOptions_->AllocationReleaseDelay.value_or(TDuration::Zero());

        MakeTestHeapAllocation(size, delay);

        YT_TLOG_DEBUG("Test heap allocation is finished")
            .With("AllocationSize", size)
            .With("AllocationReleaseDelay", delay);
    }
}

void TApiService::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMap([] (TFluentMap fluent) {
            DumpGlobalMemoryUsageSnapshot(
                fluent.GetConsumer(),
                {
                    RpcProxyUserAllocationTagKey,
                    RpcProxyMethodAllocationTagKey,
                });
        });
}

void TApiService::SetupTracing(const IServiceContextPtr& context)
{
    auto* traceContext = NTracing::TryGetCurrentTraceContext();
    if (!traceContext) {
        return;
    }

    const auto& config = Config_.Acquire();

    if (config->ForceTracing) {
        traceContext->SetSampled();
    }

    const auto& identity = context->GetAuthenticationIdentity();
    TraceSampler_->SampleTraceContext(identity.User, traceContext);

    if (traceContext->IsRecorded()) {
        traceContext->AddTag("user", identity.User);
        if (identity.UserTag != identity.User) {
            traceContext->AddTag("user_tag", identity.UserTag);
        }
    }

    if (config->EnableAllocationTags) {
        traceContext->SetAllocationTags({
            {RpcProxyUserAllocationTagKey, identity.User},
            {RpcProxyRequestIdAllocationTagKey, ToString(context->GetRequestId())},
            {RpcProxyMethodAllocationTagKey, context->RequestHeader().method()},
        });

        AllocateTestData(traceContext);
    }
}

NNative::IClientPtr TApiService::GetAuthenticatedClientOrThrow(
    const IServiceContextPtr& context,
    const google::protobuf::Message* request)
{
    SetupTracing(context);

    const auto& identity = context->GetAuthenticationIdentity();

    THROW_ERROR_EXCEPTION_IF_FAILED(AccessChecker_->CheckAccess(identity.User));

    auto multiproxyTargetCluster = GetMultiproxyTargetCluster(context);
    if (multiproxyTargetCluster) {
        MultiproxyAccessValidator_->ValidateMultiproxyAccess(*multiproxyTargetCluster, context->GetMethod());
    }
    UserAccessValidator_->ValidateUser(identity.User, multiproxyTargetCluster);

    ProxyCoordinator_->ValidateOperable();

    const auto& config = Config_.Acquire();

    // Pretty-printing Protobuf requires a bunch of effort, so we make it conditional.
    if (config->VerboseLogging) {
        YT_TLOG_DEBUG("Request body")
            .With("RequestId", context->GetRequestId())
            .With("RequestBody", request->ShortDebugString());
    }

    NApi::NNative::IConnectionPtr connection;
    if (multiproxyTargetCluster) {
        connection = WaitForFast(
            InsistentGetRemoteConnection(
                LocalConnection_,
                *multiproxyTargetCluster,
                NNative::EInsistentGetRemoteConnectionMode::WaitFirstSuccessfulSync))
            .ValueOrThrow();
    } else {
        connection = LocalConnection_;
    }

    auto client = AuthenticatedClientCache_->Get(
        multiproxyTargetCluster,
        identity,
        connection,
        NNative::TClientOptions::FromAuthenticationIdentity(identity));

    if (!client) {
        THROW_ERROR_EXCEPTION("No client found for identity %Qv", identity);
    }

    VerifyDynamicCast<IApiServiceContext*>(context.Get())->SetClient(multiproxyTargetCluster, client);

    return client;
}

ITransactionPtr TApiService::FindTransaction(
    const NNative::IClientPtr& client,
    TTransactionId transactionId,
    const std::optional<TTransactionAttachOptions>& options,
    bool searchInPool)
{
    ITransactionPtr transaction;
    if (searchInPool) {
        transaction = StickyTransactionPool_->FindTransactionAndRenewLease(transactionId);
    }
    // Attachment to a tablet transaction works via sticky transaction pool.
    // Native client AttachTransaction is only supported for master transactions.
    if (!transaction && options && IsMasterTransactionId(transactionId)) {
        transaction = client->AttachTransaction(transactionId, *options);
    }

    return transaction;
}

ITransactionPtr TApiService::GetTransactionOrThrow(
    const NNative::IClientPtr& client,
    TTransactionId transactionId,
    const std::optional<TTransactionAttachOptions>& options,
    bool searchInPool)
{
    auto transaction = FindTransaction(
        client,
        transactionId,
        options,
        searchInPool);
    if (!transaction) {
        NTransactionServer::ThrowNoSuchTransaction(transactionId);
    }
    return transaction;
}

TDetailedProfilingCountersPtr TApiService::GetOrCreateDetailedProfilingCounters(
    const TDetailedProfilingCountersKey& key)
{
    return *DetailedProfilingCountersMap_.FindOrInsert(
        key,
        [&] {
            auto profiler = Profiler_
                .WithPrefix("/detailed_table_statistics")
                .WithSparse();
            if (key.TablePath) {
                profiler = profiler
                    .WithTag("table_path", *key.TablePath);
            }
            if (key.UserTag) {
                profiler = profiler
                    .WithTag("user", *key.UserTag);
            }
            return New<TDetailedProfilingCounters>(std::move(profiler));
        })
        .first;
}

IInvokerPtr TApiService::GetStartTransactionInvoker(const NRpc::NProto::TRequestHeader& requestHeader) const
{
    auto tag = ToString(FromProto<TRequestId>(requestHeader.request_id()));
    return WorkerInvokerProvider_(DyntableLightPoolName, tag);
}

IInvokerPtr TApiService::GetWorkerInvoker(const NRpc::NProto::TRequestHeader& requestHeader) const
{
    const auto& ext = requestHeader.GetExtension(NApi::NRpcProxy::NProto::TReqFairSharePoolExt::req_fair_share_pool_ext);

    static const auto DefaultExecutionPool = "default";

    const auto& tag = ext.has_execution_tag()
        ? ext.execution_tag()
        : DefaultExecutionTag;

    const auto& poolName = ext.has_execution_pool()
        ? ext.execution_pool()
        : DefaultExecutionPool;

    return WorkerInvokerProvider_(poolName, tag);
}

IInvokerPtr TApiService::GetGenerateTimestampsInvoker(const NRpc::NProto::TRequestHeader& requestHeader) const
{
    auto tag = ToString(FromProto<TRequestId>(requestHeader.request_id()));
    return WorkerInvokerProvider_(DyntableLightPoolName, tag);
}

void TApiService::ValidateFormat(const std::string& user, const INodePtr& formatNode)
{
    const auto& config = Config_.Acquire();
    const auto& formatConfigs = config->Formats;
    TFormatManager formatManager(formatConfigs, user);
    formatManager.ValidateAndPatchFormatNode(formatNode, "format");
}

// Signature helpers.
TFuture<bool> TApiService::ValidateSignature(const TSignaturePtr& signedValue) const
{
    return SignatureValidator_->Validate(signedValue);
}

bool TApiService::IsUp(const TCtxDiscoverPtr& /*context*/)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ProxyCoordinator_->GetOperableState();
}

const TStructuredLoggingMethodDynamicConfigPtr TApiService::DefaultMethodConfig = New<TStructuredLoggingMethodDynamicConfig>();

////////////////////////////////////////////////////////////////////////////////

IApiServicePtr CreateApiService(
    TApiServiceConfigPtr config,
    IInvokerPtr defaultInvoker,
    TPooledInvokerProvider workerInvokerProvider,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IAuthenticatorPtr authenticator,
    IProxyCoordinatorPtr proxyCoordinator,
    IAccessCheckerPtr accessChecker,
    NTracing::TSamplerPtr traceSampler,
    NLogging::TLogger logger,
    TProfiler profiler,
    ISignatureValidatorPtr signatureValidator,
    INodeMemoryTrackerPtr memoryUsageTracker,
    IStickyTransactionPoolPtr stickyTransactionPool,
    IQueryCorpusReporterPtr queryCorpusReporter)
{
    YT_VERIFY(signatureValidator);
    return New<TApiService>(
        std::move(config),
        std::move(defaultInvoker),
        std::move(workerInvokerProvider),
        std::move(connection),
        std::move(authenticator),
        std::move(proxyCoordinator),
        std::move(accessChecker),
        std::move(traceSampler),
        std::move(logger),
        std::move(profiler),
        std::move(memoryUsageTracker),
        std::move(stickyTransactionPool),
        std::move(signatureValidator),
        std::move(queryCorpusReporter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
