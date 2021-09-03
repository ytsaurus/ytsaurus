#include "api_service.h"

#include "access_checker.h"
#include "bootstrap.h"
#include "config.h"
#include "proxy_coordinator.h"
#include "security_manager.h"
#include "private.h"

#include <yt/yt/server/lib/misc/format_manager.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/auth/config.h>
#include <yt/yt/ytlib/auth/cookie_authenticator.h>
#include <yt/yt/ytlib/auth/token_authenticator.h>
#include <yt/yt/ytlib/auth/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/api/rpc_proxy/api_service_proxy.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/protocol_version.h>
#include <yt/yt/client/api/rpc_proxy/wire_row_stream.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/arrow/arrow_row_stream_encoder.h>
#include <yt/yt/client/arrow/arrow_row_stream_decoder.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/cast.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/mpl.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NRpcProxy {

using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NArrow;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;
using namespace NCompression;
using namespace NAuth;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NProfiling;
using namespace NLogging;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

namespace {

using NYT::FromProto;
using NYT::ToProto;

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
    TTransactionalOptions* options,
    const NApi::NRpcProxy::NProto::TTransactionalOptions& proto)
{
    if (proto.has_transaction_id()) {
        FromProto(&options->TransactionId, proto.transaction_id());
    }
    if (proto.has_ping()) {
        options->Ping = proto.ping();
    }
    if (proto.has_ping_ancestors()) {
        options->PingAncestors = proto.ping_ancestors();
    }
    if (proto.has_suppress_transaction_coordinator_sync()) {
        options->SuppressTransactionCoordinatorSync = proto.suppress_transaction_coordinator_sync();
    }
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
        item.Revision = protoItem.revision();
        item.Path = protoItem.path();
    }
}

void FromProto(
    TMasterReadOptions* options,
    const NApi::NRpcProxy::NProto::TMasterReadOptions& proto)
{
    if (proto.has_read_from()) {
        options->ReadFrom = CheckedEnumCast<EMasterChannelKind>(proto.read_from());
    }
    if (proto.has_expire_after_successful_update_time()) {
        FromProto(&options->ExpireAfterSuccessfulUpdateTime, proto.expire_after_successful_update_time());
    }
    if (proto.has_expire_after_failed_update_time()) {
        FromProto(&options->ExpireAfterFailedUpdateTime, proto.expire_after_failed_update_time());
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
    TSuppressableAccessTrackingOptions* options,
    const NApi::NRpcProxy::NProto::TSuppressableAccessTrackingOptions& proto)
{
    if (proto.has_suppress_access_tracking()) {
        options->SuppressAccessTracking = proto.suppress_access_tracking();
    }
    if (proto.has_suppress_modification_tracking()) {
        options->SuppressModificationTracking = proto.suppress_modification_tracking();
    }
    if (proto.has_suppress_expiration_timeout_renewal()) {
        options->SuppressExpirationTimeoutRenewal = proto.suppress_expiration_timeout_renewal();
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
        options->ReadFrom = CheckedEnumCast<NHydra::EPeerKind>(proto.read_from());
    }
    if (proto.has_cached_sync_replicas_timeout()) {
        FromProto(&options->CachedSyncReplicasTimeout.emplace(), proto.cached_sync_replicas_timeout());
    }
}

void FromProto(
    std::optional<std::vector<TString>>* attributes,
    const NApi::NRpcProxy::NProto::TAttributeKeys& protoAttributes)
{
    if (protoAttributes.all()) {
        attributes->reset();
    } else {
        *attributes = NYT::FromProto<std::vector<TString>>(protoAttributes.columns());
    }
}

void FromProto(
    std::optional<THashSet<TString>>* attributes,
    const NApi::NRpcProxy::NProto::TAttributeKeys& protoAttributes)
{
    if (protoAttributes.all()) {
        attributes->reset();
    } else {
        attributes->emplace();
        NYT::CheckedHashSetFromProto(&(**attributes), protoAttributes.columns());
    }
}

template <class TRequest>
void SetMutatingOptions(
    TMutatingOptions* options,
    const TRequest* request,
    const IServiceContext* context)
{
    if (request->has_mutating_options()) {
        FromProto(options, request->mutating_options());
    }
    const auto& header = context->RequestHeader();
    if (header.retry()) {
        options->Retry = true;
    }
}

TServiceDescriptor GetServiceDescriptor()
{
    return TServiceDescriptor(NApi::NRpcProxy::ApiServiceName)
        .SetProtocolVersion({
            YTRpcProxyProtocolVersionMajor,
            YTRpcProxyServerProtocolVersionMinor
        });
}

IRowStreamEncoderPtr CreateRowStreamEncoder(
    NApi::NRpcProxy::NProto::ERowsetFormat format,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable)
{
    switch (format) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
            return CreateWireRowStreamEncoder(std::move(nameTable));

        case NApi::NRpcProxy::NProto::RF_ARROW:
            return CreateArrowRowStreamEncoder(std::move(schema), std::move(nameTable));

        default:
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(format));
    }
}

IRowStreamDecoderPtr CreateRowStreamDecoder(
    NApi::NRpcProxy::NProto::ERowsetFormat format,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable)
{
    switch (format) {
        case NApi::NRpcProxy::NProto::RF_YT_WIRE:
            return CreateWireRowStreamDecoder(std::move(nameTable));

        case NApi::NRpcProxy::NProto::RF_ARROW:
            return CreateArrowRowStreamDecoder(std::move(schema), std::move(nameTable));

        default:
            THROW_ERROR_EXCEPTION("Unsupported rowset format %Qv",
                NApi::NRpcProxy::NProto::ERowsetFormat_Name(format));
    }
}

bool IsColumnarRowsetFormat(NApi::NRpcProxy::NProto::ERowsetFormat format)
{
    return format == NApi::NRpcProxy::NProto::RF_ARROW;
}

struct TDetailedTableProfilingCounters
{
    explicit TDetailedTableProfilingCounters(const NProfiling::TProfiler& profiler)
        : LookupDuration(profiler.Histogram(
            "/lookup_duration",
            TDuration::MicroSeconds(1),
            TDuration::Seconds(10)))
        , SelectDuration(profiler.Histogram(
            "/select_duration",
            TDuration::MicroSeconds(1),
            TDuration::Seconds(10)))
    { }

    //! Histograms.
    NProfiling::TEventTimer LookupDuration;
    NProfiling::TEventTimer SelectDuration;
};

//! This context extends standard typed service context. By this moment it is used for structured
//! logging reasons.
template <class TRequestMessage, class TResponseMessage>
class TApiServiceContext
    : public TTypedServiceContext<TRequestMessage, TResponseMessage>
{
public:
    // For most cases the most important request field is "path". If it is present in request message,
    // we want to see it in the structured log.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, RequestPath);

public:
    using TTypedServiceContext<TRequestMessage, TResponseMessage>::TTypedServiceContext;

    void SetupMainMessage(TYsonString requestYson)
    {
        EmitMain_ = true;
        RequestYson_ = std::move(requestYson);
    }

    void SetupErrorMessage()
    {
        EmitError_ = true;
    }

    void LogStructured() const
    {
        if (EmitMain_) {
            DoEmitMain();
        }
        if (EmitError_) {
            DoEmitError();
        }
    }

private:
    //! True if message should be emitted to main topic.
    bool EmitMain_ = false;
    // YSON-serialized request body. This field may be really heavy.
    std::optional<TYsonString> RequestYson_;

    //! True if message should be emitted to error topic provided that request indeed resulted in error.
    bool EmitError_ = false;

    TError GetFinalError() const
    {
        return this->IsCanceled() ? MakeCanceledError() : this->GetError();
    }

    void DoEmitMain() const
    {
        // This topic is a complete verbose structured logging, similar to HTTP proxy structured logging.
        // Note that message contains full request body encoded in YSON, so messages may be really heavy.
        // At production clusters, messages from this topic should not be emitted for the most frequently
        // invoked methods like LookupRows or ModifyRows.

        const auto& header = this->GetRequestHeader();
        const auto& credentialsExt = header.GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        auto hashedCredentials = NAuth::HashCredentials(credentialsExt);
        const auto& traceContext = this->GetTraceContext();

        LogStructuredEventFluently(RpcProxyStructuredLoggerMain, ELogLevel::Info)
            .Item("request_id").Value(this->GetRequestId())
            .Item("endpoint").Value(this->GetEndpointAttributes())
            .Item("method").Value(this->GetMethod())
            .OptionalItem("path", RequestPath_)
            .Item("request").Value(RequestYson_)
            .Item("identity").Value(this->GetAuthenticationIdentity())
            .Item("credentials").Value(hashedCredentials)
            .Item("is_retry").Value(this->IsRetry())
            .OptionalItem("mutation_id", this->GetMutationId())
            .OptionalItem("realm_id", this->GetRealmId())
            // Zero-value corresponds to default tos level, so OptionalItem works reasonably,
            // producing item only if tos level is different from default one.
            .OptionalItem("tos_level", header.tos_level())
            .DoIf(static_cast<bool>(traceContext), [&] (auto fluent) {
                fluent
                    .Item("trace_id").Value(traceContext->GetTraceId());
            })
            .Item("error").Value(GetFinalError())
            .OptionalItem("user_agent", YT_PROTO_OPTIONAL(header, user_agent))
            .OptionalItem("request_body_size", GetMessageBodySize(this->GetRequestMessage()))
            .OptionalItem("request_attachment_total_size", GetTotalMessageAttachmentSize(this->GetRequestMessage()))
            .DoIf(!this->IsCanceled(), [&] (auto fluent) {
                fluent
                    .OptionalItem("response_body_size", GetMessageBodySize(this->GetResponseMessage()))
                    .OptionalItem("response_attachment_total_size", GetTotalMessageAttachmentSize(this->GetResponseMessage()));
            })
            .OptionalItem("client_start_time", this->GetStartTime())
            .OptionalItem("timeout", this->GetTimeout())
            .Item("arrive_instant").Value(this->GetArriveInstant())
            .Item("wait_time").Value(this->GetWaitDuration())
            .Item("execution_time").Value(this->GetExecutionDuration())
            .Item("finish_instant").Value(this->GetFinishInstant())
            .OptionalItem("cpu_time", this->GetTraceContextTime());
            // TODO(max42): bundles and table paths for dynamic table transactional commands.
    }

    void DoEmitError() const
    {
        // This topic is designated for (primarily dyntable) error analytics reasons.
        // It is expected to be significantly lighter than main topic, so it is enabled
        // for all methods by default. Messages are emitted only for requests resulted
        // in errors.

        auto error = GetFinalError();
        if (error.IsOK()) {
            return;
        }

        LogStructuredEventFluently(RpcProxyStructuredLoggerError, ELogLevel::Info)
            .Item("request_id").Value(this->GetRequestId())
            .Item("endpoint").Value(this->GetEndpointAttributes())
            .Item("method").Value(this->GetMethod())
            .OptionalItem("path", RequestPath_)
            .Item("identity").Value(this->GetAuthenticationIdentity())
            .Item("error").Value(error);
            // TODO(max42): YT-15042. Add error skeleton.
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TApiService)

class TApiService
    : public TServiceBase
{
public:
    template <class TRequestMessage, class TResponseMessage>
    using TTypedServiceContextImpl = TApiServiceContext<TRequestMessage, TResponseMessage>;

    TApiService(
        IBootstrap* bootstrap,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        IStickyTransactionPoolPtr stickyTransactionPool)
        : TServiceBase(
            bootstrap->GetWorkerInvoker(),
            GetServiceDescriptor(),
            std::move(logger),
            NullRealmId,
            bootstrap->GetRpcAuthenticator())
        , Bootstrap_(bootstrap)
        , Profiler_(std::move(profiler))
        , Config_(Bootstrap_->GetConfigApiService())
        , Coordinator_(Bootstrap_->GetProxyCoordinator())
        , AccessChecker_(Bootstrap_->GetAccessChecker())
        , SecurityManager_(Config_->SecurityManager, Bootstrap_, Logger)
        , StickyTransactionPool_(stickyTransactionPool
            ? stickyTransactionPool
            : CreateStickyTransactionPool(Logger))
        , AuthenticatedClientCache_(New<NApi::NNative::TClientCache>(
            Config_->ClientCache,
            Bootstrap_->GetNativeConnection()))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DetachTransaction));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExistsNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MultisetAttributesNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LockNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnlockNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CopyNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MoveNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LinkNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ConcatenateNodes));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(MountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnmountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemountTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FreezeTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnfreezeTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReshardTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReshardTableAutomatic));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TrimTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BalanceTabletCells));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompleteOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateOperationParameters));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListOperations));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListJobs));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpJobContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInput)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInputPaths));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobFailContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortJob));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(VersionedLookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MultiLookup));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectRows)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExplainQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetInSyncReplicas));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfos));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(BatchModifyRows));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateObject));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableMountInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTablePivotKeys));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(AddMember));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveMember));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckPermission));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckPermissionByAcl));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TransferAccountResources));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadFile)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteFile)
            .SetStreamingEnabled(true)
            .SetCancelable(true));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadJournal)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteJournal)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TruncateJournal));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadTable)
            .SetStreamingEnabled(true)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteTable)
            .SetStreamingEnabled(true)
            .SetCancelable(true));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetFileFromCache));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutFileToCache));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetColumnarStatistics));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckClusterLiveness));

        DeclareServerFeature(ERpcProxyFeature::GetInSyncWithoutKeys);

        if (!Bootstrap_->GetConfigAuthenticationManager()->RequireAuthentication) {
            GetOrCreateClient(NSecurityClient::RootUserName);
        }
    }

private:
    IBootstrap* const Bootstrap_;
    const NProfiling::TProfiler Profiler_;
    const TApiServiceConfigPtr Config_;
    const IProxyCoordinatorPtr Coordinator_;
    const IAccessCheckerPtr AccessChecker_;

    TSecurityManager SecurityManager_;
    const IStickyTransactionPoolPtr StickyTransactionPool_;
    const NNative::TClientCachePtr AuthenticatedClientCache_;

    NConcurrency::TSyncMap<TYPath, TDetailedTableProfilingCounters> DetailedTableProfilingCountersMap_;


    NNative::IClientPtr GetOrCreateClient(const TString& user)
    {
        auto identity = NRpc::TAuthenticationIdentity(user);
        auto options = TClientOptions::FromAuthenticationIdentity(identity);
        return AuthenticatedClientCache_->Get(identity, options);
    }

    void SetupTracing(const IServiceContextPtr& context)
    {
        auto traceContext = NTracing::GetCurrentTraceContext();
        if (!traceContext) {
            return;
        }

        if (Config_->ForceTracing) {
            traceContext->SetSampled();
        }

        const auto& identity = context->GetAuthenticationIdentity();
        Bootstrap_->GetTraceSampler()->SampleTraceContext(identity.User, traceContext);

        if (traceContext->IsRecorded()) {
            traceContext->AddTag("user", identity.User);
            if (identity.UserTag != identity.User) {
                traceContext->AddTag("user_tag", identity.UserTag);
            }
        }
    }

    template <class TRequestMessage, class TResponseMessage>
    void InitContext(const TIntrusivePtr<TApiServiceContext<TRequestMessage, TResponseMessage>>& context)
    {
        using TContext = NYT::NRpcProxy::TApiServiceContext<TRequestMessage, TResponseMessage>;

        // First, recover request path from the typed request context using the incredible power of C++20 concepts.
        std::optional<TString> requestPath;
        if constexpr (requires { context->Request().path(); }) {
            requestPath.emplace(context->Request().path());
        }

        context->SetRequestPath(std::move(requestPath));

        // Then, connect it to the typed context using subscriptions for reply and cancel signals.
        context->SubscribeReplied(BIND(&TContext::LogStructured, MakeWeak(context)));
        context->SubscribeCanceled(BIND(&TContext::LogStructured, MakeWeak(context)));

        // Finally, setup structured logging messages to be emitted.

        auto shouldEmit = [method = context->GetMethod()] (const TStructuredLoggingTopicConfigPtr& config) {
            return config->Enable && !config->SuppressedMethods.contains(method);
        };

        // NB: we try to do heavy work only if we are actually going to omit corresponding message. Conserve priceless CPU time.
        if (shouldEmit(Config_->StructuredLoggingMainTopic)) {
            TString requestYson;
            TStringOutput requestOutput(requestYson);
            TYsonWriter requestYsonWriter(&requestOutput, EYsonFormat::Text);
            TProtobufParserOptions parserOptions{
                .SkipUnknownFields = true,
            };
            WriteProtobufMessage(&requestYsonWriter, context->Request(), parserOptions);

            context->SetupMainMessage(TYsonString(std::move(requestYson)));
        }

        if (shouldEmit(Config_->StructuredLoggingErrorTopic)) {
            context->SetupErrorMessage();
        }
    }

    NNative::IClientPtr GetAuthenticatedClientOrThrow(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request)
    {
        SetupTracing(context);

        const auto& user = context->GetAuthenticationIdentity().User;

        THROW_ERROR_EXCEPTION_IF_FAILED(AccessChecker_->ValidateAccess(user));

        SecurityManager_.ValidateUser(user);

        Coordinator_->ValidateOperable();

        // Pretty-printing Protobuf requires a bunch of effort, so we make it conditional.
        if (Config_->VerboseLogging) {
            YT_LOG_DEBUG("RequestId: %v, RequestBody: %v",
                context->GetRequestId(),
                request->ShortDebugString());
        }

        auto client = GetOrCreateClient(user);
        if (!client) {
            THROW_ERROR_EXCEPTION(
                "No client found for user %Qv",
                user);
        }

        return client;
    }

    ITransactionPtr FindTransaction(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request,
        TTransactionId transactionId,
        const std::optional<TTransactionAttachOptions>& options,
        bool searchInPool = true)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        ITransactionPtr transaction;
        if (searchInPool) {
            transaction = StickyTransactionPool_->FindTransactionAndRenewLease(transactionId);
        }
        // Don't waste time trying to attach to tablet transactions.
        if (!transaction && options && IsMasterTransactionId(transactionId)) {
            transaction = client->AttachTransaction(transactionId, *options);
        }

        return transaction;
    }

    ITransactionPtr GetTransactionOrThrow(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request,
        TTransactionId transactionId,
        const std::optional<TTransactionAttachOptions>& options,
        bool searchInPool = true)
    {
        auto transaction = FindTransaction(
            context,
            request,
            transactionId,
            options,
            searchInPool);
        if (!transaction) {
            NTransactionServer::ThrowNoSuchTransaction(transactionId);
        }
        return transaction;
    }

    class TExecuteCallSessionBase
        : public TRefCounted
    {
    protected:
        const TApiServicePtr ApiService_;
        const NLogging::TLogger& Logger;
        const IServiceContextPtr Context_;

        TExecuteCallSessionBase(
            TApiServicePtr apiService,
            IServiceContextPtr context)
            : ApiService_(std::move(apiService))
            , Logger(ApiService_->Logger)
            , Context_(std::move(context))
        { }

        void HandleError(const TError& error)
        {
            Context_->Reply(TError(error.GetCode(), "Internal RPC call failed")
                << error);
        }

        virtual void Run() = 0;
    };

    template <class TContext, class TExecutor, class TResultHandler>
    class TExecuteCallSession
        : public TExecuteCallSessionBase
    {
    public:
        TExecuteCallSession(
            TApiServicePtr apiService,
            TIntrusivePtr<TContext> context,
            TExecutor&& executor,
            TResultHandler&& resultHandler)
            : TExecuteCallSessionBase(
                apiService,
                context->GetUnderlyingContext())
            , Context_(std::move(context))
            , Executor_(std::move(executor))
            , ResultHandler_(std::move(resultHandler))
        { }

        void Run() override
        {
            auto future = Executor_();

            using TResult = typename decltype(future)::TValueType;

            future.Subscribe(
                BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TResult>& resultOrError) {
                    if (!resultOrError.IsOK()) {
                        HandleError(resultOrError);
                        return;
                    }

                    try {
                        if constexpr(std::is_same_v<TResult, void>) {
                            ResultHandler_(Context_);
                        } else {
                            ResultHandler_(Context_, resultOrError.Value());
                        }
                        Context_->Reply();
                    } catch (const std::exception& ex) {
                        HandleError(ex);
                    } catch (const TFiberCanceledException&) {
                        // Intentially do nothing.
                    }
                }));

            Context_->SubscribeCanceled(BIND([future = std::move(future)] {
                future.Cancel(MakeCanceledError());
            }));
        }

    private:
        const TIntrusivePtr<TContext> Context_;
        const TExecutor Executor_;
        const TResultHandler ResultHandler_;
    };

    template <class TContext, class TExecutor, class TResultHandler>
    void ExecuteCall(
        TIntrusivePtr<TContext> context,
        TExecutor&& executor,
        TResultHandler&& resultHandler)
    {
        New<TExecuteCallSession<TContext, TExecutor, TResultHandler>>(
            this,
            std::move(context),
            std::move(executor),
            std::move(resultHandler))
        ->Run();
    }

    template <class TContext, class TExecutor>
    void ExecuteCall(
        const TIntrusivePtr<TContext>& context,
        TExecutor&& executor)
    {
        ExecuteCall(
            context,
            std::move(executor),
            [] (const TIntrusivePtr<TContext>& /*context*/) { });
    }


    TDetailedTableProfilingCounters* GetOrCreateDetailedTableProfilingCounters(const TYPath& path)
    {
        return DetailedTableProfilingCountersMap_.FindOrInsert(
            path,
            [&] {
                return TDetailedTableProfilingCounters(Profiler_
                    .WithPrefix("/detailed_table_statistics")
                    .WithTag("table_path", path)
                    .WithSparse());
            })
            .first;
    }


    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GenerateTimestamps)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto count = request->count();

        context->SetRequestInfo("Count: %v",
            count);

        const auto& timestampProvider = Bootstrap_->GetNativeConnection()->GetTimestampProvider();

        ExecuteCall(
            context,
            [=] {
                return timestampProvider->GenerateTimestamps(count);
            },
            [] (const auto& context, const TTimestamp& timestamp) {
                auto* response = &context->Response();
                response->set_timestamp(timestamp);

                context->SetResponseInfo("Timestamp: %llx",
                    timestamp);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartTransaction)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        if (!request->sticky() && request->type() == NApi::NRpcProxy::NProto::ETransactionType::TT_TABLET) {
            THROW_ERROR_EXCEPTION("Tablet transactions must be sticky");
        }

        auto transactionType = FromProto<NTransactionClient::ETransactionType>(request->type());

        TTransactionStartOptions options;
        if (request->has_timeout()) {
            options.Timeout = FromProto<TDuration>(request->timeout());
        }
        if (request->has_deadline()) {
            options.Deadline = FromProto<TInstant>(request->deadline());
        }
        if (request->has_id()) {
            FromProto(&options.Id, request->id());
        }
        if (request->has_parent_id()) {
            FromProto(&options.ParentId, request->parent_id());
        }
        options.AutoAbort = false;
        options.Sticky = request->sticky();
        options.Ping = request->ping();
        options.PingAncestors = request->ping_ancestors();
        options.Atomicity = CheckedEnumCast<NTransactionClient::EAtomicity>(request->atomicity());
        options.Durability = CheckedEnumCast<NTransactionClient::EDurability>(request->durability());
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }
        options.PrerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        if (request->has_start_timestamp()) {
            options.StartTimestamp = request->start_timestamp();
        }

        context->SetRequestInfo("TransactionType: %v, TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, "
            "Timeout: %v, Deadline: %v, AutoAbort: %v, "
            "Sticky: %v, Ping: %v, PingAncestors: %v, Atomicity: %v, Durability: %v, StartTimestamp: %v",
            transactionType,
            options.Id,
            options.ParentId,
            options.PrerequisiteTransactionIds,
            options.Timeout,
            options.Deadline,
            options.AutoAbort,
            options.Sticky,
            options.Ping,
            options.PingAncestors,
            options.Atomicity,
            options.Durability,
            options.StartTimestamp);

        ExecuteCall(
            context,
            [=] {
                return client->StartTransaction(transactionType, options);
            },
            [=, this_ = MakeStrong(this)] (const auto& context, const auto& transaction) {
                auto* response = &context->Response();
                ToProto(response->mutable_id(), transaction->GetId());
                response->set_start_timestamp(transaction->GetStartTimestamp());

                if (options.Sticky) {
                    StickyTransactionPool_->RegisterTransaction(transaction);
                }

                context->SetResponseInfo("TransactionId: %v, StartTimestamp: %v",
                    transaction->GetId(),
                    transaction->GetStartTimestamp());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PingTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = false;
        attachOptions.PingAncestors = request->ping_ancestors();

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            attachOptions);

        ExecuteCall(
            context,
            [=] {
                return transaction->Ping();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CommitTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionCommitOptions options;
        options.AdditionalParticipantCellIds = FromProto<std::vector<TCellId>>(request->additional_participant_cell_ids());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("TransactionId: %v, AdditionalParticipantCellIds: %v, PrerequisiteTransactionIds: %v",
            transactionId,
            options.PrerequisiteTransactionIds,
            options.AdditionalParticipantCellIds);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        ExecuteCall(
            context,
            [=] {
                return transaction->Commit(options);
            },
            [] (const auto& context, const TTransactionCommitResult& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_commit_timestamps(), result.CommitTimestamps);

                context->SetResponseInfo("CommitTimestamps: %v",
                    result.CommitTimestamps);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FlushTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        ExecuteCall(
            context,
            [=] {
                return transaction->Flush();
            },
            [&] (const auto& context, const TTransactionFlushResult& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_participant_cell_ids(), result.ParticipantCellIds);

                context->SetResponseInfo("ParticipantCellIds: %v",
                    result.ParticipantCellIds);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        // TODO(sandello): options are ignored
        ExecuteCall(
            context,
            [=] {
                return transaction->Abort();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AttachTransaction)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        TTransactionAttachOptions options;
        if (request->has_ping_period()) {
            options.PingPeriod = TDuration::FromValue(request->ping_period());
        }
        if (request->has_ping()) {
            options.Ping = request->ping();
        }
        if (request->has_ping_ancestors()) {
            options.PingAncestors = request->ping_ancestors();
        }

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            options,
            false /* searchInPool */);

        response->set_type(static_cast<NApi::NRpcProxy::NProto::ETransactionType>(transaction->GetType()));
        response->set_start_timestamp(transaction->GetStartTimestamp());
        response->set_atomicity(static_cast<NApi::NRpcProxy::NProto::EAtomicity>(transaction->GetAtomicity()));
        response->set_durability(static_cast<NApi::NRpcProxy::NProto::EDurability>(transaction->GetDurability()));
        response->set_timeout(static_cast<i64>(transaction->GetTimeout().GetValue()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DetachTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v",
            transactionId);

        StickyTransactionPool_->UnregisterTransaction(transactionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateObject)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto type = FromProto<EObjectType>(request->type());
        TCreateObjectOptions options;
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }

        context->SetRequestInfo("Type: %v, IgnoreExisting: %v",
            type,
            options.IgnoreExisting);

        ExecuteCall(
            context,
            [=] {
                return client->CreateObject(type, options);
            },
            [] (const auto& context, NObjectClient::TObjectId objectId) {
                auto* response = &context->Response();
                ToProto(response->mutable_object_id(), objectId);

                context->SetResponseInfo("ObjectId: %v", objectId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTableMountInfo)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<TYPath>(request->path());

        context->SetRequestInfo("Path: %v", path);

        const auto& tableMountCache = client->GetTableMountCache();
        ExecuteCall(
            context,
            [=] {
                return tableMountCache->GetTableInfo(path);
            },
            [] (const auto& context, const TTableMountInfoPtr& tableMountInfo) {
                auto* response = &context->Response();

                ToProto(response->mutable_table_id(), tableMountInfo->TableId);
                const auto& primarySchema = tableMountInfo->Schemas[ETableSchemaKind::Primary];
                ToProto(response->mutable_schema(), primarySchema);
                for (const auto& tabletInfoPtr : tableMountInfo->Tablets) {
                    ToProto(response->add_tablets(), *tabletInfoPtr);
                }

                response->set_dynamic(tableMountInfo->Dynamic);
                ToProto(response->mutable_upstream_replica_id(), tableMountInfo->UpstreamReplicaId);
                for (const auto& replica : tableMountInfo->Replicas) {
                    auto* protoReplica = response->add_replicas();
                    ToProto(protoReplica->mutable_replica_id(), replica->ReplicaId);
                    protoReplica->set_cluster_name(replica->ClusterName);
                    protoReplica->set_replica_path(replica->ReplicaPath);
                    protoReplica->set_mode(static_cast<i32>(replica->Mode));
                }

                context->SetResponseInfo("Dynamic: %v, TabletCount: %v, ReplicaCount: %v",
                    tableMountInfo->Dynamic,
                    tableMountInfo->Tablets.size(),
                    tableMountInfo->Replicas.size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTablePivotKeys)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<TYPath>(request->path());

        context->SetRequestInfo("Path: %v", path);

        ExecuteCall(
            context,
            [=] {
                return client->GetTablePivotKeys(path);
            },
            [] (const auto& context, const TYsonString& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // CYPRESS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExistsNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TNodeExistsOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->NodeExists(path, options);
            },
            [] (const auto& context, const bool& result) {
                auto* response = &context->Response();
                response->set_exists(result);

                context->SetResponseInfo("Exists: %v",
                    result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TGetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        }
        if (request->has_max_size()) {
            options.MaxSize = request->max_size();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->GetNode(path, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TListNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        }
        if (request->has_max_size()) {
            options.MaxSize = request->max_size();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->ListNode(path, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto type = CheckedEnumCast<NObjectClient::EObjectType>(request->type());

        TCreateNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_ignore_type_mismatch()) {
            options.IgnoreTypeMismatch = request->ignore_type_mismatch();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, Type: %v",
            path,
            type);

        ExecuteCall(
            context,
            [=] {
                return client->CreateNode(path, type, options);
            },
            [] (const auto& context, NCypressClient::TNodeId nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TRemoveNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveNode(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto value = TYsonString(request->value());

        TSetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->SetNode(path, value, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultisetAttributesNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        auto attributes = GetEphemeralNodeFactory()->CreateMap();
        for (const auto& protoSubrequest : request->subrequests()) {
            attributes->AddChild(
                protoSubrequest.attribute(),
                ConvertToNode(TYsonString(protoSubrequest.value())));
        }

        TMultisetAttributesNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, Attributes: %v",
            path,
            attributes->GetKeys());

        ExecuteCall(
            context,
            [=] {
                return client->MultisetAttributesNode(path, attributes, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto mode = CheckedEnumCast<NCypressClient::ELockMode>(request->mode());

        TLockNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_waitable()) {
            options.Waitable = request->waitable();
        }
        if (request->has_child_key()) {
            options.ChildKey = request->child_key();
        }
        if (request->has_attribute_key()) {
            options.AttributeKey = request->attribute_key();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, Mode: %v",
            path,
            mode);

        ExecuteCall(
            context,
            [=] {
                return client->LockNode(path, mode, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), result.NodeId);
                ToProto(response->mutable_lock_id(), result.LockId);
                response->set_revision(result.Revision);

                context->SetResponseInfo("NodeId: %v, LockId: %v, Revision: %llx",
                    result.NodeId,
                    result.LockId,
                    result.Revision);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnlockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnlockNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v", path);

        ExecuteCall(
            context,
            [=] {
                return client->UnlockNode(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CopyNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TCopyNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_preserve_account()) {
            options.PreserveAccount = request->preserve_account();
        }
        if (request->has_preserve_creation_time()) {
            options.PreserveCreationTime = request->preserve_creation_time();
        }
        if (request->has_preserve_modification_time()) {
            options.PreserveModificationTime = request->preserve_modification_time();
        }
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
        }
        if (request->has_preserve_expiration_timeout()) {
            options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
        }
        if (request->has_preserve_owner()) {
            options.PreserveOwner = request->preserve_owner();
        }
        if (request->has_preserve_acl()) {
            options.PreserveAcl = request->preserve_acl();
        }
        if (request->has_pessimistic_quota_check()) {
            options.PessimisticQuotaCheck = request->pessimistic_quota_check();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->CopyNode(srcPath, dstPath, options);
            },
            [] (const auto& context, NCypressClient::TNodeId nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MoveNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TMoveNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_preserve_account()) {
            options.PreserveAccount = request->preserve_account();
        }
        if (request->has_preserve_creation_time()) {
            options.PreserveCreationTime = request->preserve_creation_time();
        }
        if (request->has_preserve_modification_time()) {
            options.PreserveModificationTime = request->preserve_modification_time();
        }
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
        }
        if (request->has_preserve_expiration_timeout()) {
            options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
        }
        if (request->has_preserve_owner()) {
            options.PreserveOwner = request->preserve_owner();
        }
        if (request->has_pessimistic_quota_check()) {
            options.PessimisticQuotaCheck = request->pessimistic_quota_check();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->MoveNode(srcPath, dstPath, options);
            },
            [] (const auto& context, const auto& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LinkNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TLinkNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_ignore_existing()) {
            options.IgnoreExisting = request->ignore_existing();
        }
        if (request->has_lock_existing()) {
            options.LockExisting = request->lock_existing();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        ExecuteCall(
            context,
            [=] {
                return client->LinkNode(
                srcPath,
                dstPath,
                options);
            },
            [] (const auto& context, const auto& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ConcatenateNodes)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto srcPaths = FromProto<std::vector<TRichYPath>>(request->src_paths());
        auto dstPath = FromProto<TRichYPath>(request->dst_path());

        TConcatenateNodesOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        options.ChunkMetaFetcherConfig = New<NChunkClient::TFetcherConfig>();

        context->SetRequestInfo("SrcPaths: %v, DstPath: %v",
            srcPaths,
            dstPath);

        if (request->has_fetcher()) {
            options.ChunkMetaFetcherConfig->NodeRpcTimeout = FromProto<TDuration>(request->fetcher().node_rpc_timeout());
        }

        ExecuteCall(
            context,
            [=] {
                return client->ConcatenateNodes(srcPaths, dstPath, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExternalizeNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto cellTag = request->cell_tag();

        TExternalizeNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Path: %v, CellTag: %v",
            path,
            cellTag);

        ExecuteCall(
            context,
            [=] {
                return client->ExternalizeNode(path, cellTag, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, InternalizeNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TInternalizeNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->InternalizeNode(path, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (NON-TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TMountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }
        FromProto(&options.TargetCellIds, request->target_cell_ids());
        if (request->has_freeze()) {
            options.Freeze = request->freeze();
        }

        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->MountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnmountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnmountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->UnmountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TRemountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->RemountTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TFreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->FreezeTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnfreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnfreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->UnfreezeTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TReshardTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }
        if (request->has_uniform()) {
            options.Uniform = request->uniform();
        }

        TFuture<void> result;
        if (request->has_tablet_count()) {
            auto tabletCount = request->tablet_count();

            context->SetRequestInfo("Path: %v, TabletCount: %v",
                path,
                tabletCount);

            ExecuteCall(
                context,
                [=] {
                    return client->ReshardTable(path, tabletCount, options);
                });
        } else {
            TWireProtocolReader reader(MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
            auto keyRange = reader.ReadUnversionedRowset(false);
            std::vector<TLegacyOwningKey> keys;
            keys.reserve(keyRange.Size());
            for (const auto& key : keyRange) {
                keys.emplace_back(key);
            }

            context->SetRequestInfo("Path: %v, Keys: %v",
                path,
                keys);

            ExecuteCall(
                context,
                [=] {
                    return client->ReshardTable(path, keys, options);
                });
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTableAutomatic)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TReshardTableAutomaticOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }
        options.KeepActions = request->keep_actions();

        ExecuteCall(
            context,
            [=] {
                return client->ReshardTableAutomatic(path, options);
            },
            [] (const auto& context, const auto& tabletActions) {
                auto* response = &context->Response();
                ToProto(response->mutable_tablet_actions(), tabletActions);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TrimTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto tabletIndex = request->tablet_index();
        auto trimmedRowCount = request->trimmed_row_count();

        TTrimTableOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("Path: %v, TabletIndex: %v, TrimmedRowCount: %v",
            path,
            tabletIndex,
            trimmedRowCount);

        ExecuteCall(
            context,
            [=] {
                return client->TrimTable(
                    path,
                    tabletIndex,
                    trimmedRowCount,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TAlterTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_schema()) {
            options.Schema = ConvertTo<TTableSchema>(TYsonString(request->schema()));
        }
        if (request->has_dynamic()) {
            options.Dynamic = request->dynamic();
        }
        if (request->has_upstream_replica_id()) {
            options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_schema_modification()) {
            options.SchemaModification = CheckedEnumCast<ETableSchemaModification>(request->schema_modification());
        }

        context->SetRequestInfo("Path: %v",
            path);

        ExecuteCall(
            context,
            [=] {
                return client->AlterTable(path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTableReplica)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());

        TAlterTableReplicaOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_enabled()) {
            options.Enabled = request->enabled();
        }
        if (request->has_mode()) {
            options.Mode = CheckedEnumCast<ETableReplicaMode>(request->mode());
        }
        if (request->has_preserve_timestamps()) {
            options.PreserveTimestamps = request->preserve_timestamps();
        }
        if (request->has_atomicity()) {
            options.Atomicity = CheckedEnumCast<EAtomicity>(request->atomicity());
        }

        context->SetRequestInfo("ReplicaId: %v, Enabled: %v, Mode: %v",
            replicaId,
            options.Enabled,
            options.Mode);

        ExecuteCall(
            context,
            [=] {
                return client->AlterTableReplica(replicaId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BalanceTabletCells)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& bundle = request->bundle();
        auto tables = FromProto<std::vector<NYPath::TYPath>>(request->movable_tables());

        TBalanceTabletCellsOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        options.KeepActions = request->keep_actions();

        TFuture<std::vector<TTabletActionId>> result;

        ExecuteCall(
            context,
            [=] {
                return client->BalanceTabletCells(bundle, tables, options);
            },
            [] (const auto& context, const auto& tabletActions) {
                auto* response = &context->Response();
                ToProto(response->mutable_tablet_actions(), tabletActions);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto type = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(request->type());
        auto specYson = TYsonString(request->spec());

        {
            auto user = context->GetAuthenticationIdentity().User;
            auto formatConfigs = Bootstrap_->GetDynamicConfigApiService()->Formats;
            TFormatManager formatManager(formatConfigs, user);
            auto specNode = ConvertToNode(specYson);
            formatManager.ValidateAndPatchOperationSpec(specNode, type);
            specYson = ConvertToYsonString(specNode);
        }

        TStartOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("OperationType: %v, Spec: %v",
            type,
            specYson);

        ExecuteCall(
            context,
            [=] {
                return client->StartOperation(type, specYson, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                context->SetResponseInfo("OperationId: %v", result);
                ToProto(response->mutable_operation_id(), result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TAbortOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_abort_message()) {
            options.AbortMessage = request->abort_message();
        }

        context->SetRequestInfo("OperationId: %v, AbortMessage: %v",
            operationIdOrAlias,
            options.AbortMessage);

        ExecuteCall(
            context,
            [=] {
                return client->AbortOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SuspendOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TSuspendOperationOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_abort_running_jobs()) {
            options.AbortRunningJobs = request->abort_running_jobs();
        }

        context->SetRequestInfo("OperationId: %v, AbortRunningJobs: %v",
            operationIdOrAlias,
            options.AbortRunningJobs);

        ExecuteCall(
            context,
            [=] {
                return client->SuspendOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TResumeOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        ExecuteCall(
            context,
            [=] {
                return client->ResumeOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CompleteOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TCompleteOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        ExecuteCall(
            context,
            [=] {
                return client->CompleteOperation(operationIdOrAlias, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UpdateOperationParameters)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        auto parameters = TYsonString(request->parameters());

        TUpdateOperationParametersOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v, Parameters: %v",
            operationIdOrAlias,
            parameters);

        ExecuteCall(
            context,
            [=] {
                return client->UpdateOperationParameters(
                    operationIdOrAlias,
                    parameters,
                    options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TGetOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->attributes_size() != 0) {
            options.Attributes.emplace();
            NYT::CheckedHashSetFromProto(&(*options.Attributes), request->attributes());
        }
        options.IncludeRuntime = request->include_runtime();
        if (request->has_maximum_cypress_progress_age()) {
            options.MaximumCypressProgressAge = FromProto<TDuration>(request->maximum_cypress_progress_age());
        }

        context->SetRequestInfo("OperationId: %v, IncludeRuntime: %v",
            operationIdOrAlias,
            options.IncludeRuntime);

        ExecuteCall(
            context,
            [=] {
                return client->GetOperation(operationIdOrAlias, options);
            },
            [] (const auto& context, const auto& operation) {
                auto* response = &context->Response();
                response->set_meta(ConvertToYsonString(operation).ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListOperations)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TListOperationsOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        if (request->has_from_time()) {
            options.FromTime = FromProto<TInstant>(request->from_time());
        }
        if (request->has_to_time()) {
            options.ToTime = FromProto<TInstant>(request->to_time());
        }
        if (request->has_cursor_time()) {
            options.CursorTime = FromProto<TInstant>(request->cursor_time());
        }
        options.CursorDirection = static_cast<EOperationSortDirection>(request->cursor_direction());
        if (request->has_user_filter()) {
            options.UserFilter = request->user_filter();
        }

        if (request->has_access_filter()) {
            options.AccessFilter = ConvertTo<TListOperationsAccessFilterPtr>(TYsonString(request->access_filter()));
        }

        if (request->has_state_filter()) {
            options.StateFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationStateFromProto(
                request->state_filter());
        }
        if (request->has_type_filter()) {
            options.TypeFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(
                request->type_filter());
        }
        if (request->has_substr_filter()) {
            options.SubstrFilter = request->substr_filter();
        }
        if (request->has_pool()) {
            options.Pool = request->pool();
        }
        if (request->has_with_failed_jobs()) {
            options.WithFailedJobs = request->with_failed_jobs();
        }

        options.ArchiveFetchingTimeout = FromProto<TDuration>(request->archive_fetching_timeout());

        options.IncludeArchive = request->include_archive();
        options.IncludeCounters = request->include_counters();
        options.Limit = request->limit();

        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        }

        options.EnableUIMode = request->enable_ui_mode();

        context->SetRequestInfo("IncludeArchive: %v, FromTime: %v, ToTime: %v, CursorTime: %v, UserFilter: %v, "
            "AccessFilter: %v, StateFilter: %v, TypeFilter: %v, SubstrFilter: %v",
            options.IncludeArchive,
            options.FromTime,
            options.ToTime,
            options.CursorTime,
            options.UserFilter,
            ConvertToYsonString(options.AccessFilter, EYsonFormat::Text),
            options.StateFilter,
            options.TypeFilter,
            options.SubstrFilter);

        ExecuteCall(
            context,
            [=] {
                return client->ListOperations(options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("OperationsCount: %v, FailedJobsCount: %v, Incomplete: %v",
                    result.Operations.size(),
                    result.FailedJobsCount,
                    result.Incomplete);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // JOBS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ListJobs)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TListJobsOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        if (request->has_type()) {
            options.Type = NYT::NApi::NRpcProxy::NProto::ConvertJobTypeFromProto(request->type());
        }
        if (request->has_state()) {
            options.State = ConvertJobStateFromProto(request->state());
        }
        if (request->has_address()) {
            options.Address = request->address();
        }
        if (request->has_with_stderr()) {
            options.WithStderr = request->with_stderr();
        }
        if (request->has_with_fail_context()) {
            options.WithFailContext = request->with_fail_context();
        }
        if (request->has_with_spec()) {
            options.WithSpec = request->with_spec();
        }
        if (request->has_with_competitors()) {
            options.WithCompetitors = request->with_competitors();
        }
        if (request->has_job_competition_id()) {
            options.JobCompetitionId = FromProto<NJobTrackerClient::TJobId>(request->job_competition_id());
        }
        if (request->has_task_name()) {
            options.TaskName = request->task_name();
        }

        options.SortField = static_cast<EJobSortField>(request->sort_field());
        options.SortOrder = static_cast<EJobSortDirection>(request->sort_order());

        options.Limit = request->limit();
        options.Offset = request->offset();

        options.IncludeCypress = request->include_cypress();
        options.IncludeControllerAgent = request->include_controller_agent();
        options.IncludeArchive = request->include_archive();

        options.DataSource = static_cast<EDataSource>(request->data_source());
        options.RunningJobsLookbehindPeriod = FromProto<TDuration>(request->running_jobs_lookbehind_period());

        context->SetRequestInfo(
            "OperationIdOrAlias: %v, Type: %v, State: %v, Address: %v, IncludeCypress: %v, "
            "IncludeControllerAgent: %v, IncludeArchive: %v, JobCompetitionId: %v, WithCompetitors: %v",
            operationIdOrAlias,
            options.Type,
            options.State,
            options.Address,
            options.IncludeCypress,
            options.IncludeControllerAgent,
            options.IncludeArchive,
            options.JobCompetitionId,
            options.WithCompetitors);

        ExecuteCall(
            context,
            [=] {
                return client->ListJobs(operationIdOrAlias, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo(
                    "CypressJobCount: %v, ControllerAgentJobCount: %v, ArchiveJobCount: %v",
                    result.CypressJobCount,
                    result.ControllerAgentJobCount,
                    result.ArchiveJobCount);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, DumpJobContext)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto path = request->path();

        TDumpJobContextOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Path: %v",
            jobId,
            path);

        ExecuteCall(
            context,
            [=] {
                return client->DumpJobContext(jobId, path, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInput)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobInputOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v", jobId);

        auto jobInputReader = WaitFor(client->GetJobInput(jobId, options))
            .ValueOrThrow();
        HandleInputStreamingRequest(context, jobInputReader);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobInputPaths)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobInputPathsOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v", jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobInputPaths(jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_paths(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobSpec)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobSpecOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.OmitNodeDirectory = request->omit_node_directory();
        options.OmitInputTableSpecs = request->omit_input_table_specs();
        options.OmitOutputTableSpecs = request->omit_output_table_specs();

        context->SetRequestInfo("JobId: %v, OmitNodeDirectory: %v, OmitInputTableSpecs: %v, OmitOutputTableSpecs: %v",
            jobId,
            options.OmitNodeDirectory,
            options.OmitInputTableSpecs,
            options.OmitOutputTableSpecs);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobSpec(jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_job_spec(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobStderr)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobStderrOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v",
            operationIdOrAlias,
            jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobStderr(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments().push_back(std::move(result));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobFailContext)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobFailContextOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v",
            operationIdOrAlias,
            jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJobFailContext(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments().push_back(std::move(result));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        }

        context->SetRequestInfo("OperationIdOrAlias: %v, JobId: %v",
            operationIdOrAlias,
            jobId);

        ExecuteCall(
            context,
            [=] {
                return client->GetJob(operationIdOrAlias, jobId, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_info(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbandonJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        TAbandonJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v", jobId);

        ExecuteCall(
            context,
            [=] {
                return client->AbandonJob(jobId, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PollJobShell)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());
        auto shellName = request->has_shell_name()
            ? std::make_optional(request->shell_name())
            : std::nullopt;

        TPollJobShellOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Parameters: %v, ShellName: %v",
            jobId,
            parameters,
            shellName);

        ExecuteCall(
            context,
            [=] {
                return client->PollJobShell(jobId, shellName, parameters, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_result(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbortJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());

        TAbortJobOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_interrupt_timeout()) {
            options.InterruptTimeout = FromProto<TDuration>(request->interrupt_timeout());
        }

        context->SetRequestInfo("JobId: %v, InterruptTimeout: %v",
            jobId,
            options.InterruptTimeout);

        ExecuteCall(
            context,
            [=] {
                return client->AbortJob(jobId, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    DEFINE_MPL_MEMBER_DETECTOR(retention_timestamp);

    template <class TContext, class TRequest, class TOptions>
    static void LookupRowsPrelude(
        const TIntrusivePtr<TContext>& context,
        const TRequest* request,
        TOptions* options)
    {
        if (request->has_tablet_read_options()) {
            FromProto(options, request->tablet_read_options());
        }

        SetTimeoutOptions(options, context.Get());

        options->Timestamp = request->timestamp();

        if constexpr (THasretention_timestampMember<TRequest>::Value) {
            options->RetentionTimestamp = request->retention_timestamp();
        }

        if (request->has_multiplexing_band()) {
            options->MultiplexingBand = CheckedEnumCast<EMultiplexingBand>(request->multiplexing_band());
        }
    }

    template <class TContext, class TRequest>
    static void LookupRowsPrologue(
        const TIntrusivePtr<TContext>& /*context*/,
        const TRequest* request,
        TNameTablePtr* nameTable,
        TSharedRange<TUnversionedRow>* keys,
        TLookupRequestOptions* options,
        const std::vector<TSharedRef>& attachments)
    {
        if (attachments.empty()) {
            THROW_ERROR_EXCEPTION("Request is missing rowset in attachments");
        }

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(attachments));

        *nameTable = rowset->GetNameTable();
        *keys = MakeSharedRange(rowset->GetRows(), rowset);

        TColumnFilter::TIndexes columnFilterIndexes;
        for (int i = 0; i < request->columns_size(); ++i) {
            columnFilterIndexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
        }
        options->ColumnFilter = request->columns_size() == 0
            ? TColumnFilter()
            : TColumnFilter(std::move(columnFilterIndexes));
        options->KeepMissingRows = request->keep_missing_rows();
        options->EnablePartialResult = request->enable_partial_result();
        options->UseLookupCache = request->use_lookup_cache();
    }

    void ProcessLookupRowsDetailedProfilingInfo(
        TWallTimer timer,
        const TDetailedProfilingInfoPtr& detailedProfilingInfo)
    {
        if (detailedProfilingInfo->EnableDetailedProfiling) {
            auto* counters = GetOrCreateDetailedTableProfilingCounters(detailedProfilingInfo->TablePath);
            counters->LookupDuration.Record(timer.GetElapsedTime());
        }
    }

    void ProcessSelectRowsDetailedProfilingInfo(
        TWallTimer timer,
        const TDetailedProfilingInfoPtr& detailedProfilingInfo)
    {
        if (detailedProfilingInfo->EnableDetailedProfiling) {
            auto* counters = GetOrCreateDetailedTableProfilingCounters(detailedProfilingInfo->TablePath);
            counters->SelectDuration.Record(timer.GetElapsedTime());
        }
    }

    template <class TResponse, class TRow>
    static std::vector<TSharedRef> PrepareRowsetForAttachment(
        TResponse* response,
        const TIntrusivePtr<IRowset<TRow>>& rowset)
    {
        return NApi::NRpcProxy::SerializeRowset(
            rowset->GetSchema(),
            rowset->GetRows(),
            response->mutable_rowset_descriptor());
    };

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;

        TLookupRowsOptions options;
        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;

        LookupRowsPrelude(
            context,
            request,
            &options);
        LookupRowsPrologue(
            context,
            request,
            &nameTable,
            &keys,
            &options,
            request->Attachments());

        context->SetRequestInfo("Path: %v, RowCount: %v, Timestamp: %v",
            request->path(),
            keys.Size(),
            options.Timestamp);
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("yt.table_path", path);
        });

        ExecuteCall(
            context,
            [=] {
                return client->LookupRows(
                    path,
                    std::move(nameTable),
                    std::move(keys),
                    options);
            },
            [=, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& rowset) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, rowset);

                ProcessLookupRowsDetailedProfilingInfo(timer, detailedProfilingInfo);

                context->SetResponseInfo("RowCount: %v, EnableDetailedProfiling: %v",
                    rowset->GetRows().Size(),
                    detailedProfilingInfo->EnableDetailedProfiling);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, VersionedLookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;

        TVersionedLookupRowsOptions options;
        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;

        LookupRowsPrelude(
            context,
            request,
            &options);
        LookupRowsPrologue(
            context,
            request,
            &nameTable,
            &keys,
            &options,
            request->Attachments());

        context->SetRequestInfo("Path: %v, RowCount: %v, Timestamp: %v",
            request->path(),
            keys.Size(),
            options.Timestamp);
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            traceContext->AddTag("yt.table_path", path);
        });

        if (request->has_retention_config()) {
            options.RetentionConfig = New<TRetentionConfig>();
            FromProto(options.RetentionConfig.Get(), request->retention_config());
        }

        ExecuteCall(
            context,
            [=] {
                return client->VersionedLookupRows(
                    path,
                    std::move(nameTable),
                    std::move(keys),
                    options);
            },
            [=, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& rowset) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, rowset);

                ProcessLookupRowsDetailedProfilingInfo(timer, detailedProfilingInfo);

                context->SetResponseInfo("RowCount: %v, EnableDetailedProfiling: %v",
                    rowset->GetRows().Size(),
                    detailedProfilingInfo->EnableDetailedProfiling);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, MultiLookup)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TWallTimer timer;

        int subrequestCount = request->subrequests_size();

        TMultiLookupOptions options;
        LookupRowsPrelude(
            context,
            request,
            &options);

        std::vector<TMultiLookupSubrequest> subrequests;
        std::vector<TDetailedProfilingInfoPtr> profilingInfos;
        subrequests.reserve(subrequestCount);
        profilingInfos.reserve(subrequestCount);

        int beginAttachmentIndex = 0;
        for (int i = 0; i < subrequestCount; ++i) {
            const auto& protoSubrequest = request->subrequests(i);

            auto& subrequest = subrequests.emplace_back();
            subrequest.Path = protoSubrequest.path();

            profilingInfos.push_back(New<TDetailedProfilingInfo>());
            subrequest.Options.DetailedProfilingInfo = profilingInfos.back();

            int endAttachmentIndex = beginAttachmentIndex + protoSubrequest.attachment_count();
            if (endAttachmentIndex > std::ssize(request->Attachments())) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Subrequest %v refers to non-existing attachment %v (out of %v)",
                    i,
                    endAttachmentIndex,
                    request->Attachments().size());
            }
            std::vector<TSharedRef> attachments{
                request->Attachments().begin() + beginAttachmentIndex,
                request->Attachments().begin() + endAttachmentIndex};

            LookupRowsPrologue(
                context,
                &protoSubrequest,
                &subrequest.NameTable,
                &subrequest.Keys,
                &subrequest.Options,
                attachments);

            beginAttachmentIndex = endAttachmentIndex;
        }
        if (beginAttachmentIndex != std::ssize(request->Attachments())) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::ProtocolError,
                "Total number of attachments is too large: expected %v, actual %v",
                beginAttachmentIndex,
                request->Attachments().size());
        }

        context->SetRequestInfo("Timestamp: %v, Subrequests: %v",
            options.Timestamp,
            MakeFormattableView(
                subrequests,
                [&] (auto* builder, const TMultiLookupSubrequest& request) {
                    builder->AppendFormat("{Path: %v, RowCount: %v}",
                        request.Path,
                        request.Keys.Size());
                }));
        NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
            for (const auto& subrequest : subrequests) {
                traceContext->AddTag("yt.table_path", subrequest.Path);
            }
        });

        ExecuteCall(
            context,
            [=] {
                return client->MultiLookup(
                    std::move(subrequests),
                    std::move(options));
            },
            [=, this_ = MakeStrong(this), profilingInfos = std::move(profilingInfos)]
            (const auto& context, const auto& rowsets) {
                auto* response = &context->Response();

                YT_VERIFY(subrequestCount == std::ssize(rowsets));

                std::vector<int> rowCounts;
                rowCounts.reserve(subrequestCount);
                for (const auto& rowset : rowsets) {
                    auto* subresponse = response->add_subresponses();
                    auto attachments = PrepareRowsetForAttachment(subresponse, rowset);
                    subresponse->set_attachment_count(attachments.size());
                    response->Attachments().insert(
                        response->Attachments().end(),
                        attachments.begin(),
                        attachments.end());
                    rowCounts.push_back(rowset->GetRows().Size());
                }

                for (const auto& detailedProfilingInfo : profilingInfos) {
                    ProcessLookupRowsDetailedProfilingInfo(timer, detailedProfilingInfo);
                }

                context->SetResponseInfo("RowCounts: %v",
                    rowCounts);
            });
    }

    template <class TRequest>
    static void FillSelectRowsOptionsBaseFromRequest(const TRequest request, TSelectRowsOptionsBase* options)
    {
        if (request->has_timestamp()) {
            options->Timestamp = request->timestamp();
        }
        if (request->has_udf_registry_path()) {
            options->UdfRegistryPath = request->udf_registry_path();
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SelectRows)
    {
        TWallTimer timer;

        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& query = request->query();

        TSelectRowsOptions options;
        SetTimeoutOptions(&options, context.Get());
        FillSelectRowsOptionsBaseFromRequest(request, &options);

        if (request->has_input_row_limit()) {
            options.InputRowLimit = request->input_row_limit();
        }
        if (request->has_output_row_limit()) {
            options.OutputRowLimit = request->output_row_limit();
        }
        if (request->has_range_expansion_limit()) {
            options.RangeExpansionLimit = request->range_expansion_limit();
        }
        if (request->has_max_subqueries()) {
            options.MaxSubqueries = request->max_subqueries();
        }
        if (request->has_allow_full_scan()) {
            options.AllowFullScan = request->allow_full_scan();
        }
        if (request->has_allow_join_without_index()) {
            options.AllowJoinWithoutIndex = request->allow_join_without_index();
        }
        if (request->has_execution_pool()) {
            options.ExecutionPool = request->execution_pool();
        }
        if (request->has_fail_on_incomplete_result()) {
            options.FailOnIncompleteResult = request->fail_on_incomplete_result();
        }
        if (request->has_verbose_logging()) {
            options.VerboseLogging = request->verbose_logging();
        }
        if (request->has_enable_code_cache()) {
            options.EnableCodeCache = request->enable_code_cache();
        }
        if (request->has_retention_timestamp()) {
            options.RetentionTimestamp = request->retention_timestamp();
        }
        // TODO: Support WorkloadDescriptor
        if (request->has_memory_limit_per_node()) {
            options.MemoryLimitPerNode = request->memory_limit_per_node();
        }
        // TODO(lukyan): Move to FillSelectRowsOptionsBaseFromRequest
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }
        auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
        options.DetailedProfilingInfo = detailedProfilingInfo;

        context->SetRequestInfo("Query: %v, Timestamp: %llx",
            query,
            options.Timestamp);

        ExecuteCall(
            context,
            [=] {
                return client->SelectRows(query, options);
            },
            [=, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
            (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments() = PrepareRowsetForAttachment(response, result.Rowset);
                ToProto(response->mutable_statistics(), result.Statistics);

                ProcessSelectRowsDetailedProfilingInfo(timer, detailedProfilingInfo);

                context->SetResponseInfo("RowCount: %v",
                    result.Rowset->GetRows().Size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ExplainQuery)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& query = request->query();

        TExplainQueryOptions options;
        SetTimeoutOptions(&options, context.Get());
        FillSelectRowsOptionsBaseFromRequest(request, &options);

        context->SetRequestInfo("Query: %v, Timestamp: %llx",
            query,
            options.Timestamp);

        ExecuteCall(
            context,
            [=] {
                return client->ExplainQuery(query, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.ToString());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetInSyncReplicas)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TGetInSyncReplicasOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_timestamp()) {
            options.Timestamp = request->timestamp();
        }

        auto rowset = request->has_rowset_descriptor()
            ? NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
                request->rowset_descriptor(),
                MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()))
            : nullptr;
        auto keyCount = rowset
            ? std::make_optional(rowset->GetRows().Size())
            : std::nullopt;

        context->SetRequestInfo("Path: %v, Timestamp: %llx, KeyCount: %v",
            path,
            options.Timestamp,
            keyCount);

        ExecuteCall(
            context,
            [=] {
                return rowset
                    ? client->GetInSyncReplicas(
                        path,
                        rowset->GetNameTable(),
                        MakeSharedRange(rowset->GetRows(), rowset),
                        options)
                    : client->GetInSyncReplicas(
                        path,
                        options);
            },
            [] (const auto& context, const std::vector<TTableReplicaId>& replicaIds) {
                auto* response = &context->Response();
                ToProto(response->mutable_replica_ids(), replicaIds);

                context->SetResponseInfo("ReplicaIds: %v",
                    replicaIds);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetTabletInfos)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto tabletIndexes = FromProto<std::vector<int>>(request->tablet_indexes());

        context->SetRequestInfo("Path: %v, TabletIndexes: %v",
            path,
            tabletIndexes);

        TGetTabletsInfoOptions options;
        SetTimeoutOptions(&options, context.Get());

        ExecuteCall(
            context,
            [=] {
                return client->GetTabletInfos(
                path,
                tabletIndexes,
                options);
            },
            [] (const auto& context, const auto& tabletInfos) {
                auto* response = &context->Response();
                for (const auto& tabletInfo : tabletInfos) {
                    auto* protoTabletInfo = response->add_tablets();
                    protoTabletInfo->set_total_row_count(tabletInfo.TotalRowCount);
                    protoTabletInfo->set_trimmed_row_count(tabletInfo.TrimmedRowCount);
                    protoTabletInfo->set_barrier_timestamp(tabletInfo.BarrierTimestamp);
                    protoTabletInfo->set_last_write_timestamp(tabletInfo.LastWriteTimestamp);

                    if (tabletInfo.TableReplicaInfos) {
                        for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                            auto* protoReplicaInfo = protoTabletInfo->add_replicas();
                            ToProto(protoReplicaInfo->mutable_replica_id(), replicaInfo.ReplicaId);
                            protoReplicaInfo->set_last_replication_timestamp(replicaInfo.LastReplicationTimestamp);
                            protoReplicaInfo->set_mode(static_cast<NApi::NRpcProxy::NProto::ETableReplicaMode>(replicaInfo.Mode));
                            protoReplicaInfo->set_current_replication_row_index(replicaInfo.CurrentReplicationRowIndex);
                        }
                    }
                }
            });
    }

    void DoModifyRows(
        const NApi::NRpcProxy::NProto::TReqModifyRows& request,
        const std::vector<TSharedRef>& attachments,
        const ITransactionPtr& transaction)
    {
        const auto& path = request.path();

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request.rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(attachments));

        auto rowsetRows = rowset->GetRows();
        auto rowsetSize = std::ssize(rowset->GetRows());

        if (rowsetSize != request.row_modification_types_size()) {
            THROW_ERROR_EXCEPTION("Row count mismatch")
                << TErrorAttribute("rowset_size", rowsetSize)
                << TErrorAttribute("row_modification_types_size", request.row_modification_types_size());
        }

        std::vector<TRowModification> modifications;
        modifications.reserve(rowsetSize);
        for (ssize_t index = 0; index < rowsetSize; ++index) {
            TLockMask lockMask;
            if (index < request.row_read_locks_size()) {
                TLockBitmap readLockMask = request.row_read_locks(index);
                for (int index = 0; index < TLockMask::MaxCount; ++index) {
                    if (readLockMask & (1u << index)) {
                        lockMask.Set(index, ELockType::SharedWeak);
                    }
                }
            } else if (index < request.row_locks_size()) {
                lockMask = TLockMask(request.row_locks(index));
            }

            modifications.push_back({
                CheckedEnumCast<ERowModificationType>(request.row_modification_types(index)),
                rowsetRows[index].ToTypeErasedRow(),
                lockMask
            });
        }

        TModifyRowsOptions options;
        if (request.has_require_sync_replica()) {
            options.RequireSyncReplica = request.require_sync_replica();
        }
        if (request.has_upstream_replica_id()) {
            FromProto(&options.UpstreamReplicaId, request.upstream_replica_id());
        }

        if (Config_->EnableModifyRowsRequestReordering &&
            request.has_sequence_number())
        {
            options.SequenceNumber = request.sequence_number();
        }

        transaction->ModifyRows(
            path,
            rowset->GetNameTable(),
            MakeSharedRange(std::move(modifications), rowset),
            options);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ModifyRows)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo(
            "TransactionId: %v, Path: %v, ModificationCount: %v",
            transactionId,
            request->path(),
            request->row_modification_types_size());

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            std::nullopt,
            /* searchInPool */ true);

        DoModifyRows(*request, request->Attachments(), transaction);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BatchModifyRows)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, BatchSize: %v",
            transactionId,
            request->part_counts_size());

        i64 attachmentCount = request->Attachments().size();
        i64 expectedAttachmentCount = 0;
        for (int partCount : request->part_counts()) {
            if (partCount < 0) {
                THROW_ERROR_EXCEPTION("Received a negative part count")
                    << TErrorAttribute("part_count", partCount);
            }
            if (partCount >= attachmentCount) {
                THROW_ERROR_EXCEPTION("Part count is too large")
                    << TErrorAttribute("part_count", partCount);
            }
            expectedAttachmentCount += partCount + 1;
        }
        if (attachmentCount != expectedAttachmentCount) {
            THROW_ERROR_EXCEPTION("Attachment count mismatch")
                << TErrorAttribute("actual_attachment_count", attachmentCount)
                << TErrorAttribute("expected_attachment_count", expectedAttachmentCount);
        }

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            FromProto<TTransactionId>(request->transaction_id()),
            std::nullopt,
            true /* searchInPool */);

        int attachmentIndex = 0;
        for (int partCount: request->part_counts()) {
            NApi::NRpcProxy::NProto::TReqModifyRows subrequest;
            if (!TryDeserializeProto(&subrequest, request->Attachments()[attachmentIndex])) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing subrequest");
            }
            ++attachmentIndex;
            std::vector<TSharedRef> attachments(
                request->Attachments().begin() + attachmentIndex,
                request->Attachments().begin() + attachmentIndex + partCount);
            DoModifyRows(subrequest, attachments, transaction);
            attachmentIndex += partCount;
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BuildSnapshot)
    {
        TBuildSnapshotOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.CellId = FromProto<TCellId>(request->cell_id());
        options.SetReadOnly = request->set_read_only();
        options.WaitForSnapshotCompletion = request->wait_for_snapshot_completion();

        context->SetRequestInfo("CellId: %v, SetReadOnly: %v, WaitForSnapshotCompletion: %v",
            options.CellId,
            options.SetReadOnly,
            options.WaitForSnapshotCompletion);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->BuildSnapshot(options);
            },
            [] (const auto& context, int snapshotId) {
                auto* response = &context->Response();
                response->set_snapshot_id(snapshotId);
                context->SetResponseInfo("SnapshotId: %v", snapshotId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GCCollect)
    {
        TGCCollectOptions options;
        SetTimeoutOptions(&options, context.Get());
        options.CellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("CellId: %v", options.CellId);

        auto client = GetAuthenticatedClientOrThrow(context, request);
        ExecuteCall(
            context,
            [=] {
                return client->GCCollect(options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // SECURITY
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AddMember)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto group = request->group();
        auto member = request->member();

        TAddMemberOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        ExecuteCall(
            context,
            [=] {
                return client->AddMember(group, member, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMember)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto group = request->group();
        auto member = request->member();

        TRemoveMemberOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        ExecuteCall(
            context,
            [=] {
                return client->RemoveMember(group, member, options);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermission)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& user = request->user();
        const auto& path = request->path();
        auto permission = CheckedEnumCast<EPermission>(request->permission());

        TCheckPermissionOptions options;
        if (request->has_columns()) {
            options.Columns = FromProto<std::vector<TString>>(request->columns().items());
        }
        SetTimeoutOptions(&options, context.Get());
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("User: %v, Path: %v, Permission: %v",
            user,
            path,
            FormatPermissions(permission));

        ExecuteCall(
            context,
            [=] {
                return client->CheckPermission(user, path, permission, options);
            },
            [] (const auto& context, const auto& checkResponse) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), checkResponse);
                if (checkResponse.Columns) {
                    ToProto(response->mutable_columns()->mutable_items(), *checkResponse.Columns);
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckPermissionByAcl)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::optional<TString> user;
        if (request->has_user()) {
            user = request->user();
        }
        auto permission = static_cast<EPermission>(request->permission());
        auto acl = ConvertToNode(TYsonString(request->acl()));

        TCheckPermissionByAclOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        options.IgnoreMissingSubjects = request->ignore_missing_subjects();

        context->SetRequestInfo("User: %v, Permission: %v",
            user,
            FormatPermissions(permission));

        ExecuteCall(
            context,
            [=] {
                return client->CheckPermissionByAcl(user, permission, acl, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TransferAccountResources)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto srcAccount = request->src_account();
        auto dstAccount = request->dst_account();
        auto resourceDelta = ConvertToNode(TYsonString(request->resource_delta()));

        TTransferAccountResourcesOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        context->SetRequestInfo("SrcAccount: %v, DstAccount: %v",
            srcAccount,
            dstAccount);

        ExecuteCall(
            context,
            [=] {
                return client->TransferAccountResources(srcAccount, dstAccount, resourceDelta, options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // FILES
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadFile)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TFileReaderOptions options;
        if (request->has_offset()) {
            options.Offset = request->offset();
        }
        if (request->has_length()) {
            options.Length = request->length();
        }
        if (request->has_config()) {
            options.Config = ConvertTo<TFileReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, Offset: %v, Length: %v",
            path,
            options.Offset,
            options.Length);

        auto fileReader = WaitFor(client->CreateFileReader(path, options))
            .ValueOrThrow();

        auto outputStream = context->GetResponseAttachmentsStream();

        NApi::NRpcProxy::NProto::TReadFileMeta meta;
        ToProto(meta.mutable_id(), fileReader->GetId());
        meta.set_revision(fileReader->GetRevision());

        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        HandleInputStreamingRequest(context, fileReader);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteFile)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<NYPath::TRichYPath>(request->path());

        TFileWriterOptions options;
        options.ComputeMD5 = request->compute_md5();
        if (request->has_config()) {
            options.Config = ConvertTo<TFileWriterConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo(
            "Path: %v, ComputeMD5: %v",
            path,
            options.ComputeMD5);

        auto fileWriter = client->CreateFileWriter(path, options);
        WaitFor(fileWriter->Open())
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (TSharedRef block) {
                WaitFor(fileWriter->Write(std::move(block)))
                    .ThrowOnError();
            },
            [&] {
                WaitFor(fileWriter->Close())
                    .ThrowOnError();
            },
            false /* feedbackEnabled */);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // JOURNALS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TJournalReaderOptions options;
        if (request->has_first_row_index()) {
            options.FirstRowIndex = request->first_row_index();
        }
        if (request->has_row_count()) {
            options.RowCount = request->row_count();
        }
        if (request->has_config()) {
            options.Config = ConvertTo<TJournalReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Path: %v, FirstRowIndex: %v, RowCount: %v",
            path,
            options.FirstRowIndex,
            options.RowCount);

        auto journalReader = client->CreateJournalReader(path, options);
        WaitFor(journalReader->Open())
            .ThrowOnError();

        HandleInputStreamingRequest(
            context,
            [&] {
                auto rows = WaitFor(journalReader->Read())
                    .ValueOrThrow();

                if (rows.empty()) {
                    return TSharedRef();
                }

                return PackRefs(rows);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TJournalWriterOptions options;
        if (request->has_config()) {
            options.Config = ConvertTo<TJournalWriterConfigPtr>(TYsonString(request->config()));
        }
        options.EnableMultiplexing = request->enable_multiplexing();
        options.EnableChunkPreallocation = request->enable_chunk_preallocation();
        options.ReplicaLagLimit = request->replica_lag_limit();

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo(
            "Path: %v, EnableMultiplexing: %v, EnableChunkPreallocation: %v, ReplicaLagLimit: %v",
            path,
            options.EnableMultiplexing,
            options.EnableChunkPreallocation,
            options.ReplicaLagLimit);

        auto journalWriter = client->CreateJournalWriter(path, options);
        WaitFor(journalWriter->Open())
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (const TSharedRef& packedRows) {
                auto rows = UnpackRefsOrThrow(packedRows);
                WaitFor(journalWriter->Write(rows))
                    .ThrowOnError();
            },
            [&] {
                WaitFor(journalWriter->Close())
                    .ThrowOnError();
            },
            true /* feedbackEnabled */);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TruncateJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto rowCount = request->row_count();

        TTruncateJournalOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, RowCount: %v",
            path,
            rowCount);

        ExecuteCall(
            context,
            [=] {
                return client->TruncateJournal(
                path,
                rowCount,
                options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReadTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<NYPath::TRichYPath>(request->path());

        NApi::TTableReaderOptions options;
        options.Unordered = request->unordered();
        options.OmitInaccessibleColumns = request->omit_inaccessible_columns();
        options.EnableTableIndex = request->enable_table_index();
        options.EnableRowIndex = request->enable_row_index();
        options.EnableRangeIndex = request->enable_range_index();
        if (request->has_config()) {
            options.Config = ConvertTo<TTableReaderConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        auto desiredRowsetFormat = request->desired_rowset_format();

        context->SetRequestInfo(
            "Path: %v, Unordered: %v, OmitInaccessibleColumns: %v, DesiredRowsetFormat: %v",
            path,
            options.Unordered,
            options.OmitInaccessibleColumns,
            NApi::NRpcProxy::NProto::ERowsetFormat_Name(desiredRowsetFormat));

        auto tableReader = WaitFor(client->CreateTableReader(path, options))
            .ValueOrThrow();

        auto encoder = CreateRowStreamEncoder(
            desiredRowsetFormat,
            tableReader->GetTableSchema(),
            tableReader->GetNameTable());

        auto outputStream = context->GetResponseAttachmentsStream();
        NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
        meta.set_start_row_index(tableReader->GetStartRowIndex());
        ToProto(meta.mutable_omitted_inaccessible_columns(), tableReader->GetOmittedInaccessibleColumns());
        ToProto(meta.mutable_schema(), tableReader->GetTableSchema());
        meta.mutable_statistics()->set_total_row_count(tableReader->GetTotalRowCount());
        ToProto(meta.mutable_statistics()->mutable_data_statistics(), tableReader->GetDataStatistics());

        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        bool finished = false;

        HandleInputStreamingRequest(
            context,
            [&] {
                if (finished) {
                    return TSharedRef();
                }

                TRowBatchReadOptions options{
                    .MaxRowsPerRead = Config_->ReadBufferRowCount,
                    .MaxDataWeightPerRead = Config_->ReadBufferDataWeight,
                    .Columnar = IsColumnarRowsetFormat(request->desired_rowset_format())
                };
                auto batch = WaitForRowBatch(tableReader, options);
                if (!batch) {
                    finished = true;
                }

                NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
                statistics.set_total_row_count(tableReader->GetTotalRowCount());
                ToProto(statistics.mutable_data_statistics(), tableReader->GetDataStatistics());

                return encoder->Encode(
                    batch ? batch : CreateEmptyUnversionedRowBatch(),
                    &statistics);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, WriteTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = FromProto<NYPath::TRichYPath>(request->path());

        NApi::TTableWriterOptions options;
        if (request->has_config()) {
            options.Config = ConvertTo<TTableWriterConfigPtr>(TYsonString(request->config()));
        }

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo(
            "Path: %v",
            path);

        auto tableWriter = WaitFor(client->CreateTableWriter(path, options))
            .ValueOrThrow();

        THashMap<NApi::NRpcProxy::NProto::ERowsetFormat, IRowStreamDecoderPtr> parserMap;
        auto getOrCreateDecoder = [&] (NApi::NRpcProxy::NProto::ERowsetFormat format) {
            auto it =  parserMap.find(format);
            if (it == parserMap.end()) {
                auto parser = CreateRowStreamDecoder(
                    format,
                    tableWriter->GetSchema(),
                    tableWriter->GetNameTable());
                it = parserMap.emplace(format, std::move(parser)).first;
            }
            return it->second;
        };

        auto outputStream = context->GetResponseAttachmentsStream();

        NApi::NRpcProxy::NProto::TWriteTableMeta meta;
        ToProto(meta.mutable_schema(), tableWriter->GetSchema());
        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            [&] (const TSharedRef& block) {
                NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
                auto payloadRef = DeserializeRowStreamBlockEnvelope(block, &descriptor, nullptr);

                ValidateRowsetDescriptor(
                    descriptor,
                    NApi::NRpcProxy::CurrentWireFormatVersion,
                    NApi::NRpcProxy::NProto::RK_UNVERSIONED);

                auto decoder = getOrCreateDecoder(descriptor.rowset_format());

                auto batch = decoder->Decode(payloadRef, descriptor);

                auto rows = batch->MaterializeRows();

                tableWriter->Write(rows);

                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            },
            [&] {
                WaitFor(tableWriter->Close())
                    .ThrowOnError();
            },
            false /* feedbackEnabled */);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // FILE CACHING
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetFileFromCache)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto md5 = request->md5();

        TGetFileFromCacheOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.CachePath = request->cache_path();
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        context->SetRequestInfo("MD5: %v, CachePath: %v",
            md5,
            options.CachePath);

        ExecuteCall(
            context,
            [=] {
                return client->GetFileFromCache(md5, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("Path: %v",
                    result.Path);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PutFileToCache)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto path = request->path();
        auto md5 = request->md5();

        TPutFileToCacheOptions options;
        SetTimeoutOptions(&options, context.Get());
        SetMutatingOptions(&options, request, context.Get());

        options.CachePath = request->cache_path();
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }
        if (request->has_master_read_options()) {
            FromProto(&options, request->master_read_options());
        }

        context->SetRequestInfo("Path: %v, MD5: %v, CachePath: %v",
            path,
            md5,
            options.CachePath);

        ExecuteCall(
            context,
            [=] {
                return client->PutFileToCache(path, md5, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("Path: %v",
                    result.Path);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // MISC
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetColumnarStatistics)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::vector<NYPath::TRichYPath> paths;
        for (const auto& protoSubPath: request->paths()) {
            paths.emplace_back(ConvertTo<NYPath::TRichYPath>(TYsonString(protoSubPath)));
        }

        TGetColumnarStatisticsOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.FetchChunkSpecConfig = New<NChunkClient::TFetchChunkSpecConfig>();
        options.FetchChunkSpecConfig->MaxChunksPerFetch =
            request->fetch_chunk_spec().max_chunk_per_fetch();
        options.FetchChunkSpecConfig->MaxChunksPerLocateRequest =
            request->fetch_chunk_spec().max_chunk_per_locate_request();

        options.FetcherConfig = New<NChunkClient::TFetcherConfig>();
        options.FetcherConfig->NodeRpcTimeout = FromProto<TDuration>(request->fetcher().node_rpc_timeout());

        options.FetcherMode = CheckedEnumCast<NTableClient::EColumnarStatisticsFetcherMode>(request->fetcher_mode());

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Paths: %v", paths);

        ExecuteCall(
            context,
            [=] {
                return client->GetColumnarStatistics(paths, options);
            },
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                NYT::ToProto(response->mutable_statistics(), result);

                context->SetResponseInfo("StatisticsCount: %v", result.size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CheckClusterLiveness)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        TCheckClusterLivenessOptions options;
        SetTimeoutOptions(&options, context.Get());

        options.CheckCypressRoot = request->check_cypress_root();

        context->SetRequestInfo("CheckCypressRoot: %v",
            options.CheckCypressRoot);

        ExecuteCall(
            context,
            [=] {
                return client->CheckClusterLiveness(options);
            });
    }

    ////////////////////////////////////////////////////////////////////////////////

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Coordinator_->GetOperableState();
    }
};

DEFINE_REFCOUNTED_TYPE(TApiService)

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateApiService(
    IBootstrap* bootstrap,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStickyTransactionPoolPtr stickyTransactionPool)
{
    return New<TApiService>(
        bootstrap,
        std::move(logger),
        std::move(profiler),
        std::move(stickyTransactionPool));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
