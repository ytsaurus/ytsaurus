#include "api_service.h"
#include "proxy_coordinator.h"
#include "public.h"
#include "private.h"
#include "bootstrap.h"
#include "config.h"
#include "security_manager.h"

#include <yt/ytlib/auth/cookie_authenticator.h>
#include <yt/ytlib/auth/token_authenticator.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/client_cache.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/file_reader.h>
#include <yt/client/api/file_writer.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>
#include <yt/client/api/table_reader.h>
#include <yt/client/api/table_writer.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/rowset.h>
#include <yt/client/api/sticky_transaction_pool.h>

#include <yt/client/api/rpc_proxy/api_service_proxy.h>
#include <yt/client/api/rpc_proxy/helpers.h>
#include <yt/client/api/rpc_proxy/protocol_version.h>

#include <yt/client/chunk_client/config.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/wire_protocol.h>

#include <yt/client/transaction_client/helpers.h>
#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/cast.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/stream.h>

#include <yt/core/tracing/sampler.h>

namespace NYT::NRpcProxy {

using namespace NApi;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;
using namespace NCompression;
using namespace NAuth;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NYPath;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

namespace {

using NYT::FromProto;
using NYT::ToProto;

void SetTimeoutOptions(
    TTimeoutOptions* options,
    IServiceContext* context)
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
    if (proto.has_sticky()) {
        options->Sticky = proto.sticky();
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
        FromProto(&item.TransactionId, protoItem.transaction_id());
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
    if (proto.has_success_expiration_time()) {
        FromProto(&options->ExpireAfterSuccessfulUpdateTime, proto.success_expiration_time());
    }
    if (proto.has_failure_expiration_time()) {
        FromProto(&options->ExpireAfterFailedUpdateTime, proto.failure_expiration_time());
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
    TTabletReadOptions* options,
    const NApi::NRpcProxy::NProto::TTabletReadOptions& proto)
{
    if (proto.has_read_from()) {
        options->ReadFrom = CheckedEnumCast<NHydra::EPeerKind>(proto.read_from());
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

const TServiceDescriptor& GetDescriptor()
{
    static const auto descriptor = TServiceDescriptor(NApi::NRpcProxy::ApiServiceName)
        .SetProtocolVersion({
            YTRpcProxyProtocolVersionMajor,
            YTRpcProxyServerProtocolVersionMinor
        });
    return descriptor;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TApiService
    : public TServiceBase
{
public:
    explicit TApiService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetWorkerInvoker(),
            GetDescriptor(),
            RpcProxyLogger,
            NullRealmId,
            bootstrap->GetRpcAuthenticator())
        , Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->ApiService)
        , Coordinator_(bootstrap->GetProxyCoordinator())
        , SecurityManager_(Config_->SecurityManager, Bootstrap_)
        , StickyTransactionPool_(CreateStickyTransactionPool(Logger))
    {
        AuthenticatedClientCache_ = New<NApi::NNative::TClientCache>(
            Config_->ClientCache,
            Bootstrap_->GetNativeConnection());

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobFailContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortJob));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(VersionedLookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectRows));
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

        if (!Bootstrap_->GetConfig()->RequireAuthentication) {
            GetOrCreateClient(NSecurityClient::RootUserName);
        }
    }

private:
    const TBootstrap* Bootstrap_;
    const TApiServiceConfigPtr Config_;
    const IProxyCoordinatorPtr Coordinator_;
    TSecurityManager SecurityManager_;

    TSpinLock SpinLock_;
    NNative::TClientCachePtr AuthenticatedClientCache_;
    const IStickyTransactionPoolPtr StickyTransactionPool_;

    NNative::IClientPtr GetOrCreateClient(const TString& user)
    {
        return AuthenticatedClientCache_->GetClient(user);
    }

    TString ExtractIP(TString address)
    {
        YT_VERIFY(address.StartsWith("tcp://"));

        address = address.substr(6);
        {
            auto index = address.rfind(':');
            if (index != TString::npos) {
                address = address.substr(0, index);
            }
        }

        if (address.StartsWith("[") && address.EndsWith("]")) {
            address = address.substr(1, address.length() - 2);
        }

        return address;
    }

    void SetupTracing(const TString& user)
    {
        if (auto trace = NTracing::GetCurrentTraceContext()) {
            trace->AddTag("user", user);

            if (Config_->ForceTracing) {
                trace->SetSampled();
            }

            auto sampler = Coordinator_->GetTraceSampler();
            if (sampler->IsTraceSampled(user)) {
                trace->SetSampled();
            }
        }
    }

    NNative::IClientPtr GetAuthenticatedClientOrThrow(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request)
    {
        const auto& user = context->GetUser();
        SetupTracing(user);

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
            auto newOptions = *options;
            newOptions.Sticky = false;
            transaction = client->AttachTransaction(transactionId, newOptions);
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
            THROW_ERROR_EXCEPTION(
                NTransactionClient::EErrorCode::NoSuchTransaction,
                "No such transaction %v",
                transactionId);
        }
        return transaction;
    }

    template <class T>
    void CompleteCallWith(
        NNative::IClientPtr client,
        IServiceContextPtr context,
        TFuture<T>&& future)
    {
        future.Subscribe(
            BIND([client = std::move(client), context = std::move(context)] (const TErrorOr<T>& valueOrError) {
                if (valueOrError.IsOK()) {
                    // XXX(sandello): This relies on the typed service context implementation.
                    context->Reply(TError());
                } else {
                    context->Reply(TError(valueOrError.GetCode(), "Internal RPC call failed")
                        << TError(valueOrError));
                }
            }));
    }

    template <class TContext, class TResult, class F>
    void CompleteCallWith(
        NNative::IClientPtr client,
        const TIntrusivePtr<TContext>& context,
        TFuture<TResult>&& future,
        F&& functor)
    {
        future.Subscribe(
            BIND([client = std::move(client), context, functor = std::move(functor)] (const TErrorOr<TResult>& valueOrError) {
                if (valueOrError.IsOK()) {
                    try {
                        functor(context, valueOrError.Value());
                        // XXX(sandello): This relies on the typed service context implementation.
                        context->Reply(TError());
                    } catch (const std::exception& ex) {
                        context->Reply(TError(ex));
                    }
                } else {
                    context->Reply(TError(valueOrError.GetCode(), "Internal RPC call failed")
                        << TError(valueOrError));
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GenerateTimestamps)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto count = request->count();

        context->SetRequestInfo("Count: %v",
            count);

        const auto& timestampProvider = Bootstrap_->GetNativeConnection()->GetTimestampProvider();

        CompleteCallWith(
            client,
            context,
            timestampProvider->GenerateTimestamps(count),
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

        context->SetRequestInfo("TransactionId: %v, ParentId: %v, PrerequisiteTransactionIds: %v, Timeout: %v, Deadline: %v, AutoAbort: %v, "
            "Sticky: %v, Ping: %v, PingAncestors: %v, Atomicity: %v, Durability: %v",
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
            options.Durability);

        CompleteCallWith(
            client,
            context,
            client->StartTransaction(NTransactionClient::ETransactionType(request->type()), options),
            [options, this_ = MakeStrong(this), this] (const auto& context, const auto& transaction) {
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

        // TODO(sandello): Options!
        TTransactionPingOptions pingOptions;
        pingOptions.EnableRetries = false;
        CompleteCallWith(
            NNative::IClientPtr(),
            context,
            transaction->Ping(pingOptions));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CommitTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionCommitOptions options;
        options.AdditionalParticipantCellIds = FromProto<std::vector<TCellId>>(request->additional_participant_cell_ids());

        context->SetRequestInfo("TransactionId: %v, AdditionalParticipantCellIds: %v",
            transactionId,
            options.AdditionalParticipantCellIds);

        auto transaction = GetTransactionOrThrow(
            context,
            request,
            transactionId,
            TTransactionAttachOptions{
                .Ping = false,
                .PingAncestors = false
            });

        CompleteCallWith(
            NNative::IClientPtr(),
            context,
            transaction->Commit(options),
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

        CompleteCallWith(
            NNative::IClientPtr(),
            context,
            transaction->Flush(),
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

        // TODO(sandello): Options!
        CompleteCallWith(
            NNative::IClientPtr(),
            context,
            transaction->Abort());
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

        CompleteCallWith(
            client,
            context,
            client->CreateObject(type, options),
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
        CompleteCallWith(
            client,
            context,
            tableMountCache->GetTableInfo(path),
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

        CompleteCallWith(
            client,
            context,
            client->GetTablePivotKeys(path),
            [] (const auto& context, const TYsonString& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
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

        CompleteCallWith(
            client,
            context,
            client->NodeExists(path, options),
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

        CompleteCallWith(
            client,
            context,
            client->GetNode(path, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
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

        CompleteCallWith(
            client,
            context,
            client->ListNode(path, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CreateNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto type = CheckedEnumCast<NObjectClient::EObjectType>(request->type());

        TCreateNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("Path: %v, Type: %v",
            path,
            type);

        CompleteCallWith(
            client,
            context,
            client->CreateNode(path, type, options),
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->RemoveNode(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, SetNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto value = TYsonString(request->value());

        TSetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_recursive()) {
            options.Recursive = request->recursive();
        }
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

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->SetNode(path, value, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto mode = CheckedEnumCast<NCypressClient::ELockMode>(request->mode());

        TLockNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("Path: %v, Mode: %v",
            path,
            mode);

        CompleteCallWith(
            client,
            context,
            client->LockNode(path, mode, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), result.NodeId);
                ToProto(response->mutable_lock_id(), result.LockId);

                context->SetResponseInfo("NodeId: %v, LockId",
                    result.NodeId,
                    result.LockId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnlockNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnlockNodeOptions options;
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

        context->SetRequestInfo("Path: %v", path);

        CompleteCallWith(
            client,
            context,
            client->UnlockNode(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CopyNode)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TCopyNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        CompleteCallWith(
            client,
            context,
            client->CopyNode(srcPath, dstPath, options),
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        CompleteCallWith(
            client,
            context,
            client->MoveNode(srcPath, dstPath, options),
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("SrcPath: %v, DstPath: %v",
            srcPath,
            dstPath);

        CompleteCallWith(
            client,
            context,
            client->LinkNode(
                srcPath,
                dstPath,
                options),
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
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        options.ChunkMetaFetcherConfig = New<NChunkClient::TFetcherConfig>();

        context->SetRequestInfo("SrcPaths: %v, DstPath: %v",
            srcPaths,
            dstPath);

        if (request->has_fetcher()) {
            options.ChunkMetaFetcherConfig->NodeRpcTimeout = FromProto<TDuration>(request->fetcher().node_rpc_timeout());
        }

        CompleteCallWith(
            client,
            context,
            client->ConcatenateNodes(srcPaths, dstPath, options));
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

        CompleteCallWith(
            client,
            context,
            client->ExternalizeNode(path, cellTag, options));
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

        CompleteCallWith(
            client,
            context,
            client->InternalizeNode(path, options));
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
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }
        FromProto(&options.TargetCellIds, request->target_cell_ids());
        if (request->has_freeze()) {
            options.Freeze = request->freeze();
        }

        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->MountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnmountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnmountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_force()) {
            options.Force = request->force();
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->UnmountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemountTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TRemountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->RemountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, FreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TFreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->FreezeTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, UnfreezeTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TUnfreezeTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->UnfreezeTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TReshardTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }

        TFuture<void> result;
        if (request->has_tablet_count()) {
            auto tabletCount = request->tablet_count();

            context->SetRequestInfo("Path: %v, TabletCount: %v",
                path,
                tabletCount);

            CompleteCallWith(
                client,
                context,
                client->ReshardTable(path, tabletCount, options));
        } else {
            TWireProtocolReader reader(MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
            auto keyRange = reader.ReadUnversionedRowset(false);
            std::vector<TOwningKey> keys;
            keys.reserve(keyRange.Size());
            for (const auto& key : keyRange) {
                keys.emplace_back(key);
            }

            context->SetRequestInfo("Path: %v, Keys: %v",
                path,
                keys);

            CompleteCallWith(
                client,
                context,
                client->ReshardTable(path, keys, options));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ReshardTableAutomatic)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TReshardTableAutomaticOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_tablet_range_options()) {
            FromProto(&options, request->tablet_range_options());
        }
        options.KeepActions = request->keep_actions();

        TFuture<std::vector<TTabletActionId>> result;

        CompleteCallWith(
            client,
            context,
            client->ReshardTableAutomatic(path, options),
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

        CompleteCallWith(
            client,
            context,
            client->TrimTable(
                path,
                tabletIndex,
                trimmedRowCount,
                options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AlterTable)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TAlterTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_schema()) {
            options.Schema = ConvertTo<TTableSchema>(TYsonString(request->schema()));
        }
        if (request->has_dynamic()) {
            options.Dynamic = request->dynamic();
        }
        if (request->has_upstream_replica_id()) {
            options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_schema_modification()) {
            options.SchemaModification = CheckedEnumCast<ETableSchemaModification>(request->schema_modification());
        }

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            client,
            context,
            client->AlterTable(path, options));
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

        CompleteCallWith(
            client,
            context,
            client->AlterTableReplica(replicaId, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, BalanceTabletCells)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& bundle = request->bundle();
        auto tables = FromProto<std::vector<NYPath::TYPath>>(request->movable_tables());

        TBalanceTabletCellsOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        options.KeepActions = request->keep_actions();

        TFuture<std::vector<TTabletActionId>> result;

        CompleteCallWith(
            client,
            context,
            client->BalanceTabletCells(bundle, tables, options),
            [] (const auto& context, const auto& tabletActions) {
                auto* response = &context->Response();
                ToProto(response->mutable_tablet_actions(), tabletActions);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, StartOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto type = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(request->type());
        auto spec = TYsonString(request->spec());

        TStartOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("OperationType: %v, Spec: %v",
            type,
            spec);

        CompleteCallWith(
            client,
            context,
            client->StartOperation(type, spec, options),
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

        CompleteCallWith(
            client,
            context,
            client->AbortOperation(operationIdOrAlias, options));
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

        CompleteCallWith(
            client,
            context,
            client->SuspendOperation(operationIdOrAlias, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ResumeOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TResumeOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        CompleteCallWith(
            client,
            context,
            client->ResumeOperation(operationIdOrAlias, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, CompleteOperation)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        TCompleteOperationOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v",
            operationIdOrAlias);

        CompleteCallWith(
            client,
            context,
            client->CompleteOperation(operationIdOrAlias, options));
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

        CompleteCallWith(
            client,
            context,
            client->UpdateOperationParameters(
                operationIdOrAlias,
                parameters,
                options));
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

        context->SetRequestInfo("OperationId: %v, IncludeRuntime: %v",
            operationIdOrAlias,
            options.IncludeRuntime);

        CompleteCallWith(
            client,
            context,
            client->GetOperation(operationIdOrAlias, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_meta(result.GetData());
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

        CompleteCallWith(
            client,
            context,
            client->ListOperations(options),
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

        auto operationId = FromProto<TOperationId>(request->operation_id());

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
            "OperationId: %v, Type: %v, State: %v, Address: %v, IncludeCypress: %v, "
            "IncludeControllerAgent: %v, IncludeArchive: %v, JobCompetitionId: %v, WithCompetitors: %v",
            operationId,
            options.Type,
            options.State,
            options.Address,
            options.IncludeCypress,
            options.IncludeControllerAgent,
            options.IncludeArchive,
            options.JobCompetitionId,
            options.WithCompetitors);

        CompleteCallWith(
            client,
            context,
            client->ListJobs(operationId, options),
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

        CompleteCallWith(
            client,
            context,
            client->DumpJobContext(jobId, path, options));
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

        CompleteCallWith(
            client,
            context,
            client->GetJobInputPaths(jobId, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_paths(result.GetData());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobStderr)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobStderrOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v, JobId: %v",
            operationId,
            jobId);

        CompleteCallWith(
            client,
            context,
            client->GetJobStderr(operationId, jobId, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments().push_back(std::move(result));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJobFailContext)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobFailContextOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("OperationId: %v, JobId: %v",
            operationId,
            jobId);

        CompleteCallWith(
            client,
            context,
            client->GetJobFailContext(operationId, jobId, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->Attachments().push_back(std::move(result));
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        TGetJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        if (request->has_attributes()) {
            FromProto(&options.Attributes, request->attributes());
        }

        context->SetRequestInfo("OperationId: %v, JobId: %v",
            operationId,
            jobId);

        CompleteCallWith(
            client,
            context,
            client->GetJob(operationId, jobId, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_info(result.GetData());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, AbandonJob)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        TAbandonJobOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v", jobId);

        CompleteCallWith(
            client,
            context,
            client->AbandonJob(jobId, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, PollJobShell)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());

        TPollJobShellOptions options;
        SetTimeoutOptions(&options, context.Get());

        context->SetRequestInfo("JobId: %v, Parameters: %v",
            jobId,
            parameters);

        CompleteCallWith(
            client,
            context,
            client->PollJobShell(jobId, parameters, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_result(result.GetData());
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

        CompleteCallWith(
            client,
            context,
            client->AbortJob(jobId, options));
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    template <class TContext, class TRequest, class TOptions>
    static void LookupRowsPrologue(
        const TIntrusivePtr<TContext>& context,
        TRequest* request,
        const NApi::NRpcProxy::NProto::TRowsetDescriptor& rowsetDescriptor,
        TNameTablePtr* nameTable,
        TSharedRange<TUnversionedRow>* keys,
        TOptions* options)
    {
        NApi::NRpcProxy::ValidateRowsetDescriptor(
            request->rowset_descriptor(),
            NApi::NRpcProxy::CurrentWireFormatVersion,
            NApi::NRpcProxy::NProto::RK_UNVERSIONED);
        if (request->Attachments().empty()) {
            THROW_ERROR_EXCEPTION("Request is missing rowset in attachments");
        }

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
        *nameTable = TNameTable::FromSchema(rowset->Schema());
        *keys = MakeSharedRange(rowset->GetRows(), rowset);

        if (request->has_tablet_read_options()) {
            FromProto(options, request->tablet_read_options());
        }

        SetTimeoutOptions(options, context.Get());
        TColumnFilter::TIndexes columnFilterIndexes;
        for (int i = 0; i < request->columns_size(); ++i) {
            columnFilterIndexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
        }
        options->ColumnFilter = request->columns_size() == 0
            ? TColumnFilter()
            : TColumnFilter(std::move(columnFilterIndexes));
        options->Timestamp = request->timestamp();
        options->KeepMissingRows = request->keep_missing_rows();
        options->EnablePartialResult = request->enable_partial_result();
        options->UseLookupCache = request->use_lookup_cache();

        if (request->has_multiplexing_band()) {
            options->MultiplexingBand = CheckedEnumCast<EMultiplexingBand>(request->multiplexing_band());
        }

        context->SetRequestInfo("Path: %v, Rows: %v, Timestamp: %v",
            request->path(),
            keys->Size(),
            options->Timestamp);
    }

    template <class TResponse, class TRow>
    static void AttachRowset(
        TResponse* response,
        const TIntrusivePtr<IRowset<TRow>>& rowset)
    {
        response->Attachments() = NApi::NRpcProxy::SerializeRowset(
            rowset->Schema(),
            rowset->GetRows(),
            response->mutable_rowset_descriptor());
    };

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, LookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TLookupRowsOptions options;
        LookupRowsPrologue(
            context,
            request,
            request->rowset_descriptor(),
            &nameTable,
            &keys,
            &options);

        CompleteCallWith(
            client,
            context,
            client->LookupRows(
                path,
                std::move(nameTable),
                std::move(keys),
                options),
            [] (const auto& context, const auto& rowset) {
                auto* response = &context->Response();
                AttachRowset(response, rowset);

                context->SetResponseInfo("RowCount: %v",
                    rowset->GetRows().Size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, VersionedLookupRows)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TVersionedLookupRowsOptions options;
        LookupRowsPrologue(
            context,
            request,
            request->rowset_descriptor(),
            &nameTable,
            &keys,
            &options);

        if (request->has_retention_config()) {
            options.RetentionConfig = New<TRetentionConfig>();
            FromProto(options.RetentionConfig.Get(), request->retention_config());
        }

        CompleteCallWith(
            client,
            context,
            client->VersionedLookupRows(
                path,
                std::move(nameTable),
                std::move(keys),
                options),
            [] (const auto& context, const auto& rowset) {
                auto* response = &context->Response();
                AttachRowset(response, rowset);

                context->SetResponseInfo("RowCount: %v",
                    rowset->GetRows().Size());
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
        // TODO: Support WorkloadDescriptor
        if (request->has_memory_limit_per_node()) {
            options.MemoryLimitPerNode = request->memory_limit_per_node();
        }
        // TODO(lukyan): Move to FillSelectRowsOptionsBaseFromRequest
        if (request->has_suppressable_access_tracking_options()) {
            FromProto(&options, request->suppressable_access_tracking_options());
        }

        context->SetRequestInfo("Query: %v, Timestamp: %llx",
            query,
            options.Timestamp);

        CompleteCallWith(
            client,
            context,
            client->SelectRows(query, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                AttachRowset(response, result.Rowset);
                ToProto(response->mutable_statistics(), result.Statistics);

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

        CompleteCallWith(
            client,
            context,
            client->ExplainQuery(query, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
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

        auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));

        auto nameTable = TNameTable::FromSchema(rowset->Schema());

        context->SetRequestInfo("Path: %v, Timestamp: %llx, RowCount: %v",
            path,
            options.Timestamp,
            rowset->GetRows().Size());

        CompleteCallWith(
            client,
            context,
            client->GetInSyncReplicas(
                path,
                std::move(nameTable),
                MakeSharedRange(rowset->GetRows(), rowset),
                options),
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

        CompleteCallWith(
            client,
            context,
            client->GetTabletInfos(
                path,
                tabletIndexes,
                options),
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

        auto nameTable = TNameTable::FromSchema(rowset->Schema());

        const auto& rowsetRows = rowset->GetRows();
        auto rowsetSize = rowset->GetRows().Size();

        if (rowsetSize != request.row_modification_types_size()) {
            THROW_ERROR_EXCEPTION("Row count mismatch")
                << TErrorAttribute("rowset_size", rowsetSize)
                << TErrorAttribute("row_modification_types_size", request.row_modification_types_size());
        }

        std::vector<TRowModification> modifications;
        modifications.reserve(rowsetSize);
        for (size_t index = 0; index < rowsetSize; ++index) {
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
            std::move(nameTable),
            MakeSharedRange(std::move(modifications), rowset),
            options);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, ModifyRows)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo(
            "TrasactionId: %v, Path: %v, ModificationCount: %v",
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
                    << TErrorAttribute("partCount", partCount);
            }
            if (partCount >= attachmentCount) {
                THROW_ERROR_EXCEPTION("Part count is too large")
                    << TErrorAttribute("partCount", partCount);
            }
            expectedAttachmentCount += partCount + 1;
        }
        if (attachmentCount != expectedAttachmentCount) {
            THROW_ERROR_EXCEPTION("Attachment count mismatch")
                << TErrorAttribute("attachment_count", attachmentCount)
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
            // TODO(kiselyovp) if this fails, YT_VERIFY happens
            DeserializeProto(&subrequest, request->Attachments()[attachmentIndex]);
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
        if (Bootstrap_->GetConfig()->RequireAuthentication ||
            context->GetUser() != NSecurityClient::RootUserName)
        {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Only root can call \"BuildSnapshot\""));
            return;
        }

        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto connection = client->GetConnection();
        auto admin = connection->CreateAdmin();

        TBuildSnapshotOptions options;
        options.CellId = FromProto<TCellId>(request->cell_id());
        options.SetReadOnly = request->set_read_only();
        options.WaitForSnapshotCompletion = request->wait_for_snapshot_completion();

        context->SetRequestInfo("CellId: %v, SetReadOnly: %v, WaitForSnapshotCompletion: %v",
            options.CellId,
            options.SetReadOnly,
            options.WaitForSnapshotCompletion);

        CompleteCallWith(
            client,
            context,
            admin->BuildSnapshot(options),
            [] (const auto& context, int snapshotId) {
                auto* response = &context->Response();
                response->set_snapshot_id(snapshotId);
                context->SetResponseInfo("SnapshotId: %v", snapshotId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GCCollect)
    {
        if (Bootstrap_->GetConfig()->RequireAuthentication ||
            context->GetUser() != NSecurityClient::RootUserName)
        {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Only root can call \"GCCollect\""));
            return;
        }

        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto connection = client->GetConnection();
        auto admin = connection->CreateAdmin();

        TGCCollectOptions options;
        options.CellId = FromProto<TCellId>(request->cell_id());

        context->SetRequestInfo("CellId: %v", options.CellId);

        CompleteCallWith(
            nullptr,
            context,
            admin->GCCollect(options));
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        CompleteCallWith(
            client,
            context,
            client->AddMember(group, member, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, RemoveMember)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        auto group = request->group();
        auto member = request->member();

        TRemoveMemberOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Group: %v, Member: %v, MutationId: %v, Retry: %v",
            group,
            member,
            options.MutationId,
            options.Retry);

        CompleteCallWith(
            client,
            context,
            client->RemoveMember(group, member, options));
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

        CompleteCallWith(
            client,
            context,
            client->CheckPermission(user, path, permission, options),
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

        CompleteCallWith(
            client,
            context,
            client->CheckPermissionByAcl(user, permission, acl, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);
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
            BIND(&IFileWriter::Write, fileWriter),
            BIND(&IFileWriter::Close, fileWriter),
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

        HandleInputStreamingRequest(context, BIND([=] () {
            return journalReader->Read().Apply(BIND([] (const std::vector<TSharedRef>& rows) {
                if (rows.empty()) {
                    return TSharedRef();
                }

                return PackRefs(rows);
            }));
        }));
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

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo(
            "Path: %v, EnableMultiplexing: %v",
            path,
            options.EnableMultiplexing);

        auto journalWriter = client->CreateJournalWriter(path, options);
        WaitFor(journalWriter->Open())
            .ThrowOnError();

        HandleOutputStreamingRequest(
            context,
            BIND([=] (const TSharedRef& packedRows) {
                std::vector<TSharedRef> rows;
                UnpackRefsOrThrow(packedRows, &rows);
                return journalWriter->Write(rows);
            }),
            BIND(&IJournalWriter::Close, journalWriter),
            true /* feedbackEnabled */);
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, TruncateJournal)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        const auto& path = request->path();
        auto rowCount = request->row_count();

        TTruncateJournalOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }
        if (request->has_prerequisite_options()) {
            FromProto(&options, request->prerequisite_options());
        }

        context->SetRequestInfo("Path: %v, RowCount: %v",
            path,
            rowCount);

        CompleteCallWith(
            client,
            context,
            client->TruncateJournal(
                path,
                rowCount,
                options));
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

        context->SetRequestInfo(
            "Path: %v, Unordered: %v, OmitInaccessibleColumns: %v",
            path,
            options.Unordered,
            options.OmitInaccessibleColumns);

        auto tableReader = WaitFor(client->CreateTableReader(path, options))
            .ValueOrThrow();

        auto outputStream = context->GetResponseAttachmentsStream();
        NApi::NRpcProxy::NProto::TReadTableMeta meta;
        meta.set_start_row_index(tableReader->GetStartRowIndex());
        ToProto(meta.mutable_key_columns(), tableReader->GetKeyColumns());
        ToProto(meta.mutable_omitted_inaccessible_columns(),
            tableReader->GetOmittedInaccessibleColumns());
        ToProto(meta.mutable_schema(), tableReader->GetTableSchema());
        meta.mutable_payload()->set_total_row_count(tableReader->GetTotalRowCount());
        ToProto(meta.mutable_payload()->mutable_data_statistics(),
            tableReader->GetDataStatistics());

        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        int nameTableSize = 0;
        std::vector<TUnversionedRow> rows;
        rows.reserve(Config_->ReadBufferRowCount);
        bool finished = false;

        auto blockGenerator = BIND([&] {
            if (finished) {
                return MakeFuture(TSharedRef());
            }

            return AsyncReadRows(tableReader, &rows).Apply(BIND([&] {
                if (rows.empty()) {
                    finished = true;
                }

                auto rowsetData = NApi::NRpcProxy::SerializeRowsetWithNameTableDelta(
                    tableReader->GetNameTable(),
                    rows,
                    &nameTableSize);

                NApi::NRpcProxy::NProto::TTableReaderPayload payload;
                payload.set_total_row_count(tableReader->GetTotalRowCount());
                ToProto(payload.mutable_data_statistics(), tableReader->GetDataStatistics());
                auto payloadRef = SerializeProtoToRef(payload);

                return PackRefs(std::vector{rowsetData, payloadRef});
            }));
        });

        HandleInputStreamingRequest(
            context,
            blockGenerator);
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

        auto outputStream = context->GetResponseAttachmentsStream();
        const auto& schema = tableWriter->GetSchema();
        NApi::NRpcProxy::NProto::TWriteTableMeta meta;
        ToProto(meta.mutable_schema(), schema);
        auto metaRef = SerializeProtoToRef(meta);
        WaitFor(outputStream->Write(metaRef))
            .ThrowOnError();

        NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        descriptor.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        descriptor.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);

        auto blockHandler = BIND([&] (const TSharedRef& block) {
            // Here we assume our local tableWriter wouldn't modify its own name table,
            // so we don't have to use an id mapping.
            auto rows = NApi::NRpcProxy::DeserializeRowsetWithNameTableDelta(
                block,
                tableWriter->GetNameTable(),
                &descriptor);

            tableWriter->Write(rows);
            return tableWriter->GetReadyEvent();
        });

        HandleOutputStreamingRequest(
            context,
            blockHandler,
            BIND(&ITableWriter::Close, tableWriter),
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

        CompleteCallWith(
            client,
            context,
            client->GetFileFromCache(md5, options),
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
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("Path: %v, MD5: %v, CachePath: %v",
            path,
            md5,
            options.CachePath);

        CompleteCallWith(
            client,
            context,
            client->PutFileToCache(path, md5, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_result(), result);

                context->SetResponseInfo("Path: %v",
                    result.Path);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NApi::NRpcProxy::NProto, GetColumnarStatistics)
    {
        auto client = GetAuthenticatedClientOrThrow(context, request);

        std::vector<NYPath::TRichYPath> path;
        for (const auto& protoSubPath: request->path()) {
            path.emplace_back(ConvertTo<NYPath::TRichYPath>(TYsonString(protoSubPath)));
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

        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }

        context->SetRequestInfo("Path: %v", path);

        CompleteCallWith(
            client,
            context,
            client->GetColumnarStatistics(path, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                NYT::ToProto(response->mutable_statistics(), result);

                context->SetResponseInfo("StatisticsCount: %v", result.size());
            });
    }
};

IServicePtr CreateApiService(TBootstrap* bootstrap)
{
    return New<TApiService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
