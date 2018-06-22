#include "api_service.h"
#include "proxy_coordinator.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_proxy/bootstrap.h>
#include <yt/server/cell_proxy/config.h>

#include <yt/ytlib/auth/cookie_authenticator.h>
#include <yt/ytlib/auth/token_authenticator.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/rpc_proxy/api_service_proxy.h>
#include <yt/ytlib/rpc_proxy/helpers.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/cast.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NRpcProxy {

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
using namespace NTransactionClient;
using namespace NYPath;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct TApiServiceBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

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
    const NProto::TTransactionalOptions& proto)
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
    const NProto::TPrerequisiteOptions& proto)
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
    const NProto::TMasterReadOptions& proto)
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
    const NProto::TMutatingOptions& proto)
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
    const NProto::TSuppressableAccessTrackingOptions& proto)
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
    const NProto::TTabletRangeOptions& proto)
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
    const NProto::TTabletReadOptions& proto)
{
    if (proto.has_read_from()) {
        options->ReadFrom = CheckedEnumCast<NHydra::EPeerKind>(proto.read_from());
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TApiService
    : public TServiceBase
{
public:
    explicit TApiService(
        NCellProxy::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetWorkerInvoker(),
            TApiServiceProxy::GetDescriptor(),
            RpcProxyLogger,
            NullRealmId,
            bootstrap->GetRpcAuthenticator())
        , Bootstrap_(bootstrap)
        , Coordinator_(bootstrap->GetProxyCoordinator())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachTransaction));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExistsNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LockNode));
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TrimTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTable));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterTableReplica));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(VersionedLookupRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectRows));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetInSyncReplicas));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfos));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyRows));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(BuildSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateObject));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableMountInfo));

        if (!Bootstrap_->GetConfig()->RequireAuthentication) {
            GetOrCreateClient(NSecurityClient::RootUserName);
        }
    }

private:
    const NCellProxy::TBootstrap* Bootstrap_;
    const IProxyCoordinatorPtr Coordinator_;

    TSpinLock SpinLock_;
    // TODO(sandello): Introduce expiration times for clients.
    THashMap<TString, INativeClientPtr> AuthenticatedClients_;

    INativeClientPtr GetOrCreateClient(const TString& user)
    {
        auto guard = Guard(SpinLock_);

        auto it = AuthenticatedClients_.find(user);
        if (it == AuthenticatedClients_.end()) {
            const auto& connection = Bootstrap_->GetNativeConnection();
            auto client = connection->CreateNativeClient(TClientOptions(user));
            YCHECK(AuthenticatedClients_.insert(std::make_pair(user, client)).second);

            LOG_DEBUG("Created native client (User: %v)", user);

            return client;
        }

        return it->second;
    }

    TString ExtractIP(TString address)
    {
        YCHECK(address.StartsWith("tcp://"));

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

    INativeClientPtr GetAuthenticatedClientOrAbortContext(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request)
    {
        if (!Coordinator_->IsOperable(context)) {
            return nullptr;
        }

        const auto& user = context->GetUser();

        // Pretty-printing Protobuf requires a bunch of effort, so we make it conditional.
        if (Bootstrap_->GetConfig()->ApiService->VerboseLogging) {
            LOG_DEBUG("RequestId: %v, RequestBody: %v",
                context->GetRequestId(),
                request->ShortDebugString());
        }

        return GetOrCreateClient(user);
    }

    ITransactionPtr GetTransactionOrAbortContext(
        const IServiceContextPtr& context,
        const google::protobuf::Message* request,
        const TTransactionId& transactionId,
        const TTransactionAttachOptions& options)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return nullptr;
        }

        auto transaction = client->AttachTransaction(transactionId, options);

        if (!transaction) {
            context->Reply(TError(
                NTransactionClient::EErrorCode::NoSuchTransaction,
                "No such transaction %v",
                transactionId));
            return nullptr;
        }

        return transaction;
    }

    template <class T>
    void CompleteCallWith(const IServiceContextPtr& context, TFuture<T>&& future)
    {
        future.Subscribe(
            BIND([context = context] (const TErrorOr<T>& valueOrError) {
                if (valueOrError.IsOK()) {
                    // XXX(sandello): This relies on the typed service context implementation.
                    context->Reply(TError());
                } else {
                    context->Reply(TError("Internal RPC call failed")
                        << TError(valueOrError));
                }
            }));
    }

    template <class TContext, class TResult, class F>
    void CompleteCallWith(const TIntrusivePtr<TContext>& context, TFuture<TResult>&& future, F&& functor)
    {
        future.Subscribe(
            BIND([context, functor = std::move(functor)] (const TErrorOr<TResult>& valueOrError) {
                if (valueOrError.IsOK()) {
                    try {
                        functor(context, valueOrError.Value());
                        // XXX(sandello): This relies on the typed service context implementation.
                        context->Reply(TError());
                    } catch (const std::exception& ex) {
                        context->Reply(TError(ex));
                    }
                } else {
                    context->Reply(TError("Internal RPC call failed")
                        << TError(valueOrError));
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GenerateTimestamps)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto count = request->count();

        context->SetRequestInfo("Count: %v",
            count);

        const auto& timestampProvider = Bootstrap_->GetNativeConnection()->GetTimestampProvider();

        CompleteCallWith(
            context,
            timestampProvider->GenerateTimestamps(count),
            [] (const auto& context, const TTimestamp& timestamp) {
                auto* response = &context->Response();
                response->set_timestamp(timestamp);

                context->SetResponseInfo("Timestamp: %llx",
                    timestamp);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, StartTransaction)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        TTransactionStartOptions options;
        if (request->has_timeout()) {
            options.Timeout = FromProto<TDuration>(request->timeout());
        }
        if (request->has_id()) {
            FromProto(&options.Id, request->id());
        }
        if (request->has_parent_id()) {
            FromProto(&options.ParentId, request->parent_id());
        }
        options.AutoAbort = request->auto_abort();
        options.Sticky = request->sticky();
        options.Ping = request->ping();
        options.PingAncestors = request->ping_ancestors();
        options.Atomicity = CheckedEnumCast<NTransactionClient::EAtomicity>(request->atomicity());
        options.Durability = CheckedEnumCast<NTransactionClient::EDurability>(request->durability());
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }

        CompleteCallWith(
            context,
            client->StartTransaction(NTransactionClient::ETransactionType(request->type()), options),
            [] (const auto& context, const auto& transaction) {
                auto* response = &context->Response();
                ToProto(response->mutable_id(), transaction->GetId());
                response->set_start_timestamp(transaction->GetStartTimestamp());

                context->SetResponseInfo("TransactionId: %v, StartTimestamp: %v",
                    transaction->GetId(),
                    transaction->GetStartTimestamp());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, PingTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = true;
        attachOptions.PingAncestors = true;
        attachOptions.Sticky = request->sticky();

        context->SetRequestInfo("TransactionId: %v, Sticky: %v",
            transactionId,
            attachOptions.Sticky);

        auto transaction = GetTransactionOrAbortContext(
            context,
            request,
            transactionId,
            attachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        CompleteCallWith(
            context,
            transaction->Ping());
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, CommitTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = false;
        attachOptions.PingAncestors = false;
        attachOptions.Sticky = request->sticky();

        context->SetRequestInfo("TransactionId: %v, Sticky: %v",
            transactionId,
            attachOptions.Sticky);

        auto transaction = GetTransactionOrAbortContext(
            context,
            request,
            transactionId,
            attachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        CompleteCallWith(
            context,
            transaction->Commit(),
            [] (const auto& context, const TTransactionCommitResult& result) {
                auto* response = &context->Response();
                ToProto(response->mutable_commit_timestamps(), result.CommitTimestamps);

                context->SetResponseInfo("CommitTimestamps: %v",
                    result.CommitTimestamps);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, AbortTransaction)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = false;
        attachOptions.PingAncestors = false;
        attachOptions.Sticky = request->sticky();

        context->SetRequestInfo("TransactionId: %v, Sticky: %v",
            transactionId,
            attachOptions.Sticky);

        auto transaction = GetTransactionOrAbortContext(
            context,
            request,
            transactionId,
            attachOptions);
        if (!transaction) {
            return;
        }

        // TODO(sandello): Options!
        CompleteCallWith(
            context,
            transaction->Abort());
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, AttachTransaction)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        TTransactionAttachOptions options;
        if (request->has_auto_abort()) {
            options.AutoAbort = request->auto_abort();
        }
        if (request->has_sticky()) {
            options.Sticky = request->sticky();
        }
        if (request->has_ping_period()) {
            options.PingPeriod = TDuration::FromValue(request->ping_period());
        }
        if (request->has_ping()) {
            options.Ping = request->ping();
        }
        if (request->has_ping_ancestors()) {
            options.PingAncestors = request->ping_ancestors();
        }

        context->SetRequestInfo("TransactionId: %v, Sticky: %v",
            transactionId,
            options.Sticky);

        auto transaction = GetTransactionOrAbortContext(
            context,
            request,
            transactionId,
            options);
        if (!transaction) {
            return;
        }

        response->set_type(static_cast<NProto::ETransactionType>(transaction->GetType()));
        response->set_start_timestamp(transaction->GetStartTimestamp());
        response->set_atomicity(static_cast<NProto::EAtomicity>(transaction->GetAtomicity()));
        response->set_durability(static_cast<NProto::EDurability>(transaction->GetDurability()));
        response->set_timeout(static_cast<i64>(transaction->GetTimeout().GetValue()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, CreateObject)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto type = FromProto<EObjectType>(request->type());
        TCreateObjectOptions options;
        if (request->has_attributes()) {
            options.Attributes = NYTree::FromProto(request->attributes());
        }

        context->SetRequestInfo("Type: %v", type);

        CompleteCallWith(
            context,
            client->CreateObject(type, options),
            [] (const auto& context, const NObjectClient::TObjectId& objectId) {
                auto* response = &context->Response();
                ToProto(response->mutable_object_id(), objectId);

                context->SetResponseInfo("ObjectId: %v", objectId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetTableMountInfo)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto path = FromProto<TYPath>(request->path());

        context->SetRequestInfo("Path: %v", path);

        const auto& tableMountCache = client->GetTableMountCache();
        CompleteCallWith(
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

    ////////////////////////////////////////////////////////////////////////////////
    // CYPRESS
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ExistsNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->NodeExists(path, options),
            [] (const auto& context, const bool& result) {
                auto* response = &context->Response();
                response->set_exists(result);

                context->SetResponseInfo("Exists: %v",
                    result);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TGetNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            const auto& protoAttributes = request->attributes();
            if (protoAttributes.all()) {
                options.Attributes.Reset();
            } else {
                options.Attributes = std::vector<TString>();
                options.Attributes->reserve(protoAttributes.columns_size());
                for (int index = 0; index < protoAttributes.columns_size(); ++index) {
                    const auto& protoItem = protoAttributes.columns(index);
                    options.Attributes->push_back(protoItem);
                }
            }
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
            context,
            client->GetNode(path, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ListNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TListNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            const auto& protoAttributes = request->attributes();
            if (protoAttributes.all()) {
                options.Attributes.Reset();
            } else {
                options.Attributes = std::vector<TString>();
                options.Attributes->reserve(protoAttributes.columns_size());
                for (int index = 0; index < protoAttributes.columns_size(); ++index) {
                    const auto& protoItem = protoAttributes.columns(index);
                    options.Attributes->push_back(protoItem);
                }
            }
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
            context,
            client->ListNode(path, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                response->set_value(result.GetData());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, CreateNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();
        auto type = CheckedEnumCast<NObjectClient::EObjectType>(request->type());

        TCreateNodeOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_attributes()) {
            const auto& protoAttributes = request->attributes();
            auto attributes = std::shared_ptr<IAttributeDictionary>(CreateEphemeralAttributes());
            for (int index = 0; index < protoAttributes.attributes_size(); ++index) {
                const auto& protoItem = protoAttributes.attributes(index);
                attributes->SetYson(protoItem.key(), TYsonString(protoItem.value()));
            }
            options.Attributes = std::move(attributes);
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
            context,
            client->CreateNode(path, type, options),
            [] (const auto& context, const NCypressClient::TNodeId& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, RemoveNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->RemoveNode(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, SetNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            context,
            client->SetNode(path, value, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, LockNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, CopyNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& srcPath = request->src_path();
        const auto& dstPath = request->dst_path();

        TCopyNodeOptions options;
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
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
        }
        if (request->has_preserve_creation_time()) {
            options.PreserveCreationTime = request->preserve_creation_time();
        }
        if (request->has_source_transaction_id()) {
            options.SourceTransactionId = FromProto<TTransactionId>(request->source_transaction_id());
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
            context,
            client->CopyNode(srcPath, dstPath, options),
            [] (const auto& context, const NCypressClient::TNodeId& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, MoveNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
        if (request->has_preserve_expiration_time()) {
            options.PreserveExpirationTime = request->preserve_expiration_time();
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
            context,
            client->MoveNode(srcPath, dstPath, options),
            [] (const auto& context, const auto& nodeId) {
                auto* response = &context->Response();
                ToProto(response->mutable_node_id(), nodeId);

                context->SetResponseInfo("NodeId: %v",
                    nodeId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, LinkNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ConcatenateNodes)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto srcPaths = FromProto<std::vector<TYPath>>(request->src_paths());
        const auto& dstPath = request->dst_path();

        TConcatenateNodesOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_append()) {
            options.Append = request->append();
        }
        if (request->has_transactional_options()) {
            FromProto(&options, request->transactional_options());
        }
        if (request->has_mutating_options()) {
            FromProto(&options, request->mutating_options());
        }

        context->SetRequestInfo("SrcPaths: %v, DstPath: %v",
            srcPaths,
            dstPath);

        CompleteCallWith(
            context,
            client->ConcatenateNodes(srcPaths, dstPath, options));
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (NON-TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, MountTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TMountTableOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }
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
            context,
            client->MountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, UnmountTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->UnmountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, RemountTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->RemountTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, FreezeTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->FreezeTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, UnfreezeTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->UnfreezeTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ReshardTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
                context,
                client->ReshardTable(path, keys, options));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, TrimTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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
            context,
            client->TrimTable(
                path,
                tabletIndex,
                trimmedRowCount,
                options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, AlterTable)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

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

        context->SetRequestInfo("Path: %v",
            path);

        CompleteCallWith(
            context,
            client->AlterTable(path, options));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, AlterTableReplica)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());

        TAlterTableReplicaOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_enabled()) {
            options.Enabled = request->enabled();
        }
        if (request->has_mode()) {
            options.Mode = CheckedEnumCast<ETableReplicaMode>(request->mode());
        }

        context->SetRequestInfo("ReplicaId: %v, Enabled: %v, Mode: %v",
            replicaId,
            options.Enabled,
            options.Mode);

        CompleteCallWith(
            context,
            client->AlterTableReplica(replicaId, options));
    }

    ////////////////////////////////////////////////////////////////////////////////
    // TABLES (TRANSACTIONAL)
    ////////////////////////////////////////////////////////////////////////////////

    template <class TContext, class TRequest, class TOptions>
    static bool LookupRowsPrologue(
        const TIntrusivePtr<TContext>& context,
        TRequest* request,
        const NProto::TRowsetDescriptor& rowsetDescriptor,
        TNameTablePtr* nameTable,
        TSharedRange<TUnversionedRow>* keys,
        TOptions* options)
    {
        ValidateRowsetDescriptor(request->rowset_descriptor(), 1, NProto::RK_UNVERSIONED);
        if (request->Attachments().empty()) {
            context->Reply(TError("Request is missing rowset in attachments"));
            return false;
        }

        auto rowset = DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
        *nameTable = TNameTable::FromSchema(rowset->Schema());
        *keys = MakeSharedRange(rowset->GetRows(), rowset);

        if (request->has_tablet_read_options()) {
            FromProto(options, request->tablet_read_options());
        }

        SetTimeoutOptions(options, context.Get());
        for (int i = 0; i < request->columns_size(); ++i) {
            options->ColumnFilter.All = false;
            options->ColumnFilter.Indexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
        }
        options->Timestamp = request->timestamp();
        options->KeepMissingRows = request->keep_missing_rows();

        context->SetRequestInfo("Path: %v, Rows: %v",
            request->path(),
            keys->Size());

        return true;
    }

    template <class TResponse, class TRow>
    static void AttachRowset(
        TResponse* response,
        const TIntrusivePtr<IRowset<TRow>>& rowset)
    {
        response->Attachments() = SerializeRowset(
            rowset->Schema(),
            rowset->GetRows(),
            response->mutable_rowset_descriptor());
    };

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, LookupRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TLookupRowsOptions options;
        if (!LookupRowsPrologue(
            context,
            request,
            request->rowset_descriptor(),
            &nameTable,
            &keys,
            &options))
        {
            return;
        }

        CompleteCallWith(
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

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, VersionedLookupRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TNameTablePtr nameTable;
        TSharedRange<TUnversionedRow> keys;
        TVersionedLookupRowsOptions options;
        if (!LookupRowsPrologue(
            context,
            request,
            request->rowset_descriptor(),
            &nameTable,
            &keys,
            &options))
        {
            return;
        }

        CompleteCallWith(
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

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, SelectRows)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& query = request->query();

        TSelectRowsOptions options; // TODO: Fill all options.
        SetTimeoutOptions(&options, context.Get());
        if (request->has_timestamp()) {
            options.Timestamp = request->timestamp();
        }
        if (request->has_input_row_limit()) {
            options.InputRowLimit = request->input_row_limit();
        }
        if (request->has_output_row_limit()) {
            options.OutputRowLimit = request->output_row_limit();
        }
        if (request->has_range_expansion_limit()) {
            options.RangeExpansionLimit = request->range_expansion_limit();
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
        if (request->has_max_subqueries()) {
            options.MaxSubqueries = request->max_subqueries();
        }

        context->SetRequestInfo("Query: %v, Timestamp: %llx",
            query,
            options.Timestamp);

        CompleteCallWith(
            context,
            client->SelectRows(query, options),
            [] (const auto& context, const auto& result) {
                auto* response = &context->Response();
                // TODO(sandello): Statistics?
                AttachRowset(response, result.Rowset);

                context->SetResponseInfo("RowCount: %v",
                    result.Rowset->GetRows().Size());
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetInSyncReplicas)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();

        TGetInSyncReplicasOptions options;
        SetTimeoutOptions(&options, context.Get());
        if (request->has_timestamp()) {
            options.Timestamp = request->timestamp();
        }

        auto rowset = DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));

        auto nameTable = TNameTable::FromSchema(rowset->Schema());

        context->SetRequestInfo("Path: %v, Timestamp: %llx, RowCount: %v",
            path,
            options.Timestamp,
            rowset->GetRows().Size());

        CompleteCallWith(
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

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetTabletInfos)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        const auto& path = request->path();
        auto tabletIndexes = FromProto<std::vector<int>>(request->tablet_indexes());
        
        context->SetRequestInfo("Path: %v, TabletIndexes: %v",
            path,
            tabletIndexes);
        
        TGetTabletsInfoOptions options;
        SetTimeoutOptions(&options, context.Get());

        CompleteCallWith(
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
                }
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, ModifyRows)
    {
        const auto& path = request->path();

        TTransactionAttachOptions attachOptions;
        attachOptions.Ping = false;
        attachOptions.PingAncestors = false;
        attachOptions.Sticky = true; // XXX(sandello): Fix me!

        auto transaction = GetTransactionOrAbortContext(
            context,
            request,
            FromProto<TTransactionId>(request->transaction_id()),
            attachOptions);
        if (!transaction) {
            return;
        }

        auto rowset = DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));

        auto nameTable = TNameTable::FromSchema(rowset->Schema());

        const auto& rowsetRows = rowset->GetRows();
        auto rowsetSize = rowset->GetRows().Size();

        if (rowsetSize != request->row_modification_types_size()) {
            THROW_ERROR_EXCEPTION("Row count mismatch: %v != %v",
                rowsetSize,
                request->row_modification_types_size());
        }

        std::vector<TRowModification> modifications;
        modifications.reserve(rowsetSize);
        for (size_t index = 0; index < rowsetSize; ++index) {
            modifications.push_back({
                CheckedEnumCast<ERowModificationType>(request->row_modification_types(index)),
                rowsetRows[index].ToTypeErasedRow()
            });
        }

        TModifyRowsOptions options;
        if (request->has_require_sync_replica()) {
            options.RequireSyncReplica = request->require_sync_replica();
        }
        if (request->has_upstream_replica_id()) {
            FromProto(&options.UpstreamReplicaId, request->upstream_replica_id());
        }

        context->SetRequestInfo("Path: %v, ModificationCount: %v, RequireSyncReplica: %v, UpstreamReplicaId: %v",
            path,
            rowsetSize,
            options.RequireSyncReplica,
            options.UpstreamReplicaId);

        transaction->ModifyRows(
            path,
            std::move(nameTable),
            MakeSharedRange(std::move(modifications), rowset),
            options);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, BuildSnapshot)
    {
        if (Bootstrap_->GetConfig()->RequireAuthentication ||
            context->GetUser() != NSecurityClient::RootUserName)
        {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Only root can call \"BuildSnapshot\""));
            return;
        }

        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto connection = client->GetConnection();
        auto admin = connection->CreateAdmin();

        TBuildSnapshotOptions options;
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }
        if (request->has_set_read_only()) {
            options.SetReadOnly = request->set_read_only();
        }

        context->SetRequestInfo("CellId: %v, SetReadOnly: %v",
            options.CellId,
            options.SetReadOnly);

        CompleteCallWith(
            context,
            admin->BuildSnapshot(options),
            [] (const auto& context, int snapshotId) {
                auto* response = &context->Response();
                response->set_snapshot_id(snapshotId);
                context->SetResponseInfo("SnapshotId: %v", snapshotId);
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GCCollect)
    {
        if (Bootstrap_->GetConfig()->RequireAuthentication ||
            context->GetUser() != NSecurityClient::RootUserName)
        {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Only root can call \"GCCollect\""));
            return;
        }

        auto client = GetAuthenticatedClientOrAbortContext(context, request);
        if (!client) {
            return;
        }

        auto connection = client->GetConnection();
        auto admin = connection->CreateAdmin();

        TGCCollectOptions options;
        if (request->has_cell_id()) {
            FromProto(&options.CellId, request->cell_id());
        }

        context->SetRequestInfo("CellId: %v", options.CellId);

        CompleteCallWith(context, admin->GCCollect(options));
    }
};

IServicePtr CreateApiService(NCellProxy::TBootstrap* bootstrap)
{
    return New<TApiService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

