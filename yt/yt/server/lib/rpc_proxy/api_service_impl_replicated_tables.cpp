#include "api_service_impl.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TMasterMetadataApiService::RegisterReplicatedTableMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AlterTableReplica));
}

void TApiService::RegisterReplicatedTableMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AlterReplicationCard));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PingChaosLease));

    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetInSyncReplicas));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, AlterTableReplica)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto replicaId = FromProto<TTableReplicaId>(request->replica_id());

    TAlterTableReplicaOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_enabled()) {
        options.Enabled = request->enabled();
    }

    if (request->has_mode()) {
        options.Mode = FromProto<ETableReplicaMode>(request->mode());
    }

    if (request->has_preserve_timestamps()) {
        options.PreserveTimestamps = request->preserve_timestamps();
    }

    if (request->has_atomicity()) {
        options.Atomicity = FromProto<EAtomicity>(request->atomicity());
    }

    if (request->has_enable_replicated_table_tracker()) {
        options.EnableReplicatedTableTracker = request->enable_replicated_table_tracker();
    }

    if (request->has_replica_path()) {
        options.ReplicaPath = request->replica_path();
    }

    options.Force = request->force();

    context->AnnotateRequest()
        .With("ReplicaId", replicaId)
        .With("Enabled", options.Enabled)
        .With("Mode", options.Mode)
        .With("Atomicity", options.Atomicity)
        .With("PreserveTimestamps", options.PreserveTimestamps)
        .With("EnableReplicatedTableTracker", options.EnableReplicatedTableTracker)
        .With("ReplicaPath", options.ReplicaPath)
        .With("Force", options.Force);

    ExecuteCall(
        context,
        [=] {
            return client->AlterTableReplica(replicaId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AlterReplicationCard)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto replicationCardId = FromProto<TReplicationCardId>(request->replication_card_id());

    TAlterReplicationCardOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_replicated_table_options()) {
        options.ReplicatedTableOptions = ConvertTo<TReplicatedTableOptionsPtr>(TYsonString(request->replicated_table_options()));
    }
    if (request->has_enable_replicated_table_tracker()) {
        options.EnableReplicatedTableTracker = request->enable_replicated_table_tracker();
    }
    if (request->has_replication_card_collocation_id()) {
        options.ReplicationCardCollocationId = FromProto<TReplicationCardCollocationId>(request->replication_card_collocation_id());
    }
    if (request->has_collocation_options()) {
        options.CollocationOptions = ConvertTo<TReplicationCollocationOptionsPtr>(TYsonString(request->collocation_options()));
    }

    using ECase = NApi::NRpcProxy::NProto::TReqAlterReplicationCard::SecondaryIndexCase;
    switch (request->secondary_index_case()) {
        case ECase::kCreateSecondaryIndex:
            options.CreateSecondaryIndex = ConvertTo<TCreateSecondaryIndexPtr>(
                TYsonString(request->create_secondary_index()));
            break;
        case ECase::kDestroySecondaryIndex:
            FromProto(&options.DestroySecondaryIndex, request->destroy_secondary_index());
            break;
        case ECase::kProgressSecondaryIndexCorrespondence:
            options.ProgressSecondaryIndexCorrespondence = ConvertTo<TProgressSecondaryIndexCorrespondencePtr>(
                TYsonString(request->progress_secondary_index_correspondence()));
            break;
        default:
            break;
    }

    context->AnnotateRequest()
        .With("ReplicationCardId", replicationCardId);

    ExecuteCall(
        context,
        [=] {
            return client->AlterReplicationCard(replicationCardId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PingChaosLease)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);
    auto chaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id());

    TChaosLeasePingOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.PingAncestors = request->ping_ancestors();

    context->AnnotateRequest()
        .With("ChaosLeaseId", chaosLeaseId);

    ExecuteCall(
        context,
        [=] {
            return client->PingChaosLease(chaosLeaseId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetInSyncReplicas)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TGetInSyncReplicasOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_timestamp()) {
        options.Timestamp = FromProto<NTransactionClient::TTimestamp>(request->timestamp());
    }

    if (request->has_cached_sync_replicas_timeout()) {
        options.CachedSyncReplicasTimeout = FromProto<TDuration>(request->cached_sync_replicas_timeout());
    }

    auto rowset = request->has_rowset_descriptor()
        ? NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request->rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()))
        : nullptr;
    auto keyCount = rowset
        ? std::make_optional(rowset->GetRows().Size())
        : std::nullopt;

    context->AnnotateRequest()
        .With("Path", path)
        .With("Timestamp", options.Timestamp)
        .With("KeyCount", keyCount);

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

            context->AnnotateResponse()
                .With("ReplicaIds", replicaIds);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
