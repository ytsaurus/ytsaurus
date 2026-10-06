#include "api_service_impl.h"

#include <yt/yt/client/api/helpers.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterAdminMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(BuildSnapshot));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ExitReadOnly));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(MasterExitReadOnly));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ResetDynamicallyPropagatedMasterCells));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(DiscombobulateNonvotingPeers));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(GCCollect));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(SuspendCoordinator));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ResumeCoordinator));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(MigrateReplicationCards));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(SuspendChaosCells));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ResumeChaosCells));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(SuspendTabletCells));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ResumeTabletCells));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(AddMaintenance));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(RemoveMaintenance));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(DisableChunkLocations));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(DestroyChunkLocations));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(ResurrectChunkLocations));
    registerMethod(EMultiproxyMethodKind::ExplicitlyDisabled, RPC_SERVICE_METHOD_DESC(RequestRestart));

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(CheckClusterLiveness));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, BuildSnapshot)
{
    TBuildSnapshotOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.CellId = FromProto<TCellId>(request->cell_id());
    options.SetReadOnly = request->set_read_only();
    options.WaitForSnapshotCompletion = request->wait_for_snapshot_completion();
    options.EnableAutomatonReadOnlyBarrier = request->enable_automaton_read_only_barrier();

    context->AnnotateRequest()
        .With("CellId", options.CellId)
        .With("SetReadOnly", options.SetReadOnly)
        .With("WaitForSnapshotCompletion", options.WaitForSnapshotCompletion)
        .With("EnableAutomatonReadOnlyBarrier", options.EnableAutomatonReadOnlyBarrier);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->BuildSnapshot(options);
        },
        [] (const auto& context, int snapshotId) {
            auto* response = &context->Response();
            response->set_snapshot_id(snapshotId);
            context->AnnotateResponse()
                .With("SnapshotId", snapshotId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ExitReadOnly)
{
    TExitReadOnlyOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellId = FromProto<TCellId>(request->cell_id());

    context->AnnotateRequest()
        .With("CellId", cellId);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->ExitReadOnly(cellId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, MasterExitReadOnly)
{
    TMasterExitReadOnlyOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.Retry = request->retry();

    context->AnnotateRequest()
        .With("Retry", options.Retry);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->MasterExitReadOnly(options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, DiscombobulateNonvotingPeers)
{
    TDiscombobulateNonvotingPeersOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellId = FromProto<TCellId>(request->cell_id());

    context->AnnotateRequest()
        .With("CellId", cellId);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->DiscombobulateNonvotingPeers(cellId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResetDynamicallyPropagatedMasterCells)
{
    TResetDynamicallyPropagatedMasterCellsOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest();

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->ResetDynamicallyPropagatedMasterCells(options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GCCollect)
{
    TGCCollectOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.CellId = FromProto<TCellId>(request->cell_id());

    context->AnnotateRequest()
        .With("CellId", options.CellId);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->GCCollect(options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SuspendCoordinator)
{
    TSuspendCoordinatorOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());

    context->AnnotateRequest()
        .With("CoordinatorCellId", coordinatorCellId);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->SuspendCoordinator(coordinatorCellId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResumeCoordinator)
{
    TResumeCoordinatorOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto coordinatorCellId = FromProto<TCellId>(request->coordinator_cell_id());

    context->AnnotateRequest()
        .With("CoordinatorCellId", coordinatorCellId);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->ResumeCoordinator(coordinatorCellId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, MigrateReplicationCards)
{
    TMigrateReplicationCardsOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto chaosCellId = FromProto<TCellId>(request->chaos_cell_id());
    FromProto(&options.ReplicationCardIds, request->replication_card_ids());
    if (request->has_destination_cell_id()) {
        options.DestinationCellId = FromProto<TCellId>(request->destination_cell_id());
    }

    context->AnnotateRequest()
        .With("ChaosCellId", chaosCellId)
        .With("DestinationCellId", options.DestinationCellId)
        .With("ReplicationCardIds", options.ReplicationCardIds);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->MigrateReplicationCards(chaosCellId, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SuspendChaosCells)
{
    TSuspendChaosCellsOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

    context->AnnotateRequest()
        .With("ChaosCellIds", cellIds);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->SuspendChaosCells(cellIds, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResumeChaosCells)
{
    TResumeChaosCellsOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

    context->AnnotateRequest()
        .With("ChaosCellIds", cellIds);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->ResumeChaosCells(cellIds, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SuspendTabletCells)
{
    TSuspendTabletCellsOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

    context->AnnotateRequest()
        .With("TabletCellIds", cellIds);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->SuspendTabletCells(cellIds, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResumeTabletCells)
{
    TResumeTabletCellsOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto cellIds = FromProto<std::vector<TCellId>>(request->cell_ids());

    context->AnnotateRequest()
        .With("TabletCellIds", cellIds);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->ResumeTabletCells(cellIds, options);
        });
}

static EMaintenanceComponent MaintenanceComponentFromProto(
    NApi::NRpcProxy::NProto::EMaintenanceComponent component)
{
    using EProtoMaintenanceComponent = NApi::NRpcProxy::NProto::EMaintenanceComponent;

    switch (component) {
        case EProtoMaintenanceComponent::MC_CLUSTER_NODE:
            return EMaintenanceComponent::ClusterNode;
        case EProtoMaintenanceComponent::MC_HTTP_PROXY:
            return EMaintenanceComponent::HttpProxy;
        case EProtoMaintenanceComponent::MC_RPC_PROXY:
            return EMaintenanceComponent::RpcProxy;
        case EProtoMaintenanceComponent::MC_HOST:
            return EMaintenanceComponent::Host;
        default:
            THROW_ERROR_EXCEPTION("Invalid maintenance component %v",
                static_cast<int>(component));
    }
}

static EMaintenanceType MaintenanceTypeFromProto(
    NApi::NRpcProxy::NProto::EMaintenanceType type)
{
    using EProtoMaintenanceType = NApi::NRpcProxy::NProto::EMaintenanceType;

    switch (type) {
        case NApi::NRpcProxy::NProto::MT_BAN:
            return EMaintenanceType::Ban;
        case EProtoMaintenanceType::MT_DECOMMISSION:
            return EMaintenanceType::Decommission;
        case EProtoMaintenanceType::MT_DISABLE_WRITE_SESSIONS:
            return EMaintenanceType::DisableWriteSessions;
        case EProtoMaintenanceType::MT_DISABLE_TABLET_CELLS:
            return EMaintenanceType::DisableTabletCells;
        case EProtoMaintenanceType::MT_DISABLE_SCHEDULER_JOBS:
            return EMaintenanceType::DisableSchedulerJobs;
        case EProtoMaintenanceType::MT_PENDING_RESTART:
            return EMaintenanceType::PendingRestart;
        default:
            THROW_ERROR_EXCEPTION("Invalid maintenance type %v",
                static_cast<int>(type));
    }
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AddMaintenance)
{
    auto component = MaintenanceComponentFromProto(request->component());
    auto address = request->address();
    auto type = MaintenanceTypeFromProto(request->type());
    auto comment = request->comment();
    ValidateMaintenanceComment(comment);

    // COMPAT(kvk1920): For compatibility with pre-24.2 clients.
    auto supportsPerTargetResponse = request->supports_per_target_response();

    TAddMaintenanceOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("Component", component)
        .With("Address", address)
        .With("Type", type)
        .With("Comment", comment)
        .With("SupportsPerTargetResponse", supportsPerTargetResponse);

    auto client = GetAuthenticatedClientOrThrow(context, request);

    ExecuteCall(
        context,
        [=] {
            return client->AddMaintenance(component, address, type, comment, options);
        },
        [=] (const auto& context, const TMaintenanceIdPerTarget& result) {
            auto* response = &context->Response();

            context->AnnotateResponse()
                .With("MaintenanceIdPerTarget", result);

            // COMPAT(kvk1920): Compatibility with pre-24.2 RPC clients.
            if (!supportsPerTargetResponse) {
                ToProto(
                    response->mutable_id(),
                    result.size() == 1 ? result.begin()->second : TMaintenanceId{});
                return;
            }

            for (const auto& [target, id] : result) {
                ToProto(
                    &(response->mutable_id_per_target()->operator[](target)),
                    id);
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RemoveMaintenance)
{
    auto component = MaintenanceComponentFromProto(request->component());
    auto address = request->address();

    TStringBuilder requestInfo;
    requestInfo.AppendFormat("Component: %v, Address: %v",
        component,
        address);

    if (request->mine() && request->has_user()) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"user\" and \"mine\"");
    }

    TMaintenanceFilter filter;
    filter.Ids = FromProto<std::vector<TMaintenanceId>>(request->ids());

    if (request->has_type()) {
        filter.Type = MaintenanceTypeFromProto(request->type());
        requestInfo.AppendFormat(", Type: %v", filter.Type);
    }

    using TByUser = TMaintenanceFilter::TByUser;
    if (request->has_user()) {
        auto user = request->user();
        requestInfo.AppendFormat(", User: %v", user);
        filter.User = user;
    } else if (request->mine()) {
        filter.User = TByUser::TMine{};
        requestInfo.AppendString(", Mine: true");
    } else {
        filter.User = TByUser::TAll{};
    }

    // COMPAT(kvk1920): For compatibility with pre-24.2 RPC clients.
    auto supportsPerTargetResponse = request->supports_per_target_response();
    requestInfo.AppendFormat(
        ", SupportsPerTargetResponse: %v",
        supportsPerTargetResponse);

    TRemoveMaintenanceOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->SetRawRequestInfo(requestInfo.Flush(), /*incremental*/ false);

    auto client = GetAuthenticatedClientOrThrow(context, request);

    ExecuteCall(
        context,
        [=] {
            return client->RemoveMaintenance(component, address, filter);
        },
        [=] (const auto& context, const TMaintenanceCountsPerTarget& result) {
            auto& response = context->Response();

            auto fillMaintenanceCounts = [] (auto* protoMap, const TMaintenanceCounts& counts) {
                using namespace NApi::NRpcProxy::NProto;
                constexpr NApi::NRpcProxy::NProto::EMaintenanceType Types[] =  {
                    MT_BAN,
                    MT_DECOMMISSION,
                    MT_DISABLE_SCHEDULER_JOBS,
                    MT_DISABLE_WRITE_SESSIONS,
                    MT_DISABLE_TABLET_CELLS,
                    MT_PENDING_RESTART
                };

                for (auto type : Types) {
                    protoMap->insert({
                        type,
                        counts[MaintenanceTypeFromProto(type)]});
                }
            };

            // COMPAT(kvk1920): For compatibility with pre-24.2 RPC clients.
            if (!supportsPerTargetResponse) {
                TMaintenanceCounts totalCounts;
                for (const auto& [target, targetCounts] : result) {
                    for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                        totalCounts[type] += targetCounts[type];
                    }
                }

                // COMPAT(kvk1920): For compatibility with pre-23.2 RPC clients.
                response.set_ban(totalCounts[EMaintenanceType::Ban]);
                response.set_decommission(totalCounts[EMaintenanceType::Decommission]);
                response.set_disable_scheduler_jobs(totalCounts[EMaintenanceType::DisableSchedulerJobs]);
                response.set_disable_write_sessions(totalCounts[EMaintenanceType::DisableWriteSessions]);
                response.set_disable_tablet_cells(totalCounts[EMaintenanceType::DisableTabletCells]);
                response.set_pending_restart(totalCounts[EMaintenanceType::PendingRestart]);

                response.set_use_map_instead_of_fields(true);

                fillMaintenanceCounts(response.mutable_removed_maintenance_counts(), totalCounts);

                return;
            }

            response.set_supports_per_target_response(true);

            auto* protoMap = response.mutable_removed_maintenance_counts_per_target();
            for (const auto& [target, counts] : result) {
                fillMaintenanceCounts(
                    protoMap->operator[](target).mutable_counts(),
                    counts);
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, DisableChunkLocations)
{
    auto nodeAddress = request->node_address();
    auto locationUuids = request->location_uuids();

    TDisableChunkLocationsOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("NodeAddress", nodeAddress)
        .With("LocationUuids", locationUuids);

    auto client = GetAuthenticatedClientOrThrow(context, request);

    ExecuteCall(
        context,
        [=] {
            return client->DisableChunkLocations(
                nodeAddress,
                FromProto<std::vector<TGuid>>(request->location_uuids()),
                options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_location_uuids(), result.LocationUuids);

            context->AnnotateResponse()
                .With("LocationUuids", result.LocationUuids);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, DestroyChunkLocations)
{
    auto nodeAddress = request->node_address();
    auto locationUuids = request->location_uuids();
    auto recoverUnlinkedDisks = request->recover_unlinked_disks();

    TDestroyChunkLocationsOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("NodeAddress", nodeAddress)
        .With("RecoverUnlinkedDisks", recoverUnlinkedDisks)
        .With("LocationUuids", locationUuids);

    auto client = GetAuthenticatedClientOrThrow(context, request);

    ExecuteCall(
        context,
        [=] {
            return client->DestroyChunkLocations(
                nodeAddress,
                recoverUnlinkedDisks,
                FromProto<std::vector<TGuid>>(request->location_uuids()),
                options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_location_uuids(), result.LocationUuids);

            context->AnnotateResponse()
                .With("LocationUuids", result.LocationUuids);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ResurrectChunkLocations)
{
    auto nodeAddress = request->node_address();
    auto locationUuids = request->location_uuids();

    TResurrectChunkLocationsOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("NodeAddress", nodeAddress)
        .With("LocationUuids", locationUuids);

    auto client = GetAuthenticatedClientOrThrow(context, request);

    ExecuteCall(
        context,
        [=] {
            return client->ResurrectChunkLocations(
                nodeAddress,
                FromProto<std::vector<TGuid>>(request->location_uuids()),
                options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_location_uuids(), result.LocationUuids);

            context->AnnotateResponse()
                .With("LocationUuids", result.LocationUuids);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RequestRestart)
{
    auto nodeAddress = request->node_address();

    TRequestRestartOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("NodeAddress", nodeAddress);

    auto client = GetAuthenticatedClientOrThrow(context, request);
    ExecuteCall(
        context,
        [=] {
            return client->RequestRestart(
                nodeAddress,
                options);
        },
        [] (const auto& /*context*/, const auto& /*result*/) {
            // do nothing.
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CheckClusterLiveness)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TCheckClusterLivenessOptions options;
    SetTimeoutOptions(&options, context.Get());

    options.CheckCypressRoot = request->check_cypress_root();
    options.CheckSecondaryMasterCells = request->check_secondary_master_cells();
    if (request->has_check_tablet_cell_bundle()) {
        options.CheckTabletCellBundle = request->check_tablet_cell_bundle();
    }

    context->AnnotateRequest()
        .With("CheckCypressRoot", options.CheckCypressRoot)
        .With("CheckSecondaryMasterCells", options.CheckSecondaryMasterCells)
        .With("CheckTabletCellBundle", options.CheckTabletCellBundle);

    ExecuteCall(
        context,
        [=] {
            return client->CheckClusterLiveness(options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
