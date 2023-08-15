#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "helpers.h"
#include "private.h"
#include "transaction.h"

#include <yt/yt/ytlib/admin/admin_service_proxy.h>
#include <yt/yt/ytlib/admin/reboot_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/journal_client/chunk_reader.h>

#include <yt/yt/ytlib/exec_node_admin/exec_node_admin_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>
#include <yt/yt/ytlib/hydra/helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/client/api/helpers.h>

#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/client/node_tracker_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using namespace NAdmin;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NExecNode;
using namespace NHiveClient;
using namespace NHydra;
using namespace NJobTrackerClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NRpc;
using namespace NScheduler;
using namespace NTabletClient;
using namespace NYson;
using namespace NYTree;

using NApi::ValidateMaintenanceComment;

////////////////////////////////////////////////////////////////////////////////

int TClient::DoBuildSnapshot(const TBuildSnapshotOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::BuildSnapshot,
        EPermission::Use);

    auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
    auto channel = GetHydraAdminChannelOrThrow(cellId);

    THydraServiceProxy proxy(channel);
    auto req = proxy.ForceBuildSnapshot();
    req->SetTimeout(options.Timeout);
    req->set_set_read_only(options.SetReadOnly);
    req->set_wait_for_snapshot_completion(options.WaitForSnapshotCompletion);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return rsp->snapshot_id();
}

TCellIdToSnapshotIdMap TClient::DoBuildMasterSnapshots(const TBuildMasterSnapshotsOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::BuildMasterSnapshot,
        EPermission::Use);

    using TResponseFuture = TFuture<TIntrusivePtr<TTypedClientResponse<NHydra::NProto::TRspForceBuildSnapshot>>>;
    struct TSnapshotRequest
    {
        TResponseFuture Future;
        TCellId CellId;
    };

    auto constructRequest = [&] (IChannelPtr channel) {
        THydraServiceProxy proxy(std::move(channel));
        auto req = proxy.ForceBuildSnapshot();
        req->SetTimeout(options.Timeout);
        req->set_set_read_only(options.SetReadOnly);
        req->set_wait_for_snapshot_completion(options.WaitForSnapshotCompletion);
        return req;
    };

    std::vector<TCellId> cellIds;

    cellIds.push_back(Connection_->GetPrimaryMasterCellId());

    for (auto cellTag : Connection_->GetSecondaryMasterCellTags()) {
        cellIds.push_back(Connection_->GetMasterCellId(cellTag));
    }

    THashMap<TCellId, IChannelPtr> channels;
    for (auto cellId : cellIds) {
        EmplaceOrCrash(channels, std::make_pair(cellId, GetHydraAdminChannelOrThrow(cellId)));
    }

    std::queue<TSnapshotRequest> requestQueue;
    auto enqueueRequest = [&] (TCellId cellId) {
        YT_LOG_INFO("Requesting cell to build a snapshot (CellId: %v)", cellId);
        auto request = constructRequest(channels[cellId]);
        requestQueue.push({request->Invoke(), cellId});
    };

    for (auto cellId : cellIds) {
        enqueueRequest(cellId);
    }

    THashMap<TCellId, int> cellIdToSnapshotId;
    while (!requestQueue.empty()) {
        auto request = requestQueue.front();
        requestQueue.pop();

        auto cellId = request.CellId;
        YT_LOG_INFO("Waiting for snapshot (CellId: %v)", cellId);
        auto snapshotIdOrError = WaitFor(request.Future);
        if (snapshotIdOrError.IsOK()) {
            auto snapshotId = snapshotIdOrError.Value()->snapshot_id();
            YT_LOG_INFO("Snapshot built successfully (CellId: %v, SnapshotId: %v)", cellId, snapshotId);
            cellIdToSnapshotId[cellId] = snapshotId;
        } else {
            auto errorCode = snapshotIdOrError.GetCode();
            if (errorCode == NHydra::EErrorCode::ReadOnlySnapshotBuilt) {
                YT_LOG_INFO("Skipping cell since it is already in read-only mode and has a valid snapshot (CellId: %v)", cellId);
                auto snapshotId = snapshotIdOrError.Attributes().Get<int>("snapshot_id");
                cellIdToSnapshotId[cellId] = snapshotId;
            } else if (options.Retry && errorCode != NHydra::EErrorCode::ReadOnlySnapshotBuildFailed) {
                YT_LOG_INFO(snapshotIdOrError, "Failed to build snapshot; retrying (CellId: %v)", cellId);
                enqueueRequest(cellId);
            } else {
                snapshotIdOrError.ThrowOnError();
            }
        }
    }

    return cellIdToSnapshotId;
}

void TClient::DoSwitchLeader(
    TCellId cellId,
    const TString& newLeaderAddress,
    const TSwitchLeaderOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::SwitchLeader,
        EPermission::Use);

    if (TypeFromId(cellId) != EObjectType::MasterCell) {
        THROW_ERROR_EXCEPTION("%v is not a valid cell id",
            cellId);
    }

    auto addresses = GetCellAddressesOrThrow(cellId);
    auto currentLeaderChannel = GetHydraAdminChannelOrThrow(cellId);

    std::vector<IChannelPtr> peerChannels;
    peerChannels.reserve(addresses.size());

    auto createChannel = [&] (const TString& address) {
        return CreateRealmChannel(
            Connection_->GetChannelFactory()->CreateChannel(address),
            cellId);
    };

    for (const auto& address : addresses) {
        peerChannels.push_back(createChannel(address));
    }

    auto newLeaderChannel = createChannel(newLeaderAddress);

    NHydra::SwitchLeader(
        peerChannels,
        currentLeaderChannel,
        newLeaderChannel,
        addresses,
        newLeaderAddress,
        options.Timeout,
        Options_.User);
}

void TClient::DoResetStateHash(
    TCellId cellId,
    const TResetStateHashOptions& options)
{
    ValidateSuperuserPermissions();

    auto channel = GetCellChannelOrThrow(cellId);
    THydraServiceProxy proxy(std::move(channel));

    auto newStateHash = options.NewStateHash.value_or(RandomNumber<ui64>());
    auto req = proxy.ResetStateHash();
    req->set_new_state_hash(newStateHash);
    req->SetTimeout(options.Timeout);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoGCCollect(const TGCCollectOptions& options)
{
    ValidateSuperuserPermissions();

    auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
    auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellId);
    auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(channel));
    auto req = proxy.GCCollect();
    req->SetTimeout(options.Timeout);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoKillProcess(const TString& address, const TKillProcessOptions& options)
{
    ValidateSuperuserPermissions();

    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

    TAdminServiceProxy proxy(channel);
    auto req = proxy.Die();
    req->set_exit_code(options.ExitCode);

    // NB: this will always throw an error since the service can
    // never reply to the request because it makes _exit immediately.
    // This is the intended behavior.
    WaitFor(req->Invoke())
        .ThrowOnError();
}

TString TClient::DoWriteCoreDump(const TString& address, const TWriteCoreDumpOptions& /*options*/)
{
    ValidateSuperuserPermissions();

    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

    TAdminServiceProxy proxy(channel);
    auto req = proxy.WriteCoreDump();
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return rsp->path();
}

TGuid TClient::DoWriteLogBarrier(const TString& address, const TWriteLogBarrierOptions& options)
{
    ValidateSuperuserPermissions();

    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

    TAdminServiceProxy proxy(channel);
    auto req = proxy.WriteLogBarrier();
    req->SetTimeout(options.Timeout);
    req->set_category(options.Category);
    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    TGuid result;
    FromProto(&result, rsp->barrier_id());
    return result;
}

TString TClient::DoWriteOperationControllerCoreDump(
    TOperationId operationId,
    const TWriteOperationControllerCoreDumpOptions& /*options*/)
{
    ValidateSuperuserPermissions();

    auto address = FindControllerAgentAddressFromCypress(
        operationId,
        MakeStrong(this));
    if (!address) {
        THROW_ERROR_EXCEPTION("Cannot find address of the controller agent for operation %v",
            operationId);
    }

    auto channel = Connection_->GetChannelFactory()->CreateChannel(*address);

    TControllerAgentServiceProxy proxy(channel);
    auto req = proxy.WriteOperationControllerCoreDump();
    ToProto(req->mutable_operation_id(), operationId);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return rsp->path();
}

void TClient::DoHealExecNode(
    const TString& address,
    const THealExecNodeOptions& options)
{
    ValidateSuperuserPermissions();
    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

    TExecNodeAdminServiceProxy proxy(channel);
    auto req = proxy.HealNode();

    for (const auto& location : options.Locations) {
        req->add_locations(location);
    }
    for (const auto& alertType : options.AlertTypesToReset) {
        req->add_alert_types_to_reset(alertType);
    }
    req->set_force_reset(options.ForceReset);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoSuspendCoordinator(
    TCellId coordinatorCellId,
    const TSuspendCoordinatorOptions& options)
{
    auto channel = GetChaosChannelByCellTag(CellTagFromId(coordinatorCellId), EPeerKind::Leader);
    auto proxy = TCoordinatorServiceProxy(std::move(channel));

    auto req = proxy.SuspendCoordinator();
    SetMutationId(req, options);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoResumeCoordinator(
    TCellId coordinatorCellId,
    const TResumeCoordinatorOptions& options)
{
    auto channel = GetChaosChannelByCellTag(CellTagFromId(coordinatorCellId), EPeerKind::Leader);
    auto proxy = TCoordinatorServiceProxy(std::move(channel));

    auto req = proxy.ResumeCoordinator();
    SetMutationId(req, options);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoMigrateReplicationCards(
    TCellId chaosCellId,
    const TMigrateReplicationCardsOptions& options)
{
    auto channel = GetChaosChannelByCellTag(CellTagFromId(chaosCellId), EPeerKind::Leader);
    auto proxy = TChaosNodeServiceProxy(std::move(channel));

    auto destinationCellId = options.DestinationCellId;
    if (!destinationCellId) {
        auto siblingCellTag = GetSiblingChaosCellTag(CellTagFromId(chaosCellId));
        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto descriptor = cellDirectory->FindDescriptorByCellTag(siblingCellTag);
        if (!descriptor) {
            THROW_ERROR_EXCEPTION("Unable to identify sibling cell to migrate replication cards into")
                << TErrorAttribute("chaos_cell_id", chaosCellId)
                << TErrorAttribute("sibling_cell_tag", siblingCellTag);
        }

        destinationCellId = descriptor->CellId;
    }

    auto req = proxy.MigrateReplicationCards();
    SetMutationId(req, options);
    ToProto(req->mutable_migrate_to_cell_id(), destinationCellId);
    ToProto(req->mutable_replication_card_ids(), options.ReplicationCardIds);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoSuspendChaosCells(
    const std::vector<TCellId>& cellIds,
    const TSuspendChaosCellsOptions& options)
{
    SyncCellsIfNeeded(cellIds);

    std::vector<TFuture<void>> futures;
    futures.reserve(cellIds.size());

    for (auto chaosCellId : cellIds) {
        {
            // Migrate replication cards.
            auto channel = GetChaosChannelByCellTag(CellTagFromId(chaosCellId), EPeerKind::Leader);
            auto proxy = TChaosNodeServiceProxy(std::move(channel));

            auto siblingCellTag = GetSiblingChaosCellTag(CellTagFromId(chaosCellId));
            const auto& cellDirectory = Connection_->GetCellDirectory();
            auto descriptor = cellDirectory->FindDescriptorByCellTag(siblingCellTag);
            if (!descriptor) {
                THROW_ERROR_EXCEPTION("Unable to identify sibling cell to migrate replication cards into")
                    << TErrorAttribute("chaos_cell_id", chaosCellId)
                    << TErrorAttribute("sibling_cell_tag", siblingCellTag);
            }

            auto req = proxy.MigrateReplicationCards();
            SetMutationId(req, options);
            ToProto(req->mutable_migrate_to_cell_id(), descriptor->CellId);
            req->set_migrate_all_replication_cards(true);
            req->set_suspend_chaos_cell(true);

            futures.push_back(req->Invoke().AsVoid());
        }

        {
            // Suspend coordination.
            auto channel = GetChaosChannelByCellTag(CellTagFromId(chaosCellId), EPeerKind::Leader);
            auto proxy = TCoordinatorServiceProxy(std::move(channel));

            auto req = proxy.SuspendCoordinator();
            SetMutationId(req, options);

            futures.push_back(req->Invoke().AsVoid());
        }
    }

    return WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

void TClient::DoResumeChaosCells(
    const std::vector<TCellId>& cellIds,
    const TResumeChaosCellsOptions& options)
{
    SyncCellsIfNeeded(cellIds);

    std::vector<TFuture<void>> futures;
    futures.reserve(cellIds.size());

    for (auto chaosCellId : cellIds) {
        {
            // Resume replication card creation.
            auto channel = GetChaosChannelByCellTag(CellTagFromId(chaosCellId), EPeerKind::Leader);
            auto proxy = TChaosNodeServiceProxy(std::move(channel));

            auto req = proxy.ResumeChaosCell();
            req->SetTimeout(options.Timeout);
            futures.push_back(req->Invoke().AsVoid());
        }

        {
            // Resume coordination.
            auto channel = GetChaosChannelByCellTag(CellTagFromId(chaosCellId), EPeerKind::Leader);
            auto proxy = TCoordinatorServiceProxy(std::move(channel));

            auto req = proxy.ResumeCoordinator();
            SetMutationId(req, options);

            futures.push_back(req->Invoke().AsVoid());
        }
    }

    return WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

void TClient::DoSuspendTabletCells(
    const std::vector<TCellId>& cellIds,
    const TSuspendTabletCellsOptions& options)
{
    SyncCellsIfNeeded(cellIds);

    std::vector<TFuture<void>> futures;
    futures.reserve(cellIds.size());

    for (auto cellId : cellIds) {
        auto channel = GetCellChannelOrThrow(cellId);
        TTabletServiceProxy proxy(std::move(channel));

        auto req = proxy.SuspendTabletCell();
        req->SetTimeout(options.Timeout);
        futures.push_back(req->Invoke().AsVoid());
    }

    return WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

void TClient::DoResumeTabletCells(
    const std::vector<TCellId>& cellIds,
    const TResumeTabletCellsOptions& options)
{
    SyncCellsIfNeeded(cellIds);

    std::vector<TFuture<void>> futures;
    futures.reserve(cellIds.size());

    for (auto cellId : cellIds) {
        auto channel = GetCellChannelOrThrow(cellId);
        TTabletServiceProxy proxy(std::move(channel));

        auto req = proxy.ResumeTabletCell();
        req->SetTimeout(options.Timeout);
        futures.push_back(req->Invoke().AsVoid());
    }

    return WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

TMaintenanceId TClient::DoAddMaintenance(
    EMaintenanceComponent component,
    const TString& address,
    EMaintenanceType type,
    const TString& comment,
    const TAddMaintenanceOptions& options)
{
    ValidateMaintenanceComment(comment);

    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchRequest = proxy->ExecuteBatch();
    batchRequest->SetSuppressTransactionCoordinatorSync(true);

    auto request = TMasterYPathProxy::AddMaintenance();
    request->set_component(static_cast<int>(component));
    request->set_address(address);
    request->set_type(static_cast<int>(type));
    request->set_comment(comment);
    batchRequest->AddRequest(request);
    batchRequest->SetTimeout(options.Timeout);

    auto batchResponse = WaitFor(batchRequest->Invoke())
        .ValueOrThrow();

    auto response = batchResponse
        ->GetResponse<TMasterYPathProxy::TRspAddMaintenance>(0)
        .ValueOrThrow();

    return FromProto<TMaintenanceId>(response->id());
}

TMaintenanceCounts TClient::DoRemoveMaintenance(
    EMaintenanceComponent component,
    const TString& address,
    const TMaintenanceFilter& filter,
    const TRemoveMaintenanceOptions& options)
{
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    auto batchRequest = proxy->ExecuteBatch();
    batchRequest->SetSuppressTransactionCoordinatorSync(true);

    auto request = TMasterYPathProxy::RemoveMaintenance();
    request->set_component(static_cast<int>(component));
    request->set_address(address);

    ToProto(request->mutable_ids(), filter.Ids);

    if (filter.Type) {
        request->set_type(static_cast<int>(*filter.Type));
    }

    using TByUser = TMaintenanceFilter::TByUser;
    Visit(
        filter.User,
        [] (TByUser::TAll) { },
        [&] (TByUser::TMine) {
            request->set_mine(true);
        },
        [&] (const TString& user) {
            request->set_user(user);
        });

    batchRequest->AddRequest(request);
    batchRequest->SetTimeout(options.Timeout);

    auto batchResponse = WaitFor(batchRequest->Invoke())
        .ValueOrThrow();
    auto response = batchResponse->GetResponse<TMasterYPathProxy::TRspRemoveMaintenance>(0)
        .ValueOrThrow();

    TMaintenanceCounts result;

    if (response->use_map_instead_of_fields()) {
        const auto& removedMaintenanceCounts = response->removed_maintenance_counts();
        for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
            auto it = removedMaintenanceCounts.find(static_cast<int>(type));
            result[type] = it == removedMaintenanceCounts.end() ? 0 : it->second;
        }
    } else {
        result[EMaintenanceType::Ban] = response->ban();
        result[EMaintenanceType::Decommission] = response->decommission();
        result[EMaintenanceType::DisableSchedulerJobs] = response->disable_scheduler_jobs();
        result[EMaintenanceType::DisableWriteSessions] = response->disable_write_sessions();
        result[EMaintenanceType::DisableTabletCells] = response->disable_tablet_cells();
        result[EMaintenanceType::PendingRestart] = response->pending_restart();
    }

    return result;
}

TDisableChunkLocationsResult TClient::DoDisableChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TDisableChunkLocationsOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::DisableChunkLocations,
        EPermission::Use);

    TDataNodeServiceProxy proxy(Connection_->GetChannelFactory()->CreateChannel(nodeAddress));

    auto req = proxy.DisableChunkLocations();
    ToProto(req->mutable_location_uuids(), locationUuids);
    req->SetTimeout(options.Timeout);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return TDisableChunkLocationsResult{
        .LocationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids())
    };
}

TDestroyChunkLocationsResult TClient::DoDestroyChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TDestroyChunkLocationsOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::DestroyChunkLocations,
        EPermission::Use);

    TDataNodeServiceProxy proxy(Connection_->GetChannelFactory()->CreateChannel(nodeAddress));

    auto req = proxy.DestroyChunkLocations();
    ToProto(req->mutable_location_uuids(), locationUuids);
    req->SetTimeout(options.Timeout);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return TDestroyChunkLocationsResult{
        .LocationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids())
    };
}

TResurrectChunkLocationsResult TClient::DoResurrectChunkLocations(
    const TString& nodeAddress,
    const std::vector<TGuid>& locationUuids,
    const TResurrectChunkLocationsOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::ResurrectChunkLocations,
        EPermission::Use);

    TDataNodeServiceProxy proxy(Connection_->GetChannelFactory()->CreateChannel(nodeAddress));

    auto req = proxy.ResurrectChunkLocations();
    ToProto(req->mutable_location_uuids(), locationUuids);
    req->SetTimeout(options.Timeout);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return TResurrectChunkLocationsResult{
        .LocationUuids = FromProto<std::vector<TGuid>>(rsp->location_uuids())
    };
}

TRequestRebootResult TClient::DoRequestReboot(
    const TString& nodeAddress,
    const TRequestRebootOptions& options)
{
    ValidatePermissionsWithAcn(
        EAccessControlObject::RequestReboot,
        EPermission::Use);

    TRebootServiceProxy proxy(Connection_->GetChannelFactory()->CreateChannel(nodeAddress));

    auto req = proxy.RequestReboot();
    req->SetTimeout(options.Timeout);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    return TRequestRebootResult();
}

void TClient::SyncCellsIfNeeded(const std::vector<TCellId>& cellIds)
{
    bool needToSyncCells = false;

    const auto& cellDirectory = Connection_->GetCellDirectory();
    for (auto cellId : cellIds) {
        if (!cellDirectory->FindChannelByCellId(cellId)) {
            needToSyncCells = true;
        }
    }

    if (needToSyncCells) {
        const auto& cellDirectorySynchronizer = Connection_->GetCellDirectorySynchronizer();
        WaitFor(cellDirectorySynchronizer->Sync())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
