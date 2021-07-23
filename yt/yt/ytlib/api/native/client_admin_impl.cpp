#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "private.h"

#include <yt/yt/ytlib/admin/admin_service_proxy.h>

#include <yt/yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/yt/ytlib/exec_node_admin/exec_node_admin_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using namespace NAdmin;
using namespace NConcurrency;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NNodeTrackerClient;
using namespace NHydra;
using namespace NHiveClient;
using namespace NJobTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NExecNode;

////////////////////////////////////////////////////////////////////////////////

int TClient::DoBuildSnapshot(const TBuildSnapshotOptions& options)
{
    ValidateSuperuserPermissions();

    auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
    auto channel = GetLeaderCellChannelOrThrow(cellId);

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
    ValidateSuperuserPermissions();

    using TResponseFuture = TFuture<TIntrusivePtr<TTypedClientResponse<NHydra::NProto::TRspForceBuildSnapshot>>>;
    struct TSnapshotRequest
    {
        TResponseFuture Future;
        TCellId CellId;
    };

    auto constructRequest = [&] (TCellId cellId) {
        auto channel = GetLeaderCellChannelOrThrow(cellId);
        THydraServiceProxy proxy(channel);
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

    std::queue<TSnapshotRequest> requestQueue;
    auto enqueueRequest = [&] (TCellId cellId) {
        YT_LOG_INFO("Requesting cell to build a snapshot (CellId: %v)", cellId);
        auto request = constructRequest(cellId);
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
    ValidateSuperuserPermissions();

    if (TypeFromId(cellId) != EObjectType::MasterCell) {
        THROW_ERROR_EXCEPTION("%v is not a valid cell id",
            cellId);
    }

    auto addresses = GetCellAddressesOrThrow(cellId);
    auto currentLeaderChannel = GetLeaderCellChannelOrThrow(cellId);

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

    {
        YT_LOG_INFO("Validating new leader");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.GetPeerState();
        req->SetTimeout(options.Timeout);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            auto state = FromProto<EPeerState>(rspOrError.Value()->peer_state());
            if (state != EPeerState::Following) {
                THROW_ERROR_EXCEPTION("Invalid peer state: expected %Qlv, found %Qlv",
                    EPeerState::Following,
                    state);
            }
        } else {
            auto error = TError(rspOrError);
            // COMPAT(gritukan)
            if (error.FindMatching(NRpc::EErrorCode::NoSuchMethod)) {
                YT_LOG_INFO("Remote Hydra is too old, leader validation cannot be performed");
            } else {
                THROW_ERROR error;
            }
        }
    }

    {
        YT_LOG_INFO("Preparing switch at current leader");

        THydraServiceProxy proxy(currentLeaderChannel);
        auto req = proxy.PrepareLeaderSwitch();
        req->SetTimeout(options.Timeout);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    {
        YT_LOG_INFO("Synchronizing new leader with the current one");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.ForceSyncWithLeader();
        req->SetTimeout(options.Timeout);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    TError restartReason(
        "Switching leader to %v by %Qv request",
        newLeaderAddress,
        Options_.User);

    {
        YT_LOG_INFO("Restarting new leader with priority boost armed");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.ForceRestart();
        req->SetTimeout(options.Timeout);
        ToProto(req->mutable_reason(), restartReason);
        req->set_arm_priority_boost(true);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    {
        YT_LOG_INFO("Restarting all other peers");

        YT_VERIFY(peerChannels.size() == addresses.size());
        for (int peerId = 0; peerId < std::ssize(peerChannels); ++peerId) {
            const auto& peerAddress = addresses[peerId];
            if (peerAddress == newLeaderAddress) {
                continue;
            }

            THydraServiceProxy proxy(peerChannels[peerId]);
            auto req = proxy.ForceRestart();
            req->SetTimeout(options.Timeout);
            ToProto(req->mutable_reason(), restartReason);

            // Fire-and-forget.
            req->Invoke();
        }
    }
}

void TClient::DoGCCollect(const TGCCollectOptions& options)
{
    ValidateSuperuserPermissions();

    auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
    auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellId);

    TObjectServiceProxy proxy(channel);
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

TString TClient::DoWriteOperationControllerCoreDump(
    TOperationId operationId,
    const TWriteOperationControllerCoreDumpOptions& /*options*/)
{
    ValidateSuperuserPermissions();

    auto address = FindControllerAgentAddressFromCypress(
        operationId,
        Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Follower));
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

void TClient::DoRepairExecNode(
    const TString& address,
    const TRepairExecNodeOptions& options)
{
    ValidateSuperuserPermissions();
    auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

    TExecNodeAdminServiceProxy proxy(channel);
    auto req = proxy.RepairNode();

    for (const auto& location : options.Locations) {
        req->add_locations(location);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
