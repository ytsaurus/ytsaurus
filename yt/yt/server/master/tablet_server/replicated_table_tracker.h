#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/ytlib/replicated_table_tracker_client/proto/replicated_table_tracker_client.pb.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct IMasterReplicatedTableTracker
    : public virtual TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(IMasterReplicatedTableTracker)

////////////////////////////////////////////////////////////////////////////////

IMasterReplicatedTableTrackerPtr CreateMasterReplicatedTableTracker(
    NTabletServer::TReplicatedTableTrackerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

struct IReplicatedTableTrackerStateProvider
    : public TRefCounted
{
    using TQueueDrainResult = NReplicatedTableTrackerClient::NProto::TRspGetTrackerStateUpdates;

    virtual void DrainUpdateQueue(
        TQueueDrainResult* result,
        NReplicatedTableTrackerClient::TTrackerStateRevision revision,
        bool snapshotRequested) = 0;

    virtual TFuture<NTabletServer::TApplyChangeReplicaCommandResults> ApplyChangeReplicaModeCommands(
        std::vector<TChangeReplicaModeCommand> commands) = 0;

    virtual TFuture<NTabletServer::TReplicaLagTimes> ComputeReplicaLagTimes(
        std::vector<NTabletClient::TTableReplicaId> replicaIds) = 0;

    // NB: Only leader can monitor the state.
    virtual void EnableStateMonitoring() = 0;
    virtual void DisableStateMonitoring() = 0;

    virtual bool IsEnabled() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicatedTableTrackerStateProvider)

////////////////////////////////////////////////////////////////////////////////

// NB: This entity provides RTT service with necessary Cypress state parts.
// E.g. it stores update queue that is drained upon remote RTT requests.
IReplicatedTableTrackerStateProviderPtr CreateReplicatedTableTrackerStateProvider(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
