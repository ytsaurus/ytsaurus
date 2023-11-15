#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/ytlib/replicated_table_tracker_client/proto/replicated_table_tracker_client.pb.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTracker
    : public TRefCounted
{
public:
    TReplicatedTableTracker(
        NTabletServer::TReplicatedTableTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    ~TReplicatedTableTracker();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TReplicatedTableTracker)

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

// NB: This entity provides Rtt service with necessary Cypress state parts.
// E.g. it stores update queue that is drained upon remote Rtt requests.
IReplicatedTableTrackerStateProviderPtr CreateReplicatedTableTrackerStateProvider(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
