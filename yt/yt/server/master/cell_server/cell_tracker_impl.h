#pragma once

#include "public.h"
#include "cell_base.h"
#include "cell_balancer.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/cell_balancer/proto/cell_tracker_service.pb.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerImpl
    : public TRefCounted
{
public:
    TCellTrackerImpl(
        NCellMaster::TBootstrap* bootstrap,
        TInstant startTime);

    void ScanCells();

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TInstant StartTime_;

    THashMap<NCellarClient::ECellarType, ICellBalancerProviderPtr> PerCellarProviders_;
    bool WaitForCommit_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnCellPeersReassigned();

    const NTabletServer::TDynamicTabletManagerConfigPtr& GetDynamicConfig();

    void Profile(const std::vector<TCellMoveDescriptor>& moveDescriptors);

    void ScanCellarCells(NCellarClient::ECellarType cellarType);

    // Returns true if it is required to reassign leader of the cell
    // due to the decommission through extra peers. Note that leader
    // switch may not be possible right now, for example, due to
    // unfinished follower recovery.
    bool IsLeaderReassignmentRequired(TCellBase* cell);
    // If it is possible to reassign leader right now, returns the new leading peer id.
    // Otherwise, returns #InvalidPeerId.
    TPeerId FindNewLeadingPeerId(TCellBase* cell);

    void ScheduleLeaderReassignment(TCellBase* cell, TPeerId newLeaderPeerId);

    void SchedulePeerAssignment(TCellBase* cell, ICellBalancer* balancer);
    void SchedulePeerRevocation(TCellBase* cell, ICellBalancer* balancer);
    bool SchedulePeerCountChange(TCellBase* cell, NCellBalancerClient::NProto::TReqReassignPeers* request);

    TError IsFailed(
        const TCellBase::TPeer& peer,
        const TCellBase* cell,
        TDuration timeout);
    bool IsDecommissioned(
        const NNodeTrackerServer::TNode* node,
        const TCellBase* cell);

    static TPeerId FindGoodPeer(const TCellBase* cell);
    static TPeerId FindGoodFollower(const TCellBase* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
