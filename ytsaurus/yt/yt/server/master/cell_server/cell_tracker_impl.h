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
    void ScheduleLeaderReassignment(TCellBase* cell);
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
