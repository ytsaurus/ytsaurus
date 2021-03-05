#pragma once

#include "public.h"
#include "cell_base.h"
#include "cell_balancer.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/public.h>

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
    const ICellBalancerProviderPtr TCellBalancerProvider_;
    bool WaitForCommit_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnCellPeersReassigned();

    const TDynamicCellManagerConfigPtr& GetDynamicConfig();

    void Profile(const std::vector<TCellMoveDescriptor>& moveDescriptors);

    void ScheduleLeaderReassignment(TCellBase* cell);
    void SchedulePeerAssignment(TCellBase* cell, ICellBalancer* balancer);
    void SchedulePeerRevocation(TCellBase* cell, ICellBalancer* balancer);
    bool SchedulePeerCountChange(TCellBase* cell, NTabletServer::NProto::TReqReassignPeers* request);

    TError IsFailed(
        const TCellBase::TPeer& peer,
        const TCellBundle* bundle,
        TDuration timeout);
    bool IsDecommissioned(
        const NNodeTrackerServer::TNode* node,
        const TCellBundle* bundle);

    static TPeerId FindGoodPeer(const TCellBase* cell);
    static TPeerId FindGoodFollower(const TCellBase* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
