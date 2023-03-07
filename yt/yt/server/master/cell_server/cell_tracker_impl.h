#pragma once

#include "public.h"
#include "cell_base.h"
#include "cell_balancer.h"
#include "config.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/public.h>

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

    using TBundleCounter = THashMap<NProfiling::TTagIdList, int>;

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TInstant StartTime_;
    const ICellBalancerProviderPtr TCellBalancerProvider_;
    const NProfiling::TProfiler Profiler;
    bool WaitForCommit_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnCellPeersReassigned();

    const TDynamicCellManagerConfigPtr& GetDynamicConfig();

    void Profile(
        const std::vector<TCellMoveDescriptor>& moveDescriptors,
        const TBundleCounter& leaderReassignmentCounter,
        const TBundleCounter& peerRevocationCounter,
        const TBundleCounter& peerAssignmentCounter
    );

    void ScheduleLeaderReassignment(TCellBase* cell, TBundleCounter* counter);
    void SchedulePeerAssignment(TCellBase* cell, ICellBalancer* balancer, TBundleCounter* counter);
    void SchedulePeerRevocation(TCellBase* cell, ICellBalancer* balancer, TBundleCounter* counter);
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
