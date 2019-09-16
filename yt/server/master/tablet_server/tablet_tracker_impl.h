#pragma once

#include "public.h"
#include "tablet_cell.h"
#include "tablet_cell_balancer.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTrackerImpl
    : public TRefCounted
{
public:
    TTabletTrackerImpl(
        NCellMaster::TBootstrap* bootstrap,
        TInstant startTime);

    void ScanCells();

    using TBundleCounter = THashMap<NProfiling::TTagIdList, int>;

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TInstant StartTime_;
    const ITabletCellBalancerProviderPtr TTabletCellBalancerProvider_;
    const NProfiling::TProfiler Profiler;
    bool WaitForCommit_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnTabletCellPeersReassigned();

    const TDynamicTabletManagerConfigPtr& GetDynamicConfig();

    void Profile(
        const std::vector<TTabletCellMoveDescriptor>& moveDescriptors,
        const TBundleCounter& leaderReassignmentCounter,
        const TBundleCounter& peerRevocationCounter,
        const TBundleCounter& peerAssignmentCounter
    );

    void ScheduleLeaderReassignment(TTabletCell* cell, TBundleCounter* counter);
    void SchedulePeerAssignment(TTabletCell* cell, ITabletCellBalancer* balancer, TBundleCounter* counter);
    void SchedulePeerRevocation(TTabletCell* cell, ITabletCellBalancer* balancer, TBundleCounter* counter);

    TError IsFailed(
        const TTabletCell::TPeer& peer,
        const TBooleanFormula& nodeTagFilter,
        TDuration timeout);
    bool IsDecommissioned(
        const NNodeTrackerServer::TNode* node,
        const TBooleanFormula& nodeTagFilter);
    static int FindGoodPeer(const TTabletCell* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer


