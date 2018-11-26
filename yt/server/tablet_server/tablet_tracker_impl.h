#pragma once

#include "public.h"
#include "tablet_cell.h"
#include "tablet_cell_balancer.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTrackerImpl
{
public:
    TTabletTrackerImpl(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        TInstant startTime);

    void ScanCells();

private:
    const TTabletManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;
    const TInstant StartTime_;
    const ITabletCellBalancerProviderPtr TTabletCellBalancerProvider_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void ScheduleLeaderReassignment(TTabletCell* cell);
    void SchedulePeerAssignment(TTabletCell* cell, ITabletCellBalancer* balancer);
    void SchedulePeerRevocation(TTabletCell* cell, ITabletCellBalancer* balancer);

    TError IsFailed(
        const TTabletCell::TPeer& peer,
        const TBooleanFormula& nodeTagFilter,
        TDuration timeout);
    static int FindGoodPeer(const TTabletCell* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT


