#pragma once

#include "public.h"

#include "tablet_cell.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTrackerImplOld
{
public:
    TTabletTrackerImplOld(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap,
        TInstant startTime);

    void ScanCells();

private:
    class TCandidatePool;

    const TTabletManagerConfigPtr Config_;
    const NCellMaster::TBootstrap* Bootstrap_;
    const TInstant StartTime_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void ScheduleLeaderReassignment(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerAssignment(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerRevocation(TTabletCell* cell);

    bool IsFailed(
        const TTabletCell::TPeer& peer,
        const TBooleanFormula& nodeTagFilter,
        TDuration timeout);
    static bool IsGood(const NNodeTrackerServer::TNode* node);
    static int FindGoodPeer(const TTabletCell* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
