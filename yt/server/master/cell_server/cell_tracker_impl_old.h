#pragma once

#include "public.h"

#include "cell_base.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerImplOld
{
public:
    TCellTrackerImplOld(
        NCellMaster::TBootstrap* bootstrap,
        TInstant startTime);

    void ScanCells();

private:
    class TCandidatePool;

    const NCellMaster::TBootstrap* Bootstrap_;
    const TInstant StartTime_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const TDynamicCellManagerConfigPtr& GetDynamicConfig();

    void ScheduleLeaderReassignment(TCellBase* cell, TCandidatePool* pool);
    void SchedulePeerAssignment(TCellBase* cell, TCandidatePool* pool);
    void SchedulePeerRevocation(TCellBase* cell);

    bool IsFailed(
        const TCellBase::TPeer& peer,
        const TBooleanFormula& nodeTagFilter,
        TDuration timeout);
    static bool IsGood(const NNodeTrackerServer::TNode* node);
    static int FindGoodPeer(const TCellBase* cell);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
