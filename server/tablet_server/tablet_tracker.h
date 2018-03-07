#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server//public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/small_set.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker
    : public TRefCounted
{
public:
    TTabletTracker(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    class TCandidatePool;

    const TTabletManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    TNullable<bool> LastEnabled_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    bool IsEnabled();
    void ScanCells();

    void ScheduleLeaderReassignment(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerAssignment(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerRevocation(TTabletCell* cell);

    bool IsFailed(const TTabletCell* cell, TPeerId peerId, TDuration timeout);
    static bool IsGood(const NNodeTrackerServer::TNode* node);
    static int FindGoodPeer(const TTabletCell* cell);

};

DEFINE_REFCOUNTED_TYPE(TTabletTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
