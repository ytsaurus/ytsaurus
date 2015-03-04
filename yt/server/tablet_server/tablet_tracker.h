#pragma once

#include "public.h"

#include <core/misc/small_set.h>

#include <core/concurrency/public.h>
#include <core/concurrency/thread_affinity.h>

#include <server/node_tracker_server//public.h>

#include <server/cell_master/public.h>

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

    void SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerFailover(TTabletCell* cell);

    bool IsFailoverNeeded(TTabletCell* cell, TPeerId peerId);

};

DEFINE_REFCOUNTED_TYPE(TTabletTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
