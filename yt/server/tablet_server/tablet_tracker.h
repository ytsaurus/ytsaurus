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
    explicit TTabletTracker(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    class TCandidatePool;

    TTabletManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void ScanCells();

    void SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerFailover(TTabletCell* cell);

    bool IsFailoverNeeded(TTabletCell* cell, TPeerId peerId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
