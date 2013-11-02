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

class TCandidatePool
{
public:
    explicit TCandidatePool(NCellMaster::TBootstrap* bootstrap);

    NNodeTrackerServer::TNode* TryAllocate(
        TTabletCell* cell,
        const TSmallSet<Stroka, TypicalCellSize>& forbiddenAddresses);

private:
    NCellMaster::TBootstrap* Bootstrap;

    yhash_set<NNodeTrackerServer::TNode*> Candidates;

    static bool HasFreeSlots(NNodeTrackerServer::TNode* node);

};

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
    TTabletManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TInstant StartTime;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;


    void ScanCells();

    void ScheduleStateChange(TTabletCell* cell);
    void SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerFailover(TTabletCell* cell);

    bool IsFailoverNeeded(TTabletCell* cell, TPeerId peerId);
    bool IsFailoverPossible(TTabletCell* cell);


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
