#include "stdafx.h"
#include "tablet_tracker.h"
#include "tablet_manager.h"
#include "tablet_cell.h"
#include "config.h"

#include <core/concurrency/periodic_executor.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/table_server/table_node.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NObjectServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

static const TDuration CellsScanPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TCandidatePool::TCandidatePool(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{
    auto nodeTracker = Bootstrap->GetNodeTracker();
    for (auto* node : nodeTracker->Nodes().GetValues()) {
        if (HasAvailableSlots(node)) {
            YCHECK(Candidates.insert(node).second);
        }
    }
}

TNode* TCandidatePool::TryAllocate(
    TTabletCell* cell,
    const TSmallSet<Stroka, TypicalCellSize>& forbiddenAddresses)
{
    for (auto it = Candidates.begin(); it != Candidates.end(); ++it) {
        auto* node = *it;
        if (forbiddenAddresses.count(node->GetAddress()) == 0) {
            node->AddTabletSlotHint();
            if (!HasAvailableSlots(node)) {
                Candidates.erase(it);
            }
            return node;
        }
    }
    return nullptr;
}

bool TCandidatePool::HasAvailableSlots(TNode* node)
{
    return node->GetTotalUsedTabletSlots() < node->GetTotalTabletSlots();
}

////////////////////////////////////////////////////////////////////////////////

TTabletTracker::TTabletTracker(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(Config);
    YCHECK(Bootstrap);
    VERIFY_INVOKER_AFFINITY(Bootstrap->GetMetaStateFacade()->GetInvoker(), AutomatonThread);
}

void TTabletTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime = TInstant::Now();

    YCHECK(!PeriodicExecutor);
    PeriodicExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(),
        BIND(&TTabletTracker::ScanCells, MakeWeak(this)),
        CellsScanPeriod);
    PeriodicExecutor->Start();
}

void TTabletTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor) {
        PeriodicExecutor->Stop();
        PeriodicExecutor.Reset();
    }
}

void TTabletTracker::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TCandidatePool pool(Bootstrap);

    auto tabletManger = Bootstrap->GetTabletManager();
    for (auto* cell : tabletManger->TabletCells().GetValues()) {
        if (!IsObjectAlive(cell))
            continue;

        ScheduleStateChange(cell);
        SchedulePeerStart(cell, &pool);
        SchedulePeerFailover(cell);
    }
}

void TTabletTracker::ScheduleStateChange(TTabletCell* cell)
{
    if (cell->GetState() != ETabletCellState::Starting)
        return;

    if (cell->GetOnlinePeerCount() < cell->GetSize())
        return;

    // All peers online, change state to running.
    TReqSetCellState request;
    ToProto(request.mutable_cell_id(), cell->GetId());
    request.set_state(ETabletCellState::Running);

    auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
    CreateMutation(hydraManager, request)
        ->Commit();
}

void TTabletTracker::SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool)
{   
    TReqAssignPeers request;
    ToProto(request.mutable_cell_id(), cell->GetId());

    const auto& peers = cell->Peers();
    for (int index = 0; index < static_cast<int>(peers.size()); ++index) {
        request.add_node_ids(InvalidNodeId);
    }

    TSmallSet<Stroka, TypicalCellSize> forbiddenAddresses;
    for (const auto& peer : cell->Peers()) {
        if (peer.Address) {
            forbiddenAddresses.insert(*peer.Address);
        }
    }

    bool assigned = false;
    for (int index = 0; index < static_cast<int>(peers.size()); ++index) {
        if (cell->Peers()[index].Address)
            continue;

        auto* node = pool->TryAllocate(cell, forbiddenAddresses);
        if (!node)
            break;

        request.set_node_ids(index, node->GetId());
        forbiddenAddresses.insert(node->GetAddress());
        assigned = true;
    }

    if (assigned) {
        auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
        CreateMutation(hydraManager, request)
            ->Commit();
    }
}

void TTabletTracker::SchedulePeerFailover(TTabletCell* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime + Config->PeerFailoverTimeout)
        return;

    const auto& cellId = cell->GetId();

    // Look for timed out peers.
    for (TPeerId peerId = 0; peerId < static_cast<int>(cell->Peers().size()); ++peerId) {
        if (IsFailoverNeeded(cell, peerId) && IsFailoverPossible(cell)) {
            TReqRevokePeer request;
            ToProto(request.mutable_cell_id(), cellId);
            request.set_peer_id(peerId);

            auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
            CreateMutation(hydraManager, request)
                ->Commit();
        }
    }
}

bool TTabletTracker::IsFailoverNeeded(TTabletCell* cell, TPeerId peerId)
{
    const auto& peer = cell->Peers()[peerId];
    if (!peer.Address)
        return false;

    if (peer.Node)
        return false;

    if (peer.LastSeenTime > TInstant::Now() - Config->PeerFailoverTimeout)
        return false;

    return true;
}

bool TTabletTracker::IsFailoverPossible(TTabletCell* cell)
{
    switch (cell->GetState()) {
        case ETabletCellState::Starting:
            // Failover is always safe when starting.
            return true;

        case ETabletCellState::Running: {
            // Must have at least quorum.
            if (cell->GetOnlinePeerCount() < (cell->GetSize() + 1) /2)
                return false;

            // Must have completed recovery.
            for (const auto& peer : cell->Peers()) {
                if (peer.Node) {
                    const auto* slot = peer.Node->GetTabletSlot(cell);
                    if (slot->PeerState != EPeerState::Leading && slot->PeerState != EPeerState::Following) {
                        return false;
                    }
                }
            }
            return true;
        }

        default:
            YUNREACHABLE();
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
