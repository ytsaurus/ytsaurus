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

static const auto CellsScanPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TCandidatePool
{
public:
    explicit TCandidatePool(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto* node : nodeTracker->Nodes().GetValues()) {
            if (HasAvailableSlots(node)) {
                YCHECK(Candidates_.insert(node).second);
            }
        }
    }

    TNode* TryAllocate(
        const TNullable<Stroka>& preferredAddress,
        const TSmallSet<Stroka, TypicalCellSize>& forbiddenAddresses)
    {
        // Try to allocate preferred.
        if (preferredAddress && forbiddenAddresses.count(*preferredAddress) == 0) {
            auto nodeTracker = Bootstrap_->GetNodeTracker();
            auto* node = nodeTracker->FindNodeByAddress(*preferredAddress);
            if (node && HasAvailableSlots(node)) {
                AllocateSlot(node);
                return node;
            }
        }

        // Try to allocate from candidates.
        for (auto* node : Candidates_) {
            if (forbiddenAddresses.count(node->GetAddress()) == 0) {
                AllocateSlot(node);
                return node;
            }
        }

        return nullptr;
    }

private:
    NCellMaster::TBootstrap* Bootstrap_;

    yhash_set<TNode*> Candidates_;


    static bool HasAvailableSlots(TNode* node)
    {
        return node->GetTotalUsedTabletSlots() < node->GetTotalTabletSlots();
    }

    void AllocateSlot(TNode* node)
    {
        node->AddTabletSlotHint();
        if (!HasAvailableSlots(node)) {
            YCHECK(Candidates_.erase(node) == 1);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TTabletTracker::TTabletTracker(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetMetaStateFacade()->GetInvoker(), AutomatonThread);
}

void TTabletTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    YCHECK(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetMetaStateFacade()->GetEpochInvoker(),
        BIND(&TTabletTracker::ScanCells, MakeWeak(this)),
        CellsScanPeriod);
    PeriodicExecutor_->Start();
}

void TTabletTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }
}

void TTabletTracker::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TCandidatePool pool(Bootstrap_);

    auto tabletManger = Bootstrap_->GetTabletManager();
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

    auto hydraManager = Bootstrap_->GetMetaStateFacade()->GetManager();
    CreateMutation(hydraManager, request)
        ->Commit();
}

void TTabletTracker::SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool)
{   
    TReqAssignPeers request;
    ToProto(request.mutable_cell_id(), cell->GetId());

    const auto& peers = cell->Peers();
    for (int peerId = 0; peerId < static_cast<int>(peers.size()); ++peerId) {
        request.add_node_ids(InvalidNodeId);
    }

    TSmallSet<Stroka, TypicalCellSize> forbiddenAddresses;
    for (const auto& peer : cell->Peers()) {
        if (peer.Node) {
            forbiddenAddresses.insert(*peer.Address);
        }
    }

    bool assigned = false;
    for (int peerId = 0; peerId < static_cast<int>(peers.size()); ++peerId) {
        const auto& peer = cell->Peers()[peerId];
        if (peer.Node) 
            continue;

        auto* node = pool->TryAllocate(peer.Address, forbiddenAddresses);
        if (!node)
            break; // all subsequent allocations will fail anyway

        request.set_node_ids(peerId, node->GetId());
        forbiddenAddresses.insert(node->GetAddress());
        assigned = true;
    }

    if (assigned) {
        auto hydraManager = Bootstrap_->GetMetaStateFacade()->GetManager();
        CreateMutation(hydraManager, request)
            ->Commit();
    }
}

void TTabletTracker::SchedulePeerFailover(TTabletCell* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerFailoverTimeout)
        return;

    const auto& cellId = cell->GetId();

    // Look for timed out peers.
    for (TPeerId peerId = 0; peerId < static_cast<int>(cell->Peers().size()); ++peerId) {
        if (IsFailoverNeeded(cell, peerId)) {
            TReqRevokePeer request;
            ToProto(request.mutable_cell_id(), cellId);
            request.set_peer_id(peerId);

            auto hydraManager = Bootstrap_->GetMetaStateFacade()->GetManager();
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

    if (peer.LastSeenTime > TInstant::Now() - Config_->PeerFailoverTimeout)
        return false;

    return true;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
