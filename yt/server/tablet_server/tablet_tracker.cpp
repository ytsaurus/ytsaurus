#include "tablet_tracker.h"
#include "private.h"
#include "config.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/node_tracker_server/config.h>
#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/table_server/table_node.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NObjectServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NHydra;
using namespace NHive;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TCandidatePool
{
public:
    explicit TCandidatePool(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : nodeTracker->Nodes()) {
            auto* node = pair.second;
            if (!IsGood(node)) {
                continue;
            }
            int total = node->GetTotalTabletSlots();
            int used = tabletManager->GetAssignedTabletCellCount(node->GetDefaultAddress());
            int spare = total - used;
            if (used < total) {
                MinusSpareSlotsToNode_.insert(std::make_pair(-spare, node));
            }
        }
    }

    TNode* TryAllocate(
        TTabletCell* cell,
        const SmallSet<Stroka, TypicalCellSize>& forbiddenAddresses)
    {
        for (auto it = MinusSpareSlotsToNode_.begin(); it != MinusSpareSlotsToNode_.end(); ++it) {
            int spare = it->first;
            auto* node = it->second;
            if (forbiddenAddresses.count(node->GetDefaultAddress()) == 0) {
                MinusSpareSlotsToNode_.erase(it);
                --spare;
                if (spare > 0) {
                    MinusSpareSlotsToNode_.insert(std::make_pair(-spare, node));
                }
                return node;
            }
        }
        return nullptr;
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    // NB: "Minus" is to avoid iterating backwards and converting reserve iterator to forward iterator
    // in call to erase.
    std::multimap<int, TNode*> MinusSpareSlotsToNode_;

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
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(), AutomatonThread);
}

void TTabletTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    YCHECK(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TTabletTracker::ScanCells, MakeWeak(this)),
        Config_->CellScanPeriod);
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

bool TTabletTracker::IsEnabled()
{
    // This method also logs state changes.

    auto nodeTracker = Bootstrap_->GetNodeTracker();

    int needOnline = Config_->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();

    if (gotOnline < needOnline) {
        if (!LastEnabled_ || *LastEnabled_) {
            LOG_INFO("Tablet tracker disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
            LastEnabled_ = false;
        }
        return false;
    }

    if (!LastEnabled_ || !*LastEnabled_) {
        LOG_INFO("Tablet tracker enabled");
        LastEnabled_ = true;
    }

    return true;
}

void TTabletTracker::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsEnabled())
        return;

    TCandidatePool pool(Bootstrap_);

    auto tabletManger = Bootstrap_->GetTabletManager();
    for (const auto& pair : tabletManger->TabletCells()) {
        auto* cell = pair.second;
        if (!IsObjectAlive(cell))
            continue;

        ScheduleLeaderReassignment(cell, &pool);
        SchedulePeerAssignment(cell, &pool);
        SchedulePeerRevocation(cell);
    }
}

void TTabletTracker::ScheduleLeaderReassignment(TTabletCell* cell, TCandidatePool* pool)
{
    // Try to move the leader to a good peer.
    if (!IsFailed(cell, cell->GetLeadingPeerId(), Config_->LeaderReassignmentTimeout))
        return;

    auto goodPeerId = FindGoodPeer(cell);
    if (goodPeerId == InvalidPeerId)
        return;

    TReqSetLeadingPeer request;
    ToProto(request.mutable_cell_id(), cell->GetId());
    request.set_peer_id(goodPeerId);

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TTabletTracker::SchedulePeerAssignment(TTabletCell* cell, TCandidatePool* pool)
{
    const auto& peers = cell->Peers();

    // Don't assign new peers if there's a follower but no leader.
    // Try to promote the follower first.
    bool hasFollower = false;
    bool hasLeader = false;
    for (const auto& peer : peers) {
        auto* node = peer.Node;
        if (!node)
            continue;
        auto* slot = node->FindTabletSlot(cell);
        if (!slot)
            continue;

        auto state = slot->PeerState;
        if (state == EPeerState::Leading || state == EPeerState::LeaderRecovery) {
            hasLeader = true;
        }
        if (state == EPeerState::Following || state == EPeerState::FollowerRecovery) {
            hasFollower = true;
        }
    }

    if (hasFollower && !hasLeader)
        return;

    // Try to assign missing peers.
    TReqAssignPeers request;
    ToProto(request.mutable_cell_id(), cell->GetId());

    SmallSet<Stroka, TypicalCellSize> forbiddenAddresses;
    for (const auto& peer : peers) {
        if (!peer.Descriptor.IsNull()) {
            forbiddenAddresses.insert(peer.Descriptor.GetDefaultAddress());
        }
    }

    for (TPeerId id = 0; id < cell->GetPeerCount(); ++id) {
        if (!peers[id].Descriptor.IsNull())
            continue;

        auto* node = pool->TryAllocate(cell, forbiddenAddresses);
        if (!node)
            break;

        auto* peerInfo = request.add_peer_infos();
        peerInfo->set_peer_id(id);
        ToProto(peerInfo->mutable_node_descriptor(), node->GetDescriptor());

        forbiddenAddresses.insert(node->GetDefaultAddress());
    }

    if (request.peer_infos_size() == 0)
        return;

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TTabletTracker::SchedulePeerRevocation(TTabletCell* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerRevocationTimeout)
        return;

    const auto& cellId = cell->GetId();

    // Look for timed out peers.
    TReqRevokePeers request;
    ToProto(request.mutable_cell_id(), cellId);
    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        if (IsFailed(cell, peerId, Config_->PeerRevocationTimeout)) {
            request.add_peer_ids(peerId);
        }
    }

    if (request.peer_ids_size() == 0)
        return;

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TTabletTracker::IsFailed(const TTabletCell* cell, TPeerId peerId, TDuration timeout)
{
    const auto& peer = cell->Peers()[peerId];
    if (peer.Descriptor.IsNull()) {
        return false;
    }

    if (peer.Node) {
        return false;
    }

    auto nodeTracker = Bootstrap_->GetNodeTracker();
    const auto* node = nodeTracker->FindNodeByAddress(peer.Descriptor.GetDefaultAddress());
    if (node && (node->GetBanned() || node->GetDecommissioned())) {
        return true;
    }

    if (peer.LastSeenTime > TInstant::Now() - timeout) {
        return false;
    }

    return true;
}

bool TTabletTracker::IsGood(const TNode* node)
{
    if (!IsObjectAlive(node)) {
        return false;
    }

    if (node->GetAggregatedState() != ENodeState::Online) {
        return false;
    }

    if (node->GetBanned()) {
        return false;
    }

    if (node->GetDecommissioned()) {
        return false;
    }

    return true;
}

int TTabletTracker::FindGoodPeer(const TTabletCell* cell)
{
    for (TPeerId id = 0; id < cell->GetPeerCount(); ++id) {
        const auto& peer = cell->Peers()[id];
        if (IsGood(peer.Node)) {
            return id;
        }
    }
    return InvalidPeerId;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
