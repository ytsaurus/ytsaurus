#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"
#include "tablet_tracker_impl_old.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/node_tracker_server/config.h>
#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/table_server/table_node.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/small_set.h>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletTrackerImplOld::TCandidatePool
{
public:
    explicit TCandidatePool(const NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TNode* TryAllocate(
        TTabletCell* cell,
        const SmallSet<TString, TypicalPeerCount>& forbiddenAddresses)
    {
        LazyInitialization();

        auto* bundle = cell->GetCellBundle();
        for (const auto& pair : Queues_[bundle]) {
            int index = pair.second;
            auto& node = Nodes_[index];
            if (forbiddenAddresses.count(node.Node->GetDefaultAddress()) == 0) {
                ChargeNode(index, cell);
                return node.Node;
            }
        }
        return nullptr;
    }

private:
    // Key is pair (nubmer of slots assigned to this bunde, minus number of spare slots),
    // value is node index in Nodes_ array.
    using TQueueType = std::multimap<std::pair<int,int>, int>;

    struct TNodeData
    {
        TNode* Node;
        THashMap<TTabletCellBundle*, TQueueType::iterator> Iterators;
    };

    class THostilityChecker
    {
    public:
        explicit THostilityChecker(TNode* node)
            : Node_(node)
        { }

        bool IsPossibleHost(const TTabletCellBundle* bundle)
        {
            const auto& tagFilter = bundle->NodeTagFilter();
            auto formula = tagFilter.GetFormula();
            if (auto it = Cache_.find(formula)) {
                return it->second;
            }

            auto result = tagFilter.IsSatisfiedBy(Node_->Tags());
            YCHECK(Cache_.insert(std::make_pair(formula, result)).second);
            return result;
        }

    private:
        const TNode* Node_;
        THashMap<TString, bool> Cache_;
    };


    const NCellMaster::TBootstrap* const Bootstrap_;

    bool Initialized_ = false;
    std::vector<TNodeData> Nodes_;
    THashMap<TTabletCellBundle*, TQueueType> Queues_;


    void LazyInitialization()
    {
        if (Initialized_) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            Queues_.emplace(pair.second, TQueueType());
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            AddNode(pair.second);
        }

        Initialized_ = true;
    }

    void AddNode(TNode* node)
    {
        if (!CheckIfNodeCanHostTabletCells(node)) {
            return;
        }

        TNodeData data{node};
        int index = Nodes_.size();
        int spare = node->GetTotalTabletSlots();
        THashMap<TTabletCellBundle*, int> cellCount;

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        if (const auto* cells = tabletManager->FindAssignedTabletCells(node->GetDefaultAddress())) {
            for (auto& pair : *cells) {
                auto* cell = pair.first;
                auto bundle = cell->GetCellBundle();
                cellCount[bundle] += 1;
                --spare;
            }
        }
        if (spare <= 0) {
            return;
        }

        auto hostilityChecker = THostilityChecker(node);
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            auto* bundle = pair.second;
            if (!hostilityChecker.IsPossibleHost(bundle)) {
                continue;
            }

            int count = 0;
            auto it = cellCount.find(bundle);
            if (it != cellCount.end()) {
                count = it->second;
            }

            data.Iterators[bundle] = Queues_[bundle].insert(
                std::make_pair(std::make_pair(count, -spare), index));
        }

        Nodes_.push_back(std::move(data));
    }

    void ChargeNode(int index, TTabletCell* cell)
    {
        auto& node = Nodes_[index];
        SmallVector<TTabletCellBundle*, TypicalTabletSlotCount> remove;

        for (auto& pair : node.Iterators) {
            auto* bundle = pair.first;
            auto& it = pair.second;
            YCHECK(it->second == index);

            int count = it->first.first;
            int spare = -it->first.second - 1;
            if (bundle == cell->GetCellBundle()) {
                count += 1;
            }

            Queues_[bundle].erase(it);

            if (spare > 0) {
                it = Queues_[bundle].insert(std::make_pair(std::make_pair(count, -spare), index));
            } else {
                remove.push_back(bundle);
            }
        }

        for (auto* bundle : remove) {
            YCHECK(node.Iterators.erase(bundle));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletTrackerImplOld::TTabletTrackerImplOld(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap,
    TInstant startTime)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , StartTime_(startTime)
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
}

void TTabletTrackerImplOld::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

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

void TTabletTrackerImplOld::ScheduleLeaderReassignment(TTabletCell* cell, TCandidatePool* pool)
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

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TTabletTrackerImplOld::SchedulePeerAssignment(TTabletCell* cell, TCandidatePool* pool)
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

    SmallSet<TString, TypicalPeerCount> forbiddenAddresses;
    for (const auto& peer : peers) {
        if (!peer.Descriptor.IsNull()) {
            forbiddenAddresses.insert(peer.Descriptor.GetDefaultAddress());
        }
    }

    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
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

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TTabletTrackerImplOld::SchedulePeerRevocation(TTabletCell* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerRevocationTimeout)
        return;

    const auto& cellId = cell->GetId();

    TReqRevokePeers request;
    ToProto(request.mutable_cell_id(), cellId);
    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        if (IsFailed(cell, peerId, Config_->PeerRevocationTimeout)) {
            request.add_peer_ids(peerId);
        }
    }

    if (request.peer_ids_size() == 0)
        return;

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TTabletTrackerImplOld::IsFailed(const TTabletCell* cell, TPeerId peerId, TDuration timeout)
{
    const auto& peer = cell->Peers()[peerId];
    if (peer.Descriptor.IsNull()) {
        return false;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    const auto* node = nodeTracker->FindNodeByAddress(peer.Descriptor.GetDefaultAddress());
    if (node) {
        if (node->GetBanned()) {
            return true;
        }

        if (node->GetDecommissioned()) {
            return true;
        }

        if (node->GetDisableTabletCells()) {
            return true;
        }

        if (!cell->GetCellBundle()->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
            return true;
        }
    }

    if (peer.LastSeenTime + timeout > TInstant::Now()) {
        return false;
    }

    if (peer.Node) {
        return false;
    }

    return true;
}

int TTabletTrackerImplOld::FindGoodPeer(const TTabletCell* cell)
{
    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        const auto& peer = cell->Peers()[id];
        if (CheckIfNodeCanHostTabletCells(peer.Node)) {
            return id;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
