#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "tamed_cell_manager.h"
#include "cell_tracker_impl_old.h"

#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/node_tracker_server/config.h>
#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/table_server/table_node.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/small_set.h>

namespace NYT::NCellServer {

using namespace NConcurrency;
using namespace NTabletClient;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerImplOld::TCandidatePool
{
public:
    explicit TCandidatePool(const NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TNode* TryAllocate(
        TCellBase* cell,
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
        THashMap<TCellBundle*, TQueueType::iterator> Iterators;
    };

    class THostilityChecker
    {
    public:
        explicit THostilityChecker(TNode* node)
            : Node_(node)
        { }

        bool IsPossibleHost(const TCellBundle* bundle)
        {
            const auto& tagFilter = bundle->NodeTagFilter();
            auto formula = tagFilter.GetFormula();
            if (auto it = Cache_.find(formula)) {
                return it->second;
            }

            auto result = tagFilter.IsSatisfiedBy(Node_->Tags());
            YT_VERIFY(Cache_.insert(std::make_pair(formula, result)).second);
            return result;
        }

    private:
        const TNode* Node_;
        THashMap<TString, bool> Cache_;
    };


    const NCellMaster::TBootstrap* const Bootstrap_;

    bool Initialized_ = false;
    std::vector<TNodeData> Nodes_;
    THashMap<TCellBundle*, TQueueType> Queues_;


    void LazyInitialization()
    {
        if (Initialized_) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTamedCellManager();
        for (const auto [bundleId, bundle] : tabletManager->CellBundles()) {
            Queues_.emplace(bundle, TQueueType());
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto [nodeId, node] : nodeTracker->Nodes()) {
            AddNode(node);
        }

        Initialized_ = true;
    }

    void AddNode(TNode* node)
    {
        if (!CheckIfNodeCanHostCells(node)) {
            return;
        }

        TNodeData data{node};
        int index = Nodes_.size();
        int spare = node->GetTotalTabletSlots();
        THashMap<TCellBundle*, int> cellCount;

        const auto& tabletManager = Bootstrap_->GetTamedCellManager();
        if (const auto* cells = tabletManager->FindAssignedCells(node->GetDefaultAddress())) {
            for (auto [cell, peerId] : *cells) {
                auto bundle = cell->GetCellBundle();
                cellCount[bundle] += 1;
                --spare;
            }
        }
        if (spare <= 0) {
            return;
        }

        auto hostilityChecker = THostilityChecker(node);
        for (const auto& pair : tabletManager->CellBundles()) {
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

    void ChargeNode(int index, TCellBase* cell)
    {
        auto& node = Nodes_[index];
        SmallVector<TCellBundle*, TypicalTabletSlotCount> remove;

        for (auto& pair : node.Iterators) {
            auto* bundle = pair.first;
            auto& it = pair.second;
            YT_VERIFY(it->second == index);

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
            YT_VERIFY(node.Iterators.erase(bundle));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellTrackerImplOld::TCellTrackerImplOld(
    NCellMaster::TBootstrap* bootstrap,
    TInstant startTime)
    : Bootstrap_(bootstrap)
    , StartTime_(startTime)
{
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
}

void TCellTrackerImplOld::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TCandidatePool pool(Bootstrap_);

    auto tabletManger = Bootstrap_->GetTamedCellManager();
    for (const auto& pair : tabletManger->Cells()) {
        auto* cell = pair.second;
        if (!IsObjectAlive(cell))
            continue;

        ScheduleLeaderReassignment(cell, &pool);
        SchedulePeerAssignment(cell, &pool);
        SchedulePeerRevocation(cell);
    }
}

void TCellTrackerImplOld::ScheduleLeaderReassignment(TCellBase* cell, TCandidatePool* pool)
{
    // Try to move the leader to a good peer.
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];

    if (!leadingPeer.Descriptor.IsNull() &&
        !IsFailed(leadingPeer, cell->GetCellBundle()->NodeTagFilter(), GetDynamicConfig()->LeaderReassignmentTimeout))
    {
        return;
    }

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

const TDynamicCellManagerConfigPtr& TCellTrackerImplOld::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
}

void TCellTrackerImplOld::SchedulePeerAssignment(TCellBase* cell, TCandidatePool* pool)
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
        auto* slot = node->FindCellSlot(cell);
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

void TCellTrackerImplOld::SchedulePeerRevocation(TCellBase* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + GetDynamicConfig()->PeerRevocationTimeout)
        return;

    auto cellId = cell->GetId();

    TReqRevokePeers request;
    ToProto(request.mutable_cell_id(), cellId);
    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        const auto& peer = cell->Peers()[peerId];

        if (!peer.Descriptor.IsNull() &&
            IsFailed(peer, cell->GetCellBundle()->NodeTagFilter(), GetDynamicConfig()->PeerRevocationTimeout))
        {
            request.add_peer_ids(peerId);
        }
    }

    if (request.peer_ids_size() == 0)
        return;

    ToProto(request.mutable_reason(), TError("Revoked by old cell tracker"));

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TCellTrackerImplOld::IsFailed(
    const TCellBase::TPeer& peer,
    const TBooleanFormula& nodeTagFilter,
    TDuration timeout)
{
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

        if (!nodeTagFilter.IsSatisfiedBy(node->Tags())) {
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

int TCellTrackerImplOld::FindGoodPeer(const TCellBase* cell)
{
    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        const auto& peer = cell->Peers()[id];
        if (CheckIfNodeCanHostCells(peer.Node)) {
            return id;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
