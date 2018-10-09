#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"
#include "tablet_tracker_impl.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/node_tracker_server/config.h>
#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/table_server/table_node.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NHydra;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBalancerProvider
    : public ITabletCellBalancerProvider
{
public:
    explicit TTabletCellBalancerProvider(const TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTabletManager()->GetBundleNodeTracker();
        bundleNodeTracker->SubscribeBundleNodesChanged(BIND(&TTabletCellBalancerProvider::OnBundleNodesChanged, MakeWeak(this)));
    }

    virtual std::vector<TNodeHolder> GetNodes() override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto isGood = [&] (const auto* node) {
            return CheckIfNodeCanHostTabletCells(node) && node->GetTotalTabletSlots() > 0;
        };

        int nodeCount = 0;
        for (const auto& pair : nodeTracker->Nodes()) {
            if (isGood(pair.second)) {
                ++nodeCount;
            }
        }

        std::vector<TNodeHolder> nodes;
        nodes.reserve(nodeCount);

        for (const auto& pair : nodeTracker->Nodes()) {
            const auto* node = pair.second;
            if (!isGood(node)) {
                continue;
            }

            const auto* cells = tabletManager->FindAssignedTabletCells(node->GetDefaultAddress());
            nodes.emplace_back(
                node,
                node->GetTotalTabletSlots(),
                cells ? *cells : TTabletCellSet());
        }

        return nodes;
    }

    virtual const TReadOnlyEntityMap<TTabletCellBundle>& TabletCellBundles() override
    {
        return Bootstrap_->GetTabletManager()->TabletCellBundles();
    }

    virtual bool IsPossibleHost(const TNode* node, const TTabletCellBundle* bundle) override
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTabletManager()->GetBundleNodeTracker();
        return bundleNodeTracker->GetBundleNodes(bundle).has(node);
    }

    virtual bool IsVerboseLoggingEnabled() override
    {
        return Bootstrap_->GetConfigManager()->GetConfig()
            ->TabletManager->TabletCellBalancer->EnableVerboseLogging;
    }

    virtual bool IsBalancingRequired() override
    {
        bool result = BalancingRequired_;
        BalancingRequired_ = false;
        return result;
    }

private:
    const TBootstrap* Bootstrap_;
    bool BalancingRequired_ = true;

    void OnBundleNodesChanged(const TTabletCellBundle* /*bundle*/)
    {
        BalancingRequired_ = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletTrackerImpl::TTabletTrackerImpl(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap,
    TInstant startTime)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , StartTime_(startTime)
    , TTabletCellBalancerProvider_(New<TTabletCellBalancerProvider>(Bootstrap_))
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
}

void TTabletTrackerImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto balancer = CreateTabletCellBalancer(TTabletCellBalancerProvider_);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    const auto& tabletManger = Bootstrap_->GetTabletManager();
    for (const auto& pair : tabletManger->TabletCells()) {
        auto* cell = pair.second;
        if (!IsObjectAlive(cell))
            continue;

        ScheduleLeaderReassignment(cell);
        SchedulePeerAssignment(cell, balancer.get());
        SchedulePeerRevocation(cell, balancer.get());
    }

    auto moveDescriptors = balancer->GetTabletCellMoveDescriptors();

    {
        TReqRevokePeers request;
        const TTabletCell* requestCell = nullptr;
        auto commit = [&] (const TTabletCell* cell) {
            if (cell != requestCell) {
                if (requestCell) {
                    CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger);
                }
                request = TReqRevokePeers();
                requestCell = cell;
                if (cell) {
                    ToProto(request.mutable_cell_id(), cell->GetId());
                }
            }
        };

        for (const auto& moveDescriptor : moveDescriptors) {
            if (moveDescriptor.Source || !moveDescriptor.Target) {
                commit(moveDescriptor.Cell);
                request.add_peer_ids(moveDescriptor.PeerId);
            }
        }

        commit(nullptr);
    }

    {
        TReqAssignPeers request;
        const TTabletCell* requestCell = nullptr;
        auto commit = [&] (const TTabletCell* cell) {
            if (cell != requestCell) {
                if (requestCell) {
                    CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger);
                }
                request = TReqAssignPeers();
                requestCell = cell;
                if (cell) {
                    ToProto(request.mutable_cell_id(), cell->GetId());
                }
            }
        };

        for (const auto& moveDescriptor : moveDescriptors) {
            if (moveDescriptor.Target) {
                commit(moveDescriptor.Cell);
                auto* peerInfo = request.add_peer_infos();
                peerInfo->set_peer_id(moveDescriptor.PeerId);
                ToProto(peerInfo->mutable_node_descriptor(), moveDescriptor.Target->GetDescriptor());
            }
        }

        commit(nullptr);
    }
}

void TTabletTrackerImpl::ScheduleLeaderReassignment(TTabletCell* cell)
{
    // Try to move the leader to a good peer.
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];

    if (!leadingPeer.Descriptor.IsNull() &&
        !IsFailed(leadingPeer, cell->GetCellBundle()->NodeTagFilter(), Config_->LeaderReassignmentTimeout))
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

void TTabletTrackerImpl::SchedulePeerAssignment(TTabletCell* cell, ITabletCellBalancer* balancer)
{
    const auto& peers = cell->Peers();

    // Don't assign new peers if there's a follower but no leader.
    // Try to promote the follower first.
    bool hasFollower = false;
    bool hasLeader = false;
    for (const auto& peer : peers) {
        auto* node = peer.Node;
        if (!node) {
            continue;
        }

        auto* slot = node->FindTabletSlot(cell);
        if (!slot) {
            continue;
        }

        auto state = slot->PeerState;
        if (state == EPeerState::Leading || state == EPeerState::LeaderRecovery) {
            hasLeader = true;
        }
        if (state == EPeerState::Following || state == EPeerState::FollowerRecovery) {
            hasFollower = true;
        }
    }

    if (hasFollower && !hasLeader) {
        return;
    }

    // Try to assign missing peers.
    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        if (peers[id].Descriptor.IsNull()) {
            balancer->AssignPeer(cell, id);
        }
    }
}

void TTabletTrackerImpl::SchedulePeerRevocation(TTabletCell* cell, ITabletCellBalancer* balancer)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerRevocationTimeout) {
        return;
    }

    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        const auto& peer = cell->Peers()[peerId];

        if (!peer.Descriptor.IsNull() &&
            IsFailed(peer, cell->GetCellBundle()->NodeTagFilter(), Config_->PeerRevocationTimeout))
        {
            balancer->RevokePeer(cell, peerId);
        }
    }
}

bool TTabletTrackerImpl::IsFailed(
    const TTabletCell::TPeer& peer,
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

int TTabletTrackerImpl::FindGoodPeer(const TTabletCell* cell)
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
