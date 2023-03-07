#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "tamed_cell_manager.h"
#include "cell_tracker_impl.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/node_tracker_server/config.h>
#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/table_server/table_node.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NHydra;
using namespace NHiveServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerProvider
    : public ICellBalancerProvider
{
public:
    explicit TCellBalancerProvider(const TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , BalanceRequestTime_(Now())
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
        bundleNodeTracker->SubscribeBundleNodesChanged(BIND(&TCellBalancerProvider::OnBundleNodesChanged, MakeWeak(this)));
    }

    virtual std::vector<TNodeHolder> GetNodes() override
    {
        BalanceRequestTime_.reset();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        auto isGood = [&] (const auto* node) {
            return CheckIfNodeCanHostCells(node) && node->GetTotalTabletSlots() > 0;
        };

        int nodeCount = 0;
        for (const auto [nodeId, node] : nodeTracker->Nodes()) {
            if (isGood(node)) {
                ++nodeCount;
            }
        }

        std::vector<TNodeHolder> nodes;
        nodes.reserve(nodeCount);

        for (const auto [nodeId, node] : nodeTracker->Nodes()) {
            if (!isGood(node)) {
                continue;
            }

            const auto* cells = cellManager->FindAssignedCells(node->GetDefaultAddress());
            nodes.emplace_back(
                node,
                node->GetTotalTabletSlots(),
                cells ? *cells : TCellSet());
        }

        return nodes;
    }

    virtual const TReadOnlyEntityMap<TCellBundle>& CellBundles() override
    {
        return Bootstrap_->GetTamedCellManager()->CellBundles();
    }

    virtual bool IsPossibleHost(const TNode* node, const TCellBundle* bundle) override
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
        return bundleNodeTracker->GetBundleNodes(bundle).contains(node);
    }

    virtual bool IsVerboseLoggingEnabled() override
    {
        return Bootstrap_->GetConfigManager()->GetConfig()
            ->TabletManager->TabletCellBalancer->EnableVerboseLogging;
    }

    virtual bool IsBalancingRequired() override
    {
        if (!GetConfig()->EnableTabletCellSmoothing) {
            return false;
        }

        auto waitTime = GetConfig()->RebalanceWaitTime;

        if (BalanceRequestTime_ && *BalanceRequestTime_ + waitTime < Now()) {
            BalanceRequestTime_.reset();
            return true;
        }

        return false;
    }

private:
    const TBootstrap* Bootstrap_;
    std::optional<TInstant> BalanceRequestTime_;

    void OnBundleNodesChanged(const TCellBundle* /*bundle*/)
    {
        if (!BalanceRequestTime_) {
            BalanceRequestTime_ = Now();
        }
    }

    const TDynamicTabletCellBalancerMasterConfigPtr& GetConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()
            ->TabletManager->TabletCellBalancer;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellTrackerImpl::TCellTrackerImpl(
    NCellMaster::TBootstrap* bootstrap,
    TInstant startTime)
    : Bootstrap_(bootstrap)
    , StartTime_(startTime)
    , TCellBalancerProvider_(New<TCellBalancerProvider>(Bootstrap_))
    , Profiler("/tablet_server/tablet_tracker")
{
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

    const auto& cellManager = Bootstrap_->GetTamedCellManager();
    cellManager->SubscribeCellPeersAssigned(BIND(&TCellTrackerImpl::OnCellPeersReassigned, MakeWeak(this)));
}

void TCellTrackerImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (WaitForCommit_) {
        return;
    }

    TBundleCounter leaderReassignmentCounter, peerRevocationCounter, peerAssignmentCounter;

    auto balancer = CreateCellBalancer(TCellBalancerProvider_);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    const auto& cellManger = Bootstrap_->GetTamedCellManager();

    TReqReassignPeers request;

    for (const auto [cellId, cell] : cellManger->Cells()) {
        if (!IsObjectAlive(cell)) {
            continue;
        }

        bool peerCountChanged = false;
        if (GetDynamicConfig()->DecommissionThroughExtraPeers) {
            peerCountChanged = SchedulePeerCountChange(cell, &request);
        }

        // NB: If peer count changes cells state is not valid.
        if (!peerCountChanged) {
            ScheduleLeaderReassignment(cell, &leaderReassignmentCounter);
            SchedulePeerAssignment(cell, balancer.get(), &peerAssignmentCounter);
            SchedulePeerRevocation(cell, balancer.get(), &peerRevocationCounter);
        }
    }

    auto moveDescriptors = balancer->GetCellMoveDescriptors();
    Profile(moveDescriptors, leaderReassignmentCounter, peerRevocationCounter, peerAssignmentCounter);

    {
        TReqRevokePeers* revocation;
        const TCellBase* cell = nullptr;

        for (const auto& moveDescriptor : moveDescriptors) {
            const auto* source = moveDescriptor.Source;
            const auto* target = moveDescriptor.Target;

            if (source || !target) {
                if (moveDescriptor.Cell != cell) {
                    cell = moveDescriptor.Cell;
                    revocation = request.add_revocations();
                    ToProto(revocation->mutable_cell_id(), cell->GetId());
                }

                if (!target && IsDecommissioned(source, cell->GetCellBundle())) {
                    continue;
                }

                revocation->add_peer_ids(moveDescriptor.PeerId);
                ToProto(revocation->mutable_reason(), moveDescriptor.Reason);
            }
        }
    }

    {
        TReqAssignPeers* assignment;
        const TCellBase* cell = nullptr;

        for (const auto& moveDescriptor : moveDescriptors) {
            if (moveDescriptor.Target) {
                if (moveDescriptor.Cell != cell) {
                    cell = moveDescriptor.Cell;
                    assignment = request.add_assignments();
                    ToProto(assignment->mutable_cell_id(), cell->GetId());
                }

                auto* peerInfo = assignment->add_peer_infos();
                peerInfo->set_peer_id(moveDescriptor.PeerId);
                ToProto(peerInfo->mutable_node_descriptor(), moveDescriptor.Target->GetDescriptor());
            }
        }
    }

    WaitForCommit_ = true;

    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TCellTrackerImpl::OnCellPeersReassigned()
{
    WaitForCommit_ = false;
}

const TDynamicCellManagerConfigPtr& TCellTrackerImpl::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
}

void TCellTrackerImpl::Profile(
    const std::vector<TCellMoveDescriptor>& moveDescriptors,
    const TBundleCounter& leaderReassignmentCounter,
    const TBundleCounter& peerRevocationCounter,
    const TBundleCounter& peerAssignmentCounter)
{
    TBundleCounter moveCounts;

    for (const auto& moveDescriptor : moveDescriptors) {
        moveCounts[NProfiling::TTagIdList{moveDescriptor.Cell->GetCellBundle()->GetProfilingTag()}]++;
    }

    for (const auto& [tags, count] : moveCounts) {
        Profiler.Enqueue(
            "/tablet_cell_moves",
            count,
            NProfiling::EMetricType::Gauge,
            tags);
    }

    for (const auto& [tags, count] : leaderReassignmentCounter) {
        Profiler.Enqueue(
            "/leader_reassignment",
            count,
            NProfiling::EMetricType::Gauge,
            tags);
    }

    for (const auto& [tags, count] : peerRevocationCounter) {
        Profiler.Enqueue(
            "/peer_revocation",
            count,
            NProfiling::EMetricType::Gauge,
            tags);
    }

    for (const auto& [tags, count] : peerAssignmentCounter) {
        Profiler.Enqueue(
            "/peer_assignment",
            count,
            NProfiling::EMetricType::Gauge,
            tags);
    }
}

void TCellTrackerImpl::ScheduleLeaderReassignment(TCellBase* cell, TBundleCounter* counter)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    TError error;

    if (!leadingPeer.Descriptor.IsNull()) {
        error = IsFailed(leadingPeer, cell->GetCellBundle(), GetDynamicConfig()->LeaderReassignmentTimeout);
        if (error.IsOK()) {
            return;
        }
    }

    if (error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) &&
        GetDynamicConfig()->DecommissionedLeaderReassignmentTimeout &&
            (cell->LastPeerCountUpdateTime() == TInstant{} ||
             cell->LastPeerCountUpdateTime() + *GetDynamicConfig()->DecommissionedLeaderReassignmentTimeout > TInstant::Now()))
    {
        return;
    }

    // Switching to good follower is always better than switching to non-follower.
    int newLeaderId = FindGoodFollower(cell);

    if (GetDynamicConfig()->DecommissionThroughExtraPeers) {
        // If node is decommissioned we switch only to followers, otherwise to any good peer.
        if (!error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) && newLeaderId == InvalidPeerId) {
            newLeaderId = FindGoodPeer(cell);
        }
    } else if (newLeaderId == InvalidPeerId) {
        newLeaderId = FindGoodPeer(cell);
    }

    if (newLeaderId == InvalidPeerId) {
        return;
    }

    YT_LOG_DEBUG(error, "Scheduling leader reassignment (CellId: %v, PeerId: %v, Address: %v)",
        cell->GetId(),
        cell->GetLeadingPeerId(),
        leadingPeer.Descriptor.GetDefaultAddress());

    TReqSetLeadingPeer request;
    ToProto(request.mutable_cell_id(), cell->GetId());
    request.set_peer_id(newLeaderId);

    NProfiling::TTagIdList tagIds{
        cell->GetCellBundle()->GetProfilingTag(),
        NProfiling::TProfileManager::Get()->RegisterTag("reason", error.GetMessage())
    };

    (*counter)[tagIds]++;

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TCellTrackerImpl::SchedulePeerAssignment(TCellBase* cell, ICellBalancer* balancer, TBundleCounter* counter)
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

        auto* slot = node->FindCellSlot(cell);
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

    int assignCount = 0;

    // Try to assign missing peers.
    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        if (peers[id].Descriptor.IsNull()) {
            ++assignCount;
            balancer->AssignPeer(cell, id);
        }
    }

    (*counter)[NProfiling::TTagIdList{cell->GetCellBundle()->GetProfilingTag()}] += assignCount;
}

void TCellTrackerImpl::SchedulePeerRevocation(
    TCellBase* cell,
    ICellBalancer* balancer,
    TBundleCounter* counter)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + GetDynamicConfig()->PeerRevocationTimeout) {
        return;
    }

    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        const auto& peer = cell->Peers()[peerId];
        if (peer.Descriptor.IsNull()) {
            continue;
        }

        auto error = IsFailed(peer, cell->GetCellBundle(), GetDynamicConfig()->PeerRevocationTimeout);
        if (!error.IsOK()) {
            if (GetDynamicConfig()->DecommissionThroughExtraPeers && error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned)) {
                // If decommission through extra peers is enabled we never revoke leader during decommission.
                if (peerId == cell->GetLeadingPeerId()) {
                    continue;
                }

                // Do not revoke old leader until decommission is finished.
                if (cell->PeerCount()) {
                    continue;
                }

                // Followers are decommssioned by simple revocation.
            }

            YT_LOG_DEBUG(error, "Scheduling peer revocation (CellId: %v, PeerId: %v, Address: %v)",
                cell->GetId(),
                peerId,
                peer.Descriptor.GetDefaultAddress());

            balancer->RevokePeer(cell, peerId, error);

            NProfiling::TTagIdList tagIds{
                cell->GetCellBundle()->GetProfilingTag(),
                NProfiling::TProfileManager::Get()->RegisterTag("reason", error.GetMessage())
            };

            (*counter)[tagIds]++;
        }
    }
}

bool TCellTrackerImpl::SchedulePeerCountChange(TCellBase* cell, TReqReassignPeers* request)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    bool leaderDecommissioned = leadingPeer.Node && leadingPeer.Node->GetDecommissioned();
    bool hasExtraPeers = cell->PeerCount().has_value();
    if (cell->Peers().size() == 1 && leaderDecommissioned && !hasExtraPeers) {
        // There are no followers and leader's node is decommissioned
        // so we need extra peer to perform decommission.
        auto* updatePeerCountRequest = request->add_peer_count_updates();
        ToProto(updatePeerCountRequest->mutable_cell_id(), cell->GetId());
        updatePeerCountRequest->set_peer_count(static_cast<int>(cell->Peers().size() + 1));
        return true;
    } else if (!leaderDecommissioned && leadingPeer.LastSeenState == EPeerState::Leading && hasExtraPeers) {
        // Decommission finished, extra peers can be dropped.
        auto* updatePeerCountRequest = request->add_peer_count_updates();
        ToProto(updatePeerCountRequest->mutable_cell_id(), cell->GetId());
        return true;
    }

    return false;
}

TError TCellTrackerImpl::IsFailed(
    const TCellBase::TPeer& peer,
    const TCellBundle* bundle,
    TDuration timeout)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    const auto* node = nodeTracker->FindNodeByAddress(peer.Descriptor.GetDefaultAddress());
    if (node) {
        if (node->GetBanned()) {
            return TError(
                NCellServer::EErrorCode::NodeBanned,
                "Node %v banned",
                node->GetDefaultAddress());
        }

        if (node->GetDecommissioned()) {
            return TError(
                NCellServer::EErrorCode::NodeDecommissioned,
                "Node %v decommissioned",
                node->GetDefaultAddress());
        }

        if (node->GetDisableTabletCells()) {
            return TError(
                NCellServer::EErrorCode::NodeTabletSlotsDisabled,
                "Node %v tablet slots disabled",
                node->GetDefaultAddress());
        }

        if (!bundle->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
            return TError(
                NCellServer::EErrorCode::NodeFilterMismatch,
                "Node %v does not satisfy tag filter of cell bundle %v",
                node->GetDefaultAddress(),
                bundle->GetId());
        }
    }

    if (peer.LastSeenTime + timeout > TInstant::Now()) {
        return TError();
    }

    if (peer.Node) {
        return TError();
    }

    return TError(
        NCellServer::EErrorCode::CellDidNotAppearWithinTimeout,
        "Node %v did not report appearance of cell within timeout",
        peer.Descriptor.GetDefaultAddress());
}

bool TCellTrackerImpl::IsDecommissioned(
    const TNode* node,
    const TCellBundle* bundle)
{
    if (!node) {
        return false;
    }

    if (node->GetBanned()) {
        return false;
    }

    if (!bundle->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
        return false;
    }

    if (node->GetDecommissioned()) {
        return true;
    }

    if (node->GetDisableTabletCells()) {
        return true;
    }

    return false;
}

TPeerId TCellTrackerImpl::FindGoodFollower(const TCellBase* cell)
{
    for (TPeerId peerId = 0; peerId < static_cast<TPeerId>(cell->Peers().size()); ++peerId) {
        const auto& peer = cell->Peers()[peerId];
        if (!CheckIfNodeCanHostCells(peer.Node)) {
            continue;
        }

        if (cell->GetPeerState(peerId) != EPeerState::Following) {
            continue;
        }

        auto* slot = cell->FindCellSlot(peerId);
        if (slot && !slot->IsResponseKeeperWarmingUp && slot->PreloadPendingStoreCount == 0 &&
            slot->PreloadFailedStoreCount == 0)
        {
            return peerId;
        }
    }

    return InvalidPeerId;
}

TPeerId TCellTrackerImpl::FindGoodPeer(const TCellBase* cell)
{
    for (TPeerId id = 0; id < static_cast<TPeerId>(cell->Peers().size()); ++id) {
        const auto& peer = cell->Peers()[id];
        if (CheckIfNodeCanHostCells(peer.Node)) {
            return id;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
