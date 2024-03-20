#include "area.h"
#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "tamed_cell_manager.h"
#include "cell_tracker_impl.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

#include <yt/yt/server/master/node_tracker_server/config.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NCellServer {

using namespace NCellarClient;
using namespace NCellBalancerClient::NProto;
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
    TCellBalancerProvider(TBootstrap* bootstrap, ECellarType cellarType)
        : Bootstrap_(bootstrap)
        , CellarType_(cellarType)
        , BalanceRequestTime_(Now())
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
        bundleNodeTracker->SubscribeAreaNodesChanged(BIND(&TCellBalancerProvider::OnAreaNodesChanged, MakeWeak(this)));
    }

    std::vector<TNodeHolder> GetNodes() override
    {
        BalanceRequestTime_.reset();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& cellManager = Bootstrap_->GetTamedCellManager();

        auto isGood = [&] (const auto* node) {
            return node->GetCellarSize(CellarType_) > 0 && CheckIfNodeCanHostCells(node);
        };

        int nodeCount = 0;
        for (auto [nodeId, node] : nodeTracker->Nodes()) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            if (isGood(node)) {
                ++nodeCount;
            }
        }

        std::vector<TNodeHolder> nodes;
        nodes.reserve(nodeCount);
        for (auto [nodeId, node] : nodeTracker->Nodes()) {
            if (!IsObjectAlive(node)) {
                continue;
            }

            if (!isGood(node)) {
                continue;
            }

            const auto* cells = cellManager->FindAssignedCells(node->GetDefaultAddress());
            nodes.emplace_back(
                node,
                node->GetCellarSize(CellarType_),
                cells ? *cells : TCellSet());
        }

        return nodes;
    }

    const TReadOnlyEntityMap<TCellBundle>& CellBundles() override
    {
        return Bootstrap_->GetTamedCellManager()->CellBundles();
    }

    bool IsPossibleHost(const TNode* node, const TArea* area) override
    {
        const auto& bundleNodeTracker = Bootstrap_->GetTamedCellManager()->GetBundleNodeTracker();
        return bundleNodeTracker->GetAreaNodes(area).contains(node);
    }

    bool IsVerboseLoggingEnabled() override
    {
        return Bootstrap_->GetConfigManager()->GetConfig()
            ->TabletManager->TabletCellBalancer->EnableVerboseLogging;
    }

    bool IsBalancingRequired() override
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
    TBootstrap* const Bootstrap_;
    const ECellarType CellarType_;

    std::optional<TInstant> BalanceRequestTime_;

    void OnAreaNodesChanged(const TArea* /*area*/)
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
{
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

    for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
        PerCellarProviders_[cellarType] = New<TCellBalancerProvider>(Bootstrap_, cellarType);
    }

    const auto& cellManager = Bootstrap_->GetTamedCellManager();
    cellManager->SubscribeCellPeersAssigned(BIND(&TCellTrackerImpl::OnCellPeersReassigned, MakeWeak(this)));
}

void TCellTrackerImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (WaitForCommit_) {
        return;
    }

    const auto& config = Bootstrap_->GetConfigManager()->GetConfig();

    if (!config->TabletManager->EnableCellTracker) {
        YT_LOG_DEBUG("Cell tracker is disabled; skipping iteration");
        return;
    }

    for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
        ScanCellarCells(cellarType);
    }
}

void TCellTrackerImpl::ScanCellarCells(ECellarType cellarType)
{
    auto balancer = CreateCellBalancer(PerCellarProviders_[cellarType]);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    const auto& cellManger = Bootstrap_->GetTamedCellManager();

    TReqReassignPeers request;

    // List of the cells that are ready to reassign leader together with new leading peer id.
    std::vector<std::pair<TCellBase*, TPeerId>> leaderReassignments;
    // Set of the bundles that contain cell that are going to reassign leader but leader reassignment
    // is not ready yet. For such bundles we do not execute any leader reassignments to synchronize
    // reassignment downtimes of different cells.
    THashSet<TCellBundle*> bundlesWithUnreadyLeaderReassignments;
    for (auto* cell : cellManger->Cells(cellarType)) {
        if (!IsObjectAlive(cell)) {
            continue;
        }

        if (cellarType == ECellarType::Tablet && GetDynamicConfig()->DecommissionThroughExtraPeers) {
            if (SchedulePeerCountChange(cell, &request)) {
                // NB: If peer count changes cells state is not valid.
                continue;
            }
        }

        if (!cell->CellBundle()->GetOptions()->IndependentPeers) {
            if (auto peerId = FindNewLeadingPeerId(cell); peerId != InvalidPeerId) {
                leaderReassignments.emplace_back(cell, peerId);
            } else if (IsLeaderReassignmentRequired(cell)) {
                // Leader reassignment is required not is not ready.
                bundlesWithUnreadyLeaderReassignments.insert(cell->CellBundle().Get());
            }
        }
        SchedulePeerAssignment(cell, balancer.get());
        SchedulePeerRevocation(cell, balancer.get());
    }

    for (auto [cell, peerId] : leaderReassignments) {
        // NB: IsLeaderReassignmentRequired check is essential here to distinguish between
        // extra peer decommission and multipeer cell leader switch. We don't want to delay
        // reassignments of the second kind.
        if (GetDynamicConfig()->SynchronizeTabletCellLeaderSwitches &&
            IsLeaderReassignmentRequired(cell) &&
            bundlesWithUnreadyLeaderReassignments.contains(cell->CellBundle().Get()))
        {
            continue;
        }

        ScheduleLeaderReassignment(cell, peerId);
    }

    auto moveDescriptors = balancer->GetCellMoveDescriptors();
    Profile(moveDescriptors);

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

                if (!target && IsDecommissioned(source, cell)) {
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

const TDynamicTabletManagerConfigPtr& TCellTrackerImpl::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
}

void TCellTrackerImpl::Profile(const std::vector<TCellMoveDescriptor>& moveDescriptors)
{
    for (const auto& moveDescriptor : moveDescriptors) {
        moveDescriptor.Cell->CellBundle()
            ->ProfilingCounters()
            .TabletCellMoves.Increment();
    }
}

bool TCellTrackerImpl::IsLeaderReassignmentRequired(TCellBase* cell)
{
    if (!GetDynamicConfig()->DecommissionThroughExtraPeers) {
        return false;
    }

    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    if (leadingPeer.Descriptor.IsNull()) {
        return false;
    }

    auto error = IsFailed(leadingPeer, cell, GetDynamicConfig()->LeaderReassignmentTimeout);
    return static_cast<bool>(error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned));
}

TPeerId TCellTrackerImpl::FindNewLeadingPeerId(TCellBase* cell)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    TError error;

    if (!leadingPeer.Descriptor.IsNull()) {
        error = IsFailed(leadingPeer, cell, GetDynamicConfig()->LeaderReassignmentTimeout);
        if (error.IsOK()) {
            return InvalidPeerId;
        }
    }

    if (error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) &&
        GetDynamicConfig()->DecommissionedLeaderReassignmentTimeout &&
            (cell->LastPeerCountUpdateTime() == TInstant{} ||
             cell->LastPeerCountUpdateTime() + *GetDynamicConfig()->DecommissionedLeaderReassignmentTimeout > TInstant::Now()))
    {
        return InvalidPeerId;
    }

    // Switching to good follower is always better than switching to non-follower.
    int newLeadingPeerId = FindGoodFollower(cell);

    if (GetDynamicConfig()->DecommissionThroughExtraPeers) {
        // If node is decommissioned we switch only to followers, otherwise to any good peer.
        if (!error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) && newLeadingPeerId == InvalidPeerId) {
            newLeadingPeerId = FindGoodPeer(cell);
        }
    } else if (newLeadingPeerId == InvalidPeerId) {
        newLeadingPeerId = FindGoodPeer(cell);
    }

    if (newLeadingPeerId != InvalidPeerId && newLeadingPeerId != cell->GetLeadingPeerId()) {
        return newLeadingPeerId;
    } else {
        return InvalidPeerId;
    }
}

void TCellTrackerImpl::ScheduleLeaderReassignment(TCellBase* cell, TPeerId newLeadingPeerId)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    TError error;

    if (!leadingPeer.Descriptor.IsNull()) {
        error = IsFailed(leadingPeer, cell, GetDynamicConfig()->LeaderReassignmentTimeout);
    }

    YT_LOG_DEBUG(error, "Scheduling leader reassignment (CellId: %v, LeaderPeerId: %v, NewLeadingPeerId: %v, Address: %v)",
        cell->GetId(),
        cell->GetLeadingPeerId(),
        newLeadingPeerId,
        leadingPeer.Descriptor.GetDefaultAddress());

    TReqSetLeadingPeer request;
    ToProto(request.mutable_cell_id(), cell->GetId());
    request.set_peer_id(newLeadingPeerId);

    cell->CellBundle()
        ->ProfilingCounters()
        .GetLeaderReassignment(error.GetCode())
        .Increment();

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

void TCellTrackerImpl::SchedulePeerAssignment(TCellBase* cell, ICellBalancer* balancer)
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
    for (TPeerId peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (cell->IsAlienPeer(peerId)) {
            continue;
        }

        if (peers[peerId].Descriptor.IsNull()) {
            ++assignCount;
            balancer->AssignPeer(cell, peerId);
        }
    }

    cell->CellBundle()
        ->ProfilingCounters()
        .PeerAssignment
        .Increment(assignCount);
}

void TCellTrackerImpl::SchedulePeerRevocation(
    TCellBase* cell,
    ICellBalancer* balancer)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + GetDynamicConfig()->PeerRevocationTimeout) {
        return;
    }

    for (TPeerId peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (cell->IsAlienPeer(peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (peer.Descriptor.IsNull()) {
            continue;
        }

        auto error = IsFailed(peer, cell, GetDynamicConfig()->PeerRevocationTimeout);
        if (!error.IsOK()) {
            if (GetDynamicConfig()->DecommissionThroughExtraPeers && error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned)) {
                // If decommission through extra peers is enabled we never revoke leader during decommission.
                if (peerId == cell->GetLeadingPeerId()) {
                    continue;
                }

                // Do not revoke old leader until decommission is finished.
                if (cell->PeerCount() && peerId == 0) {
                    continue;
                }

                // Followers are decommssioned by simple revocation.
            }

            YT_LOG_DEBUG(error, "Scheduling peer revocation (CellId: %v, PeerId: %v, Address: %v)",
                cell->GetId(),
                peerId,
                peer.Descriptor.GetDefaultAddress());

            balancer->RevokePeer(cell, peerId, error);

            cell->CellBundle()
                ->ProfilingCounters()
                .GetPeerRevocation(error.GetCode())
                .Increment();
        }
    }
}

bool TCellTrackerImpl::SchedulePeerCountChange(TCellBase* cell, TReqReassignPeers* request)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    bool leaderDecommissioned = leadingPeer.Node && leadingPeer.Node->IsDecommissioned();
    bool hasExtraPeers = cell->PeerCount().has_value();
    if (cell->Peers().size() == 1 && leaderDecommissioned && !hasExtraPeers) {
        // There are no followers and leader's node is decommissioned
        // so we need extra peer to perform decommission.
        auto* updatePeerCountRequest = request->add_peer_count_updates();
        ToProto(updatePeerCountRequest->mutable_cell_id(), cell->GetId());
        updatePeerCountRequest->set_peer_count(static_cast<int>(cell->Peers().size() + 1));
        return true;
    } else if ((!leaderDecommissioned || cell->GetLeadingPeerId() != 0) && leadingPeer.LastSeenState == EPeerState::Leading && hasExtraPeers) {
        // Wait for a proper amount of time before dropping an extra peer.
        // This enables for a truly zero-downtime failover from a former leader to the new one, at least in certain cases.
        if (TInstant::Now() < cell->LastLeaderChangeTime() + GetDynamicConfig()->ExtraPeerDropDelay) {
            return false;
        }

        // Decommission finished, extra peers can be dropped.
        // If a new leader became decommissioned, we still make him a single peer
        // and multipeer decommission will run again.
        auto* updatePeerCountRequest = request->add_peer_count_updates();
        ToProto(updatePeerCountRequest->mutable_cell_id(), cell->GetId());
        return true;
    }

    return false;
}

TError TCellTrackerImpl::IsFailed(
    const TCellBase::TPeer& peer,
    const TCellBase* cell,
    TDuration timeout)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    const auto* node = nodeTracker->FindNodeByAddress(peer.Descriptor.GetDefaultAddress());

    if (node) {
        if (!peer.Node && peer.LastSeenTime + timeout < TInstant::Now()) {
            return TError(
                NCellServer::EErrorCode::CellDidNotAppearWithinTimeout,
                "Node %v did not report appearance of cell within timeout",
                peer.Descriptor.GetDefaultAddress());
        }

        if (node->IsBanned()) {
            return TError(
                NCellServer::EErrorCode::NodeBanned,
                "Node %v banned",
                node->GetDefaultAddress());
        }

        if (node->IsDecommissioned()) {
            return TError(
                NCellServer::EErrorCode::NodeDecommissioned,
                "Node %v decommissioned",
                node->GetDefaultAddress());
        }

        if (node->AreTabletCellsDisabled()) {
            return TError(
                NCellServer::EErrorCode::NodeTabletSlotsDisabled,
                "Node %v tablet slots disabled",
                node->GetDefaultAddress());
        }

        if (!cell->GetArea()->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
            return TError(
                NCellServer::EErrorCode::NodeFilterMismatch,
                "Node %v does not satisfy tag filter of cell bundle %Qv area %Qv",
                node->GetDefaultAddress(),
                cell->GetArea()->GetCellBundle()->GetName(),
                cell->GetArea()->GetName());
        }
    }

    return TError();
}

bool TCellTrackerImpl::IsDecommissioned(
    const TNode* node,
    const TCellBase* cell)
{
    if (!node) {
        return false;
    }

    if (node->IsBanned()) {
        return false;
    }

    if (!cell->GetArea()->NodeTagFilter().IsSatisfiedBy(node->Tags())) {
        return false;
    }

    if (node->IsDecommissioned()) {
        return true;
    }

    if (node->AreTabletCellsDisabled()) {
        return true;
    }

    return false;
}

TPeerId TCellTrackerImpl::FindGoodFollower(const TCellBase* cell)
{
    for (TPeerId peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (cell->IsAlienPeer(peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (!CheckIfNodeCanHostCells(peer.Node)) {
            continue;
        }

        if (cell->GetPeerState(peerId) != EPeerState::Following) {
            continue;
        }

        auto* slot = cell->FindCellSlot(peerId);
        if (slot && slot->PreloadPendingStoreCount == 0 &&
            slot->PreloadFailedStoreCount == 0)
        {
            return peerId;
        }
    }

    return InvalidPeerId;
}

TPeerId TCellTrackerImpl::FindGoodPeer(const TCellBase* cell)
{
    for (TPeerId peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (cell->IsAlienPeer(peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (CheckIfNodeCanHostCells(peer.Node)) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
