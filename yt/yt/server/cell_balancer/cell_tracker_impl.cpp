#include "cell_tracker_impl.h"

#include "cluster_state_provider.h"

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cell_balancer/cell_tracker_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NCellBalancer {

using namespace NApi;
using namespace NCellBalancerClient;
using namespace NCellServer;
using namespace NCellarClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NTabletServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerProvider
    : public ICellBalancerProvider
{
public:
    TCellBalancerProvider(
        ECellarType cellarType,
        TClusterStateProviderPtr clusterStateProvider,
        TDynamicTabletCellBalancerMasterConfigPtr config)
        : CellarType_(cellarType)
        , Config_(std::move(config))
        , ClusterStateProvider_(clusterStateProvider)
    { }

    std::vector<TNodeHolder> GetNodes() override
    {
        return ClusterStateProvider_->GetNodes(CellarType_);
    }

    const TReadOnlyEntityMap<TCellBundle>& CellBundles() override
    {
        return ClusterStateProvider_->CellBundles();
    }

    bool IsPossibleHost(const TNode* node, const TArea* area) override
    {
        return ClusterStateProvider_->IsPossibleHost(node, area);
    }

    bool IsVerboseLoggingEnabled() override
    {
        return Config_->EnableVerboseLogging;
    }

    bool IsBalancingRequired() override
    {
        if (!GetConfig()->EnableTabletCellSmoothing) {
            return false;
        }

        return true;
    }

private:
    const ECellarType CellarType_;
    const TDynamicTabletCellBalancerMasterConfigPtr Config_;
    const TClusterStateProviderPtr ClusterStateProvider_;

    const TDynamicTabletCellBalancerMasterConfigPtr& GetConfig()
    {
        return Config_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellTrackerImpl::TCellTrackerImpl(
    IBootstrap* bootstrap,
    TInstant startTime,
    TDynamicTabletManagerConfigPtr config)
    : Bootstrap_(bootstrap)
    , StartTime_(startTime)
    , Config_(std::move(config))
{ }

void TCellTrackerImpl::UpdateDynamicConfig()
{
    YT_LOG_DEBUG("Updating dynamic config");
    auto result = WaitFor(Bootstrap_->GetClient()->GetNode("//sys/@config/tablet_manager"));
    if (!result.IsOK()) {
        YT_LOG_ERROR(result, "Failed to update dynamic config");
        return;
    }
    Config_ = ConvertTo<TDynamicTabletManagerConfigPtr>(result.Value());
    return;
}

void TCellTrackerImpl::ScanCells()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    YT_LOG_DEBUG("Starting scan cells");

    if (!Bootstrap_->GetElectionManager()->IsLeader()) {
        YT_LOG_DEBUG("Cell balancer is not leading");
        return;
    }

    UpdateDynamicConfig();

    TCellTrackerServiceProxy proxy(Bootstrap_
        ->GetClient()
        ->GetMasterChannelOrThrow(EMasterChannelKind::Follower));

    auto req = proxy.GetClusterState();

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

    YT_LOG_DEBUG("Cluster state is received");

    ClusterStateProvider_ = New<TClusterStateProvider>(rsp.Get());

    auto mutationRequest = proxy.ReassignPeers();

    for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
        ICellBalancerProviderPtr cellarProvider = New<TCellBalancerProvider>(
            cellarType,
            ClusterStateProvider_,
            Config_->TabletCellBalancer);
        ScanCellarCells(cellarType, std::move(cellarProvider), mutationRequest.Get());
    }

    auto* prerequisitesExt = mutationRequest->Header().MutableExtension(
        NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);

    auto transactionId = Bootstrap_->GetElectionManager()->GetPrerequisiteTransactionId();
    if (transactionId == NTransactionClient::NullTransactionId) {
        YT_LOG_DEBUG("Cell balancer is not leading");
        return;
    }

    auto* prerequisiteTransaction = prerequisitesExt->add_transactions();
    ToProto(prerequisiteTransaction->mutable_transaction_id(), transactionId);

    YT_LOG_DEBUG(
        "Request is created "
        "(AssignmentCount: %v, "
        "RevokationCount: %v, "
        "UpdateLeadingPeerCount: %v, "
        "PeerCountUpdateCount: %v)",
        mutationRequest.Get()->assignments_size(),
        mutationRequest.Get()->revocations_size(),
        mutationRequest.Get()->leading_peer_updates_size(),
        mutationRequest.Get()->peer_count_updates_size());

    WaitFor(mutationRequest->Invoke()).ValueOrThrow();

    YT_LOG_DEBUG("Scan cells completed");
}

void TCellTrackerImpl::ScanCellarCells(
    ECellarType cellarType,
    ICellBalancerProviderPtr cellarProvider,
    NCellBalancerClient::NProto::TReqReassignPeers* request)
{
    auto balancer = CreateCellBalancer(cellarProvider);

    for (auto* cell : ClusterStateProvider_->Cells(cellarType)) {

        if (cellarType == ECellarType::Tablet && Config_->DecommissionThroughExtraPeers) {
            if (SchedulePeerCountChange(cell, request)) {
                // NB: If peer count changes cells state is not valid.
                continue;
            }
        }

        if (!cell->CellBundle()->GetOptions()->IndependentPeers) {
            if (ScheduleLeaderReassignment(cell, request)) {
                continue;
            }
        }
        SchedulePeerAssignment(cell, balancer.get());
        SchedulePeerRevocation(cell, balancer.get());
    }

    auto moveDescriptors = balancer->GetCellMoveDescriptors();

    {
        NCellBalancerClient::NProto::TReqRevokePeers* revocation;
        const TCellBase* cell = nullptr;

        for (const auto& moveDescriptor : moveDescriptors) {
            const auto* source = moveDescriptor.Source;
            const auto* target = moveDescriptor.Target;

            if (source || !target) {
                if (moveDescriptor.Cell != cell) {
                    cell = moveDescriptor.Cell;
                    revocation = request->add_revocations();
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
        NCellBalancerClient::NProto::TReqAssignPeers* assignment;
        const TCellBase* cell = nullptr;

        for (const auto& moveDescriptor : moveDescriptors) {

            if (moveDescriptor.Target) {
                if (moveDescriptor.Cell != cell) {
                    cell = moveDescriptor.Cell;
                    assignment = request->add_assignments();
                    ToProto(assignment->mutable_cell_id(), cell->GetId());
                }

                auto* peerInfo = assignment->add_peer_infos();
                peerInfo->set_peer_id(moveDescriptor.PeerId);
                ToProto(peerInfo->mutable_node_descriptor(), moveDescriptor.Target->GetDescriptor());
            }
        }
    }
}

bool TCellTrackerImpl::ScheduleLeaderReassignment(
    TCellBase* cell,
    NCellBalancerClient::NProto::TReqReassignPeers* request)
{
    const auto& leadingPeer = cell->Peers()[cell->GetLeadingPeerId()];
    TError error;

    if (!leadingPeer.Descriptor.IsNull()) {
        error = IsFailed(leadingPeer, cell, Config_->LeaderReassignmentTimeout);
        if (error.IsOK()) {
            return false;
        }
    }

    if (error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) &&
        Config_->DecommissionedLeaderReassignmentTimeout &&
            (cell->LastPeerCountUpdateTime() == TInstant{} ||
             cell->LastPeerCountUpdateTime() + *Config_->DecommissionedLeaderReassignmentTimeout > TInstant::Now()))
    {
        return false;
    }

    // Switching to good follower is always better than switching to non-follower.
    int newLeaderId = FindGoodFollower(cell);

    if (Config_->DecommissionThroughExtraPeers) {
        // If node is decommissioned we switch only to followers, otherwise to any good peer.
        if (!error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned) && newLeaderId == InvalidPeerId) {
            newLeaderId = FindGoodPeer(cell);
        }
    } else if (newLeaderId == InvalidPeerId) {
        newLeaderId = FindGoodPeer(cell);
    }

    if (newLeaderId == InvalidPeerId || newLeaderId == cell->GetLeadingPeerId()) {
        return false;
    }

    YT_LOG_DEBUG(error, "Scheduling leader reassignment (CellId: %v, PeerId: %v, Address: %v)",
        cell->GetId(),
        cell->GetLeadingPeerId(),
        leadingPeer.Descriptor.GetDefaultAddress());

    auto leaderPeerUpdateRequest = request->add_leading_peer_updates();
    ToProto(leaderPeerUpdateRequest->mutable_cell_id(), cell->GetId());
    leaderPeerUpdateRequest->set_peer_id(newLeaderId);
    return true;
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


    // Try to assign missing peers.
    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (ClusterStateProvider_->IsAlienPeer(cell, peerId)) {
            continue;
        }

        if (peers[peerId].Descriptor.IsNull()) {
            balancer->AssignPeer(cell, peerId);
        }
    }
}

void TCellTrackerImpl::SchedulePeerRevocation(
    TCellBase* cell,
    ICellBalancer* balancer)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerRevocationTimeout) {
        return;
    }

    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (ClusterStateProvider_->IsAlienPeer(cell, peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (peer.Descriptor.IsNull()) {
            continue;
        }

        auto error = IsFailed(peer, cell, Config_->PeerRevocationTimeout);
        if (!error.IsOK()) {
            if (Config_->DecommissionThroughExtraPeers && error.FindMatching(NCellServer::EErrorCode::NodeDecommissioned)) {
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
        }
    }
}

bool TCellTrackerImpl::SchedulePeerCountChange(TCellBase* cell, NCellBalancerClient::NProto::TReqReassignPeers* request)
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
        if (TInstant::Now() < cell->LastLeaderChangeTime() + Config_->ExtraPeerDropDelay) {
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
    const auto* node = ClusterStateProvider_->FindNodeByAddress(peer.Descriptor.GetDefaultAddress());

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

        if (!ClusterStateProvider_->IsPossibleHost(node, cell->GetArea())) {
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

    if (!ClusterStateProvider_->IsPossibleHost(node, cell->GetArea())) {
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

int TCellTrackerImpl::FindGoodFollower(const TCellBase* cell)
{
    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (ClusterStateProvider_->IsAlienPeer(cell, peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (!ClusterStateProvider_->CheckIfNodeCanHostCells(peer.Node)) {
            continue;
        }

        if (cell->GetPeerState(peerId) != EPeerState::Following) {
            continue;
        }

        auto* slot = cell->FindCellSlot(peerId);
        if (slot && ClusterStateProvider_->IsSlotWarmedUp(slot))
        {
            return peerId;
        }
    }

    return InvalidPeerId;
}

int TCellTrackerImpl::FindGoodPeer(const TCellBase* cell)
{
    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
        if (ClusterStateProvider_->IsAlienPeer(cell, peerId)) {
            continue;
        }

        const auto& peer = cell->Peers()[peerId];
        if (ClusterStateProvider_->CheckIfNodeCanHostCells(peer.Node)) {
            return peerId;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
