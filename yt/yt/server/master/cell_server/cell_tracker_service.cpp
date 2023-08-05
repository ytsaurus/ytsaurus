#include "cell_tracker_service.h"

#include "area.h"
#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/ytlib/cell_balancer/cell_tracker_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellBalancerClient;
using namespace NHydra;
using namespace NRpc;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TCellTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCellTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::CellTrackerService,
            CellServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReassignPeers)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetClusterState)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NCellBalancerClient::NProto, ReassignPeers)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo(
            "AssignPeerCount: %v, "
            "RevokePeerCount: %v, "
            "PeerCountUpdateCount: %v, "
            "SetLeadingPeerCount: %v",
            request->assignments_size(),
            request->revocations_size(),
            request->peer_count_updates_size(),
            request->leading_peer_updates_size());

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        context->ReplyFrom(CreateMutation(hydraManager, *request)
            ->CommitAndLog(Logger).AsVoid());
    }

    DECLARE_RPC_SERVICE_METHOD(NCellBalancerClient::NProto, GetClusterState)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);

        context->SetRequestInfo();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            THROW_ERROR_EXCEPTION("Cannot get cluster state at secondary master");
        }

        SyncWithUpstream();

        IBundleNodeTracker::TNodeSet allNodes;

        auto fillPeer = [&] (
            NCellBalancerClient::NProto::TPeer* protoPeer,
            const TCellBase::TPeer& peer,
            const TCellBase& cell,
            TPeerId peerId)
        {
            if (!peer.Descriptor.IsNull()) {
                ToProto(protoPeer->mutable_assigned_node_descriptor(), peer.Descriptor);
            }
            if (peer.Node) {
                protoPeer->set_last_seen_node_id(ToProto<ui32>(peer.Node->GetId()));
                allNodes.insert(peer.Node);
            }
            protoPeer->set_last_peer_state(::NYT::ToProto<i32>(peer.LastSeenState));
            protoPeer->set_last_seen_time(::NYT::ToProto<i64>(peer.LastSeenTime));
            protoPeer->set_is_alien(cell.IsAlienPeer(peerId));
        };

        int cellCount = 0;

        auto fillCell = [&] (
            NCellBalancerClient::NProto::TCell* protoCell,
            const TCellBase& cell)
        {
            ++cellCount;
            ToProto(protoCell->mutable_cell_id(), cell.GetId());
            for (int index = 0; index < std::ssize(cell.Peers()); ++index) {
                fillPeer(protoCell->add_peers(), cell.Peers()[index], cell, index);
            }
            protoCell->set_leading_peer_id(cell.GetLeadingPeerId());
            if (auto peerCountOverride = cell.PeerCount()) {
                protoCell->set_peer_count_override(*peerCountOverride);
            }
            protoCell->set_last_leader_change_time(::NYT::ToProto<i64>(cell.LastLeaderChangeTime()));
            protoCell->set_last_peer_count_update(::NYT::ToProto<i64>(cell.LastPeerCountUpdateTime()));
        };

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        const auto& bundleTracker = cellManager->GetBundleNodeTracker();

        auto fillArea = [&] (
            NCellBalancerClient::NProto::TArea* protoArea,
            const TArea& area)
        {
            ToProto(protoArea->mutable_area_id(), area.GetId());
            protoArea->set_name(area.GetName());
            for (auto cell : area.Cells()) {
                if (IsObjectAlive(cell)) {
                    fillCell(protoArea->add_cells(), *cell);
                }
            }
            const auto& nodeSet = bundleTracker->GetAreaNodes(&area);
            for (auto node : nodeSet) {
                if (IsObjectAlive(node)) {
                    protoArea->add_node_ids(ToProto<ui32>(node->GetId()));
                }
            }
            allNodes.insert(nodeSet.begin(), nodeSet.end());
        };

        auto fillCellBundle = [&] (
            NCellBalancerClient::NProto::TCellBundle* protoCellBundle,
            const TCellBundle& cellBundle)
        {
            ToProto(protoCellBundle->mutable_bundle_id(), cellBundle.GetId());
            protoCellBundle->set_name(cellBundle.GetName());
            protoCellBundle->set_independent_peers(cellBundle.GetOptions()->IndependentPeers);
            protoCellBundle->set_cell_balancer_config(ConvertToYsonString(cellBundle.CellBalancerConfig()).ToString());
            for (const auto& [areaId, area] : cellBundle.Areas()) {
                if (IsObjectAlive(area)) {
                    fillArea(protoCellBundle->add_areas(), *area);
                }
            }
        };

        auto fillSlot = [&] (
            NCellBalancerClient::NProto::TSlot* protoSlot,
            const NNodeTrackerServer::TNode::TCellSlot& slot)
        {
            if (!slot.Cell) {
                return;
            }
            ToProto(protoSlot->mutable_cell_id(), slot.Cell->GetId());
            protoSlot->set_peer_id(slot.PeerId);
            protoSlot->set_peer_state(::NYT::ToProto<i32>(slot.PeerState));
            protoSlot->set_is_warmed_up(slot.IsWarmedUp());
        };

        auto fillCellar = [&] (
            NCellBalancerClient::NProto::TCellar* protoCellar,
            const NNodeTrackerServer::TNode::TCellar& cellar,
            NCellarClient::ECellarType type)
        {
            protoCellar->set_type(::NYT::ToProto<i32>(type));
            for (const auto& cell : cellar) {
                fillSlot(protoCellar->add_slots(), cell);
            }
        };

        auto fillNode = [&] (
            NCellBalancerClient::NProto::TCellarNode* protoNode,
            const NNodeTrackerServer::TNode& node)
        {
            protoNode->set_node_id(::NYT::ToProto<i32>(node.GetId()));
            protoNode->set_is_node_can_host_cells(CheckIfNodeCanHostCells(&node));
            ToProto(protoNode->mutable_node_addresses(), node.GetNodeAddresses());
            protoNode->set_decommissioned(node.IsDecommissioned());
            protoNode->set_banned(node.IsBanned());
            protoNode->set_disable_tablet_cells(node.AreTabletCellsDisabled());
            for (const auto& [type, cellar] : node.Cellars()) {
                fillCellar(protoNode->add_cellars(), cellar, type);
            }
        };

        for (auto cellarType : TEnumTraits<NCellarClient::ECellarType>::GetDomainValues()) {
            for (const auto* cellBundle : cellManager->CellBundles(cellarType)) {
                if (IsObjectAlive(cellBundle)) {
                    fillCellBundle(response->add_cell_bundles(), *cellBundle);
                }
            }
        }
        for (auto node : allNodes) {
            if (IsObjectAlive(node)) {
                fillNode(response->add_nodes(), *node);
            }
        }

        context->SetResponseInfo(
            "NodeCount: %v, "
            "CellCount: %v",
            response->nodes_size(),
            cellCount);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCellTrackerService(TBootstrap* bootstrap)
{
    return New<TCellTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
