#include "cluster_state_provider.h"

#include <yt/yt/server/master/cell_server/config.h>
#include <yt/yt/server/master/chaos_server/chaos_cell.h>
#include <yt/yt/server/master/tablet_server/tablet_cell.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <memory>

namespace NYT::NCellBalancer {

using namespace NCellServer;
using namespace NCellarClient;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

TClusterStateProvider::TClusterStateProvider(NCellBalancerClient::NProto::TRspGetClusterState* response)
{
    THashMap<TNode::TCellSlot*, TTamedCellId> fullSlots;
    auto fillSlot = [&](TNode::TCellSlot* slot, const NCellBalancerClient::NProto::TSlot* protoSlot)
    {
        if (protoSlot->has_cell_id()) {
            YT_VERIFY(protoSlot->has_is_warmed_up() && protoSlot->has_peer_id() && protoSlot->has_peer_state());
            fullSlots[slot] = FromProto<TTamedCellId>(protoSlot->cell_id());
            slot->PeerId = protoSlot->peer_id();
            slot->PeerState = FromProto<EPeerState>(protoSlot->peer_state());
            if (protoSlot->is_warmed_up()) {
                WarmedUpSlots_.insert(slot);
            }
        }
        return;

    };

    auto createNode = [&](NCellBalancerClient::NProto::TCellarNode* protoNode)
    {
        auto nodeId = FromProto<TNodeId>(protoNode->node_id());
        NodeMap_[nodeId] = TPoolAllocator::New<TNode>(NullObjectId);
        auto node = NodeMap_[nodeId].get();
        node->RefObject();
        for (auto& protoCellar : *protoNode->mutable_cellars()) {
            auto cellarType = FromProto<ECellarType>(protoCellar.type());
            node->Cellars()[cellarType].resize(protoCellar.slots_size());
            for (int idx = 0; idx < protoCellar.slots_size(); ++idx) {
                fillSlot(&node->Cellars()[cellarType][idx], protoCellar.mutable_slots(idx));
            }
        }
        if (protoNode->is_node_can_host_cells()) {
            CellHostNodes_.insert(node);
        }
        node->SetNodeAddresses(FromProto<TNodeAddressMap>(protoNode->node_addresses()));
        if (protoNode->decommissioned()) {
            Y_UNUSED(node->SetMaintenanceFlag(NApi::EMaintenanceType::Decommission, "", TInstant::Zero()));
        }
        if (protoNode->disable_tablet_cells()) {
            Y_UNUSED(node->SetMaintenanceFlag(NApi::EMaintenanceType::DisableTabletCells, "", TInstant::Zero()));
        }
        if (protoNode->banned()) {
            Y_UNUSED(node->SetMaintenanceFlag(NApi::EMaintenanceType::Ban, "", TInstant::Zero()));
        }
        return node;
    };

    auto fillPeer = [&](
        TCellBase::TPeer* peer,
        NCellBalancerClient::NProto::TPeer* protoPeer,
        TCellBase* cell,
        TPeerId peerId)
    {
        if (protoPeer->has_assigned_node_descriptor()) {
            FromProto(&peer->Descriptor, protoPeer->assigned_node_descriptor());
            AddressToCell_[peer->Descriptor.GetDefaultAddress()].emplace_back(cell, peerId);
        }
        if (protoPeer->has_last_seen_node_id()) {
            peer->Node = NodeMap_[FromProto<TNodeId>(protoPeer->last_seen_node_id())].get();
        }
        FromProto(&peer->LastSeenState, protoPeer->last_peer_state());
        FromProto(&peer->LastSeenTime, protoPeer->last_seen_time());
        if (protoPeer->is_alien()) {
            AlienPeers_[cell].insert(peerId);
        }
    };

    auto createCell = [&](NCellBalancerClient::NProto::TCell* protoCell, TArea* area, TCellBundle* cellBundle)
    {
        auto cellId = FromProto<TTamedCellId>(protoCell->cell_id());
        std::unique_ptr<TCellBase> cellHolder;
        switch (cellBundle->GetCellarType()) {
            case ECellarType::Tablet:
                cellHolder = TPoolAllocator::New<NTabletServer::TTabletCell>(cellId);
                break;
            case ECellarType::Chaos:
                cellHolder = TPoolAllocator::New<NChaosServer::TChaosCell>(cellId);
                break;
        }
        auto cell = CellMap_.Insert(cellId, std::move(cellHolder));
        cell->RefObject();
        cell->SetLeadingPeerId(protoCell->leading_peer_id());
        cell->CellBundle().Assign(cellBundle);
        cell->SetArea(area);
        if (protoCell->has_peer_count_override()) {
            cell->PeerCount() = protoCell->peer_count_override();
        }
        FromProto(&cell->LastPeerCountUpdateTime(), protoCell->last_peer_count_update());
        FromProto(&cell->LastLeaderChangeTime(), protoCell->last_leader_change_time());
        cell->Peers().resize(protoCell->peers_size());
        for (int idx = 0; idx < protoCell->peers_size(); ++idx) {
            fillPeer(&cell->Peers()[idx], protoCell->mutable_peers(idx), cell, idx);
        }
        return cell;
    };

    auto createArea = [&](NCellBalancerClient::NProto::TArea* protoArea, TCellBundle* cellBundle)
    {
        auto areaId = FromProto<TObjectId>(protoArea->area_id());
        auto area = AreaMap_.Insert(areaId, TPoolAllocator::New<TArea>(areaId));
        area->RefObject();
        area->SetName(protoArea->name());
        area->SetCellBundle(cellBundle);
        for (auto& protoCell : *protoArea->mutable_cells()) {
            auto cell = createCell(&protoCell, area, cellBundle);
            area->Cells().insert(cell);
        }
        for (auto& protoId : protoArea->node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoId);
            AreaToNodesMap_[area].insert(NodeMap_[nodeId].get());
        }
        return area;
    };

    auto createCellBundle = [&](NCellBalancerClient::NProto::TCellBundle* protoCellBundle)
    {
        auto bundleId = FromProto<TCellBundleId>(protoCellBundle->bundle_id());
        auto cellBundle = CellBundleMap_.Insert(bundleId, TPoolAllocator::New<TCellBundle>(bundleId));
        cellBundle->RefObject();
        cellBundle->SetName(protoCellBundle->name());
        for (auto& protoArea : *protoCellBundle->mutable_areas()) {
            auto area = createArea(&protoArea, cellBundle);
            cellBundle->Cells().insert(area->Cells().begin(), area->Cells().end());
            if (area->GetName() == DefaultAreaName) {
                cellBundle->SetDefaultArea(area);
            }
            cellBundle->Areas()[area->GetName()] = area;
        }
        auto options = New<TTabletCellOptions>();
        options->IndependentPeers = protoCellBundle->independent_peers();
        cellBundle->SetOptions(std::move(options));
        cellBundle->CellBalancerConfig() = ConvertTo<NCellServer::TCellBalancerConfigPtr>(
            TYsonString(protoCellBundle->cell_balancer_config()));
        return cellBundle;
    };

    for (auto& protoNode : *response->mutable_nodes()) {
        createNode(&protoNode);
    }

    for (auto& protoCellBundle : *response->mutable_cell_bundles()) {
        createCellBundle(&protoCellBundle);
    }

    for (auto& [slot, cellId] : fullSlots) {
        slot->Cell = CellMap_.Get(cellId);
    }

    for (auto& [cellId, cell] : CellMap_) {
        CellsPerTypeMap_[cell->GetCellarType()].insert(cell);
    }

    for (auto& [nodeId, node] : NodeMap_) {
        AddressToNode_[node->GetDefaultAddress()] = node.get();
    }

    YT_LOG_DEBUG("Cluster state is created (CellCount: %v, AreaCount: %v, CellBundleCount: %v, NodeCount: %v)",
        CellMap_.GetSize(),
        AreaMap_.GetSize(),
        CellBundleMap_.GetSize(),
        std::ssize(NodeMap_));
}

std::vector<TNodeHolder> TClusterStateProvider::GetNodes(NCellarClient::ECellarType cellarType)
{
    auto isGood = [&] (const auto* node) {
        return node->GetCellarSize(cellarType) > 0 && CheckIfNodeCanHostCells(node);
    };

    int nodeCount = 0;
    for (auto& [nodeId, node] : NodeMap_) {
        if (isGood(node.get())) {
            ++nodeCount;
        }
    }

    std::vector<TNodeHolder> nodes;
    nodes.reserve(nodeCount);
    for (auto& [nodeId, node] : NodeMap_) {
        if (!isGood(node.get())) {
            continue;
        }

        const auto* cells = FindAssignedCells(node->GetDefaultAddress());
        nodes.emplace_back(
            node.get(),
            node->GetCellarSize(cellarType),
            cells ? *cells : TCellSet());
    }

    return nodes;
}

bool TClusterStateProvider::IsPossibleHost(const TNode* node, const TArea* area) const
{
    auto it = AreaToNodesMap_.find(area);
    if (it == AreaToNodesMap_.end()) {
        return false;
    }
    return it->second.contains(node);
}

bool TClusterStateProvider::IsSlotWarmedUp(const TNode::TCellSlot* slot) const
{
    return WarmedUpSlots_.contains(slot);
}

bool TClusterStateProvider::CheckIfNodeCanHostCells(const TNode* node) const
{
    return CellHostNodes_.contains(node);
}

const TCellSet* TClusterStateProvider::FindAssignedCells(const TString& address) const
{
    auto it = AddressToCell_.find(address);
    return it != AddressToCell_.end()
        ? &it->second
        : nullptr;
}

const THashSet<TCellBase*>& TClusterStateProvider::Cells(ECellarType cellarType)
{
    return CellsPerTypeMap_[cellarType];
}

bool TClusterStateProvider::IsAlienPeer(const TCellBase* cell, TPeerId peerId) const
{
    auto it = AlienPeers_.find(cell);
    if (it == AlienPeers_.end()) {
        return false;
    }
    return it->second.contains(peerId);
}

TNode* TClusterStateProvider::FindNodeByAddress(const TString& address)
{
    auto it = AddressToNode_.find(address);
    if (it == AddressToNode_.end()) {
        return nullptr;
    }
    return it->second;
}

DEFINE_ENTITY_MAP_ACCESSORS(TClusterStateProvider, CellBundle, TCellBundle, CellBundleMap_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
