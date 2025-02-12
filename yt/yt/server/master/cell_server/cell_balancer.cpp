#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_balancer.h"
#include "cell_bundle.h"
#include "area.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/object_server/helpers.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NCellServer {

using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

TCellMoveDescriptor::TCellMoveDescriptor(
    const TCellBase* cell,
    int peerId,
    const TNode* source,
    const TNode* target,
    TError reason)
    : Cell(cell)
    , PeerId(peerId)
    , Source(source)
    , Target(target)
    , Reason(std::move(reason))
{ }

bool TCellMoveDescriptor::operator<(const TCellMoveDescriptor& other) const
{
    return Cell == other.Cell
        ? PeerId < other.PeerId
        : Cell < other.Cell;
}

bool TCellMoveDescriptor::operator==(const TCellMoveDescriptor& other) const
{
    return Cell == other.Cell && PeerId == other.PeerId;
}

////////////////////////////////////////////////////////////////////////////////

TNodeHolder::TNodeHolder(const TNode* node, int totalSlots, const TCellSet& slots)
    : Node_(node)
    , TotalSlots_(totalSlots)
    , Slots_(slots)
{
    UpdateCellCounts();
}

const TNode* TNodeHolder::GetNode() const
{
    return Node_;
}

int TNodeHolder::GetTotalSlots() const
{
    return TotalSlots_;
}

const TCellSet& TNodeHolder::GetSlots() const
{
    return Slots_;
}

std::pair<const TCellBase*, int> TNodeHolder::ExtractCell(int cellIndex)
{
    YT_ASSERT(cellIndex < std::ssize(Slots_));

    auto pair = Slots_[cellIndex];
    Slots_[cellIndex] = Slots_.back();
    Slots_.pop_back();
    --CellCount_[pair.first->GetArea()];
    return pair;
}

void TNodeHolder::InsertCell(std::pair<const TCellBase*, int> pair)
{
    Slots_.push_back(pair);
    ++CellCount_[pair.first->GetArea()];
}

std::optional<int> TNodeHolder::FindCell(const TCellBase* cell)
{
    for (int cellIndex = 0; cellIndex < std::ssize(Slots_); ++cellIndex) {
        if (Slots_[cellIndex].first == cell) {
            return cellIndex;
        }
    }
    return std::nullopt;
}

std::pair<const TCellBase*, int> TNodeHolder::RemoveCell(const TCellBase* cell)
{
    return ExtractCell(*FindCell(cell));
}

int TNodeHolder::GetCellCount(const TArea* area) const
{
    auto it = CellCount_.find(area);
    return it != CellCount_.end() ? it->second : 0;
}

void TNodeHolder::UpdateCellCounts()
{
    for (auto [cell, _] : Slots_) {
        CellCount_[cell->GetArea()] += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCellBalancer
    : public ICellBalancer
{
public:
    explicit TCellBalancer(
        ICellBalancerProviderPtr provider)
        : Provider_(std::move(provider))
    { }

    void AssignPeer(const TCellBase* cell, int peerId) override
    {
        LazyInitNodes();

        auto* node = TryAllocateNode(cell);

        if (Provider_->IsVerboseLoggingEnabled()) {
            YT_LOG_DEBUG("Tablet tracker assigning peer (CellId: %v, PeerId: %v, AllocatedNode: %v)",
                cell->GetId(),
                peerId,
                node ? node->GetNode()->GetDefaultAddress() : "None");
        }

        if (node) {
            AddCell(node, cell, peerId);
        }
    }

    void RevokePeer(const TCellBase* cell, int peerId, const TError& reason) override
    {
        LazyInitNodes();

        const auto& descriptor = cell->Peers()[peerId].Descriptor;

        if (Provider_->IsVerboseLoggingEnabled()) {
            auto node = cell->Peers()[peerId].Node;
            YT_LOG_DEBUG(reason, "Tablet tracker revoking peer (CellId: %v, PeerId: %v, Node: %v, DescriptorAddress: %v)",
                cell->GetId(),
                peerId,
                node ? node->GetDefaultAddress() : NullNodeAddress(),
                descriptor.GetDefaultAddress());
        }

        const auto* node = PeerTracker_.FindPeer(cell, peerId);
        if (node) {
            BannedPeerTracker_.AddPeer(cell, peerId, node);
            PeerTracker_.RemovePeer(cell, peerId, node);
            if (auto it = NodeToIndex_.find(node)) {
                MoveNodeToFreedListIfNotFilled(&Nodes_[it->second]);
                Nodes_[it->second].RemoveCell(cell);
            }
        }

        MoveDescriptors_.emplace_back(
            cell,
            peerId,
            node,
            nullptr,
            reason);
    }

    std::vector<TCellMoveDescriptor> GetCellMoveDescriptors() override
    {
        if (Provider_->IsBalancingRequired()) {
            LazyInitNodes();
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            auto dumpId = TGuid::Create();
            YT_LOG_DEBUG("Tablet cells distribution before balancing (DumpId: %v)", dumpId);
            DumpState(dumpId);
        }

        bool hasDecommissionedNodes = false;
        for (const auto& node : Nodes_) {
            if (!node.GetSlots().empty() && node.GetNode()->IsDecommissioned()) {
                hasDecommissionedNodes = true;
            }
        }

        if (hasDecommissionedNodes) {
            if (Provider_->IsVerboseLoggingEnabled()) {
                YT_LOG_DEBUG("Cluster has decommissioned tablet nodes with cells, skipping cell rebalancing");
            }
        } else {
            for (auto [bundleId, bundle] : Provider_->CellBundles()) {
                if (!IsObjectAlive(bundle)) {
                    continue;
                }
                if (bundle->CellBalancerConfig()->EnableTabletCellSmoothing) {
                    RebalanceBundle(bundle);
                }
            }
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            auto dumpId = TGuid::Create();
            YT_LOG_DEBUG("Tablet cells distribution after balancing (DumpId: %v)", dumpId);
            DumpState(dumpId);
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            YT_LOG_DEBUG("Tablet cell balancer requests moves (before filter): %v",
                MakeFormattableView(MoveDescriptors_, [] (TStringBuilderBase* builder, const TCellMoveDescriptor& action) {
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        action.Cell->GetId(),
                        action.PeerId,
                        action.Source ? action.Source->GetDefaultAddress() : NullNodeAddress(),
                        action.Target ? action.Target->GetDefaultAddress() : NullNodeAddress());
                }));
        }

        FilterActions();

        if (Provider_->IsVerboseLoggingEnabled()) {
            YT_LOG_DEBUG("Tablet cell balancer requests moves (after filter): %v",
                MakeFormattableView(MoveDescriptors_, [] (TStringBuilderBase* builder, const TCellMoveDescriptor& action) {
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        action.Cell->GetId(),
                        action.PeerId,
                        action.Source ? action.Source->GetDefaultAddress() : NullNodeAddress(),
                        action.Target ? action.Target->GetDefaultAddress() : NullNodeAddress());
                }));
        }

        return std::move(MoveDescriptors_);
    }

private:
    class TPeerTracker
    {
    public:
        void AddPeer(const TCellBase* cell, int peerId, const TNode* peer)
        {
            YT_ASSERT(!IsPeer(cell, peer));

            auto& peers = Peers_[cell];
            if (std::ssize(peers) <= peerId) {
                peers.resize(peerId + 1, nullptr);
            }

            YT_ASSERT(peers[peerId] == nullptr);
            peers[peerId] = peer;
        }

        const TNode* FindPeer(const TCellBase* cell, int peerId)
        {
            if (auto it = Peers_.find(cell)) {
                const auto& peers = it->second;
                if (peerId < std::ssize(peers)) {
                    return peers[peerId];
                }
            }

            return nullptr;
        }

        void RemovePeer(const TCellBase* cell, int peerId, const TNode* peer)
        {
            YT_ASSERT(IsPeer(cell, peer));

            auto& peers = Peers_[cell];
            YT_ASSERT(peers[peerId] == peer);
            peers[peerId] = nullptr;
        }

        int MoveCell(const TCellBase* cell, const TNode* src, const TNode* dst)
        {
            YT_ASSERT(IsPeer(cell, src));

            auto& peers = Peers_[cell];
            for (int peerId = 0; peerId < std::ssize(peers); ++peerId) {
                if (peers[peerId] == src) {
                    peers[peerId] = dst;
                    return peerId;
                }
            }

            YT_ABORT();
        }

        bool IsPeer(const TCellBase* cell, const TNode* node) const
        {
            auto it = Peers_.find(cell);
            if (it == Peers_.end()) {
                return false;
            }

            for (const auto* peer : it->second) {
                if (peer == node) {
                    return true;
                }
            }

            return false;
        }

        void Clear()
        {
            Peers_.clear();
        }

    private:
        THashMap<const TCellBase*, TCompactVector<const TNode*, TypicalPeerCount>> Peers_;
    };

    const ICellBalancerProviderPtr Provider_;

    bool Initialized_ = false;
    std::vector<TNodeHolder> Nodes_;
    THashMap<const TNode*, int> NodeToIndex_;
    TPeerTracker PeerTracker_;
    TPeerTracker BannedPeerTracker_;
    THashMap<const TArea*, std::vector<int>> FreeNodes_;
    THashMap<const TArea*, THashSet<int>> FilledNodes_;

    std::vector<TCellMoveDescriptor> MoveDescriptors_;

    void DumpState(TGuid dumpId)
    {
        for (const auto& node : Nodes_) {
            YT_LOG_DEBUG("Tablet cell distribution: %v %v (DumpId: %v)",
                node.GetNode()->GetDefaultAddress(),
                MakeFormattableView(node.GetSlots(), [] (TStringBuilderBase* builder, const std::pair<const TCellBase*, int>& pair) {
                    auto [cell, peerId] = pair;
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        cell->CellBundle()->GetName(),
                        cell->GetArea()->GetName(),
                        cell->GetId(),
                        peerId);
                }),
                dumpId);
        }
    }

    void LazyInitNodes()
    {
        if (Initialized_) {
            return;
        }

        Initialized_ = true;

        Nodes_ = Provider_->GetNodes();

        for (int nodeIndex = 0; nodeIndex < std::ssize(Nodes_); ++nodeIndex) {
            const auto& node = Nodes_[nodeIndex];
            NodeToIndex_[node.GetNode()] = nodeIndex;

            for (const auto& [bundleId, bundle] : Provider_->CellBundles()) {
                if (!IsObjectAlive(bundle)) {
                    continue;
                }
                for (const auto& [_, area] : bundle->Areas()) {
                    if (Provider_->IsPossibleHost(node.GetNode(), area)) {
                        if (node.GetTotalSlots() > std::ssize(node.GetSlots())) {
                            FreeNodes_[area].push_back(nodeIndex);
                        } else {
                            FilledNodes_[area].insert(nodeIndex);
                        }
                    }
                }
            }

            for (auto [cell, peerId] : node.GetSlots()) {
                PeerTracker_.AddPeer(cell, peerId, node.GetNode());
            }
        }
    }

    void FilterActions()
    {
        std::stable_sort(MoveDescriptors_.begin(), MoveDescriptors_.end());

        int last = -1;
        for (int index = 0; index < std::ssize(MoveDescriptors_); ++index) {
            if (last < 0 || MoveDescriptors_[last] != MoveDescriptors_[index]) {
                if (last >= 0 && MoveDescriptors_[last].Source == MoveDescriptors_[last].Target && MoveDescriptors_[last].Target) {
                    --last;
                }

                ++last;
                if (last != index) {
                    MoveDescriptors_[last] = MoveDescriptors_[index];
                }
            }
            if (MoveDescriptors_[last] == MoveDescriptors_[index]) {
                MoveDescriptors_[last].Target = MoveDescriptors_[index].Target;
            }
        }
        MoveDescriptors_.resize(last + 1);
    }

    TNodeHolder* TryAllocateNode(const TCellBase* cell)
    {
        auto area = cell->GetArea();

        auto it = FreeNodes_.find(area);
        if (it == FreeNodes_.end()) {
            return nullptr;
        }

        std::optional<int> peerNodeIndex;
        auto& queue = it->second;

        for (int index = 0; index < std::ssize(queue); ++index) {
            auto nodeIndex = queue[index];
            YT_VERIFY(nodeIndex < std::ssize(Nodes_));
            auto* node = &Nodes_[nodeIndex];
            if (node->GetTotalSlots() == std::ssize(node->GetSlots())) {
                std::swap(queue[index], queue.back());
                queue.pop_back();
                YT_ASSERT(!FilledNodes_[area].contains(nodeIndex));
                FilledNodes_[area].insert(nodeIndex);
                --index;
            } else if (!NodeInPeers(cell, node)) {
                return node;
            } else {
                peerNodeIndex = nodeIndex;
            }
        }

        if (peerNodeIndex) {
            return TryAllocateMultipeerNode(cell, *peerNodeIndex);
        }

        return nullptr;
    }

    TNodeHolder* TryAllocateMultipeerNode(const TCellBase* cell, int peerNodeIndex)
    {
        auto* peerNode = &Nodes_[peerNodeIndex];

        auto it = FilledNodes_.find(cell->GetArea());
        if (it == FilledNodes_.end()) {
            return nullptr;
        }

        for (auto nodeIndex : it->second) {
            YT_VERIFY(nodeIndex < std::ssize(Nodes_));
            YT_VERIFY(nodeIndex != peerNodeIndex);
            auto* node = &Nodes_[nodeIndex];
            if (NodeInPeers(cell, node)) {
                continue;
            }

            if (TryExchangeCell(cell, peerNode, node)) {
                return peerNode;
            }
        }

        return nullptr;
    }

    bool TryExchangeCell(const TCellBase* cell, TNodeHolder* srcNode, TNodeHolder* dstNode)
    {
        int srcIndex = *srcNode->FindCell(cell);

        int dstIndex = 0;
        for (auto [dstCell, _] : dstNode->GetSlots()) {
            if (NodeInPeers(dstCell, srcNode) ||
                !Provider_->IsPossibleHost(srcNode->GetNode(), dstCell->GetArea()))
            {
                ++dstIndex;
                continue;
            }

            ExchangeCells(srcNode, srcIndex, dstNode, dstIndex);
            return true;
        }

        return false;
    }

    void AddCell(TNodeHolder* dstNode, const TCellBase* cell, int peerId)
    {
        dstNode->InsertCell(std::pair(cell, peerId));
        PeerTracker_.AddPeer(cell, peerId, dstNode->GetNode());
        MoveDescriptors_.emplace_back(
            cell,
            peerId,
            nullptr,
            dstNode->GetNode(),
            TError("Cell balancer is adding peer at %v",
                dstNode->GetNode()->GetDefaultAddress()));
    }

    void MoveCell(TNodeHolder* srcNode, int srcIndex, TNodeHolder* dstNode)
    {
        MoveNodeToFreedListIfNotFilled(srcNode);
        auto srcCell = srcNode->ExtractCell(srcIndex);
        dstNode->InsertCell(srcCell);
        // TODO(savrus) use peerId form ExtractCell.
        int srcPeerId = PeerTracker_.MoveCell(srcCell.first, srcNode->GetNode(), dstNode->GetNode());
        MoveDescriptors_.emplace_back(
            srcCell.first,
            srcPeerId,
            srcNode->GetNode(),
            dstNode->GetNode(),
            TError("Cell balancer is moving peer from %v to %v",
                srcNode->GetNode()->GetDefaultAddress(),
                dstNode->GetNode()->GetDefaultAddress()));
    }

    void ExchangeCells(TNodeHolder* srcNode, int srcIndex, TNodeHolder* dstNode, int dstIndex)
    {
        auto srcCell = srcNode->ExtractCell(srcIndex);
        auto dstCell = dstNode->ExtractCell(dstIndex);
        srcNode->InsertCell(dstCell);
        dstNode->InsertCell(srcCell);
        // TODO(savrus) use peerId form ExtractCell.
        int srcPeerId = PeerTracker_.MoveCell(srcCell.first, srcNode->GetNode(), dstNode->GetNode());
        int dstPeerId = PeerTracker_.MoveCell(dstCell.first, dstNode->GetNode(), srcNode->GetNode());
        MoveDescriptors_.emplace_back(
            srcCell.first,
            srcPeerId,
            srcNode->GetNode(),
            dstNode->GetNode(),
            TError("Cell balancer is exchanging cell %v at %v with cell %v at %v",
                srcCell.first->GetId(),
                srcNode->GetNode()->GetDefaultAddress(),
                dstCell.first->GetId(),
                dstNode->GetNode()->GetDefaultAddress()));
        MoveDescriptors_.emplace_back(
            dstCell.first,
            dstPeerId,
            dstNode->GetNode(),
            srcNode->GetNode(),
            TError("Cell balancer is exchanging cell %v at %v with cell %v at %v",
                dstCell.first->GetId(),
                dstNode->GetNode()->GetDefaultAddress(),
                srcCell.first->GetId(),
                srcNode->GetNode()->GetDefaultAddress()));
    }

    void MoveNodeToFreedListIfNotFilled(TNodeHolder* node)
    {
        /* This function is called from MoveCell and RevokePeer to process the situation
         * when node stops being filled, so it can be added to vector of FreeNodes of possibly hosted bundles.
         * In other cases when exchange is done it swaps real nodes, so filled nodes remain filled.
         * There is no need to update nodes from free to filled, because it is done lazily upon peer assigning.
         */

        if (node->GetTotalSlots() != std::ssize(node->GetSlots())) {
            return;
        }

        auto nodeIndex = NodeToIndex_[node->GetNode()];
        for (const auto& [bundleId, bundle] : Provider_->CellBundles()) {
            if (!IsObjectAlive(bundle)) {
                continue;
            }
            for (const auto& [_, area] : bundle->Areas()) {
                if (Provider_->IsPossibleHost(node->GetNode(), area)) {
                    if (auto it = FilledNodes_[area].find(nodeIndex); it != FilledNodes_[area].end()) {
                        FilledNodes_[area].erase(it);
                        YT_ASSERT(std::find(FreeNodes_[area].begin(), FreeNodes_[area].end(), nodeIndex) ==
                            FreeNodes_[area].end());
                        FreeNodes_[area].emplace_back(nodeIndex);
                    }
                }
            }
        }
    }

    bool NodeInPeers(const TCellBase* cell, const TNodeHolder* node)
    {
        return PeerTracker_.IsPeer(cell, node->GetNode()) ||
            BannedPeerTracker_.IsPeer(cell, node->GetNode());
    }

    void SmoothNodes(TNodeHolder* srcNode, TNodeHolder* dstNode, const TArea* area, int limit)
    {
        if (srcNode->GetCellCount(area) < dstNode->GetCellCount(area)) {
            std::swap(srcNode, dstNode);
        }

        int srcIndex = 0;
        int dstIndex = 0;
        while (srcIndex < std::ssize(srcNode->GetSlots()) &&
            dstIndex < dstNode->GetTotalSlots() &&
            srcNode->GetCellCount(area) != limit &&
            dstNode->GetCellCount(area) != limit)
        {
            auto* srcCell = srcNode->GetSlots()[srcIndex].first;
            if (srcCell->GetArea() != area ||
                NodeInPeers(srcCell, dstNode))
            {
                ++srcIndex;
                continue;
            }

            if (dstNode->GetTotalSlots() > std::ssize(dstNode->GetSlots())) {
                MoveCell(srcNode, srcIndex, dstNode);
                continue;
            }

            const auto* dstCell = dstNode->GetSlots()[dstIndex].first;
            auto dstArea = dstCell->GetArea();
            if (dstArea == area ||
                NodeInPeers(dstCell, srcNode) ||
                srcNode->GetCellCount(dstArea) >= dstNode->GetCellCount(dstArea) ||
                !Provider_->IsPossibleHost(srcNode->GetNode(), dstArea))
            {
                ++dstIndex;
                continue;
            }

            ExchangeCells(srcNode, srcIndex, dstNode, dstIndex);
        }
    }

    void RebalanceBundle(const TCellBundle* bundle)
    {
        for (const auto& [_, area] : bundle->Areas()) {
            RebalanceArea(area);
        }
    }

    void RebalanceArea(const TArea* area)
    {
        std::vector<TNodeHolder*> nodes;
        for (auto& node : Nodes_) {
            if (Provider_->IsPossibleHost(node.GetNode(), area)) {
                nodes.push_back(&node);
            }
        }

        if (nodes.empty()) {
            return;
        }

        auto smooth = [&] (std::vector<TNodeHolder*>& candidates, int limit, auto filter) {
            for (auto* srcNode : nodes) {
                if (!filter(srcNode)) {
                    continue;
                }

                int candidateIndex = 0;
                while (candidateIndex < std::ssize(candidates)) {
                    if (srcNode->GetCellCount(area) == limit) {
                        break;
                    }
                    auto* dstNode = candidates[candidateIndex];
                    SmoothNodes(srcNode, dstNode, area, limit);
                    if (dstNode->GetCellCount(area) == limit) {
                        candidates[candidateIndex] = candidates.back();
                        candidates.pop_back();
                    } else {
                        ++candidateIndex;
                    }
                }
            }
        };

        auto slotCount = std::ssize(area->Cells()) * area->GetCellBundle()->GetOptions()->PeerCount;
        auto ceil = DivCeil<i64>(slotCount, std::ssize(nodes));
        auto floor = slotCount / std::ssize(nodes);

        auto aboveCeil = std::count_if(nodes.begin(), nodes.end(), [&] (const auto* node) {
            return node->GetCellCount(area) > ceil;
        });
        auto belowFloor = std::count_if(nodes.begin(), nodes.end(), [&] (const auto* node) {
            return node->GetCellCount(area) < floor;
        });

        if (aboveCeil > 0 || belowFloor > 0) {
            YT_LOG_DEBUG("Tablet cell balancer needs to smooth bundle (Bundle: %v, Area: %v, Ceil: %v, Floor: %v, AboveCeilCount: %v, BelowFloorCount: %v)",
                area->GetCellBundle()->GetName(),
                area->GetName(),
                ceil,
                floor,
                aboveCeil,
                belowFloor);
        }

        std::vector<TNodeHolder*> candidates;
        std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(candidates), [&] (const auto* node) {
            return node->GetCellCount(area) < ceil;
        });
        smooth(candidates, ceil, [&] (const auto* node) {
            return node->GetCellCount(area) > ceil;
        });

        candidates.clear();
        std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(candidates), [&] (const auto* node) {
            return node->GetCellCount(area) > floor;
        });
        smooth(candidates, floor, [&] (const auto* node) {
            return node->GetCellCount(area) < floor;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICellBalancer> CreateCellBalancer(
    ICellBalancerProviderPtr provider)
{
    return std::make_unique<TCellBalancer>(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
