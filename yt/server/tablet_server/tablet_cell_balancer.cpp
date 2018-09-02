#include "private.h"
#include "tablet_cell.h"
#include "tablet_cell_balancer.h"
#include "tablet_cell_bundle.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NTabletServer {

using namespace NNodeTrackerServer;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTabletCellMoveDescriptor::TTabletCellMoveDescriptor(const TTabletCell* cell, int peerId, const TNode* source, const TNode* target)
    : Cell(cell)
    , PeerId(peerId)
    , Source(source)
    , Target(target)
{ }

bool TTabletCellMoveDescriptor::operator<(const TTabletCellMoveDescriptor& other) const
{
    return Cell == other.Cell
        ? PeerId < other.PeerId
        : Cell < other.Cell;
}

bool TTabletCellMoveDescriptor::operator==(const TTabletCellMoveDescriptor& other) const
{
    return Cell == other.Cell && PeerId == other.PeerId;
}

bool TTabletCellMoveDescriptor::operator!=(const TTabletCellMoveDescriptor& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TNodeHolder::TNodeHolder(const TNode* node, int totalSlots, const TTabletCellSet& slots)
    : Node_(node)
    , TotalSlots_(totalSlots)
    , Slots_(slots)
{
    CountCells();
}

const TNode* TNodeHolder::GetNode() const
{
    return Node_;
}

int TNodeHolder::GetTotalSlots() const
{
    return TotalSlots_;
}

const TTabletCellSet& TNodeHolder::GetSlots() const
{
    return Slots_;
}

std::pair<const TTabletCell*, int> TNodeHolder::ExtractCell(int cellIndex)
{
    Y_ASSERT(cellIndex < Slots_.size());

    auto pair = Slots_[cellIndex];
    Slots_[cellIndex] = Slots_.back();
    Slots_.pop_back();
    --CellCount_[pair.first->GetCellBundle()];
    return pair;
}

void TNodeHolder::InsertCell(std::pair<const TTabletCell*, int> pair)
{
    Slots_.push_back(pair);
    ++CellCount_[pair.first->GetCellBundle()];
}

void TNodeHolder::RemoveCell(const TTabletCell* cell)
{
    for (int cellIndex = 0; cellIndex < Slots_.size(); ++cellIndex) {
        if (Slots_[cellIndex].first == cell) {
            ExtractCell(cellIndex);
            return;
        }
    }
    Y_ASSERT(false);
}

int TNodeHolder::GetCellCount(const TTabletCellBundle* bundle) const
{
    auto it = CellCount_.find(bundle);
    return it != CellCount_.end() ? it->second : 0;
}

void TNodeHolder::CountCells()
{
    for (const auto& pair : Slots_) {
        const auto* cell = pair.first;
        CellCount_[cell->GetCellBundle()] += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBalancer
    : public ITabletCellBalancer
{
public:
    explicit TTabletCellBalancer(
        ITabletCellBalancerProviderPtr provider)
        : Provider_(std::move(provider))
    { }

    virtual void AssignPeer(const TTabletCell* cell, int peerId) override
    {
        LazyInitNodes();

        auto* node = TryAllocateNode(cell);

        if (Provider_->IsVerboseLoggingEnabled()) {
            LOG_DEBUG("Tablet tracker assigning peer (CellId: %v, PeerId: %v, AllocatedNode: %v)",
                cell->GetId(),
                peerId,
                node ? node->GetNode()->GetDefaultAddress() : "None");
        }

        if (node) {
            AddCell(node, cell, peerId);
        }
    }

    virtual void RevokePeer(const TTabletCell* cell, int peerId) override
    {
        LazyInitNodes();

        const auto& descriptor = cell->Peers()[peerId].Descriptor;

        if (Provider_->IsVerboseLoggingEnabled()) {
            auto* node = cell->Peers()[peerId].Node;
            LOG_DEBUG("Tablet tracker revoking peer (CellId: %v, PeerId: %v, Node: %v, DescriptorAddress: %v)",
                cell->GetId(),
                peerId,
                node ? node->GetDefaultAddress() : "None",
                descriptor.GetDefaultAddress());
        }

        const auto* node = PeerTracker_.FindPeer(cell, peerId);
        if (node) {
            BannedPeerTracker_.AddPeer(cell, peerId, node);
            PeerTracker_.RemovePeer(cell, peerId, node);
            if (auto it = NodeToIndex_.find(node)) {
                Nodes_[it->second].RemoveCell(cell);
            }
        }

        TabletCellMoveDescriptors_.emplace_back(cell, peerId, node, nullptr);
    }

    virtual std::vector<TTabletCellMoveDescriptor> GetTabletCellMoveDescriptors() override
    {
        if (Provider_->IsBalancingRequired()) {
            LazyInitNodes();
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            LOG_DEBUG("Tablet cells distribution before balancing: %v",
                StateToString());
        }

        for (const auto& pair : Provider_->TabletCellBundles()) {
            RebalanceBundle(pair.second);
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            LOG_DEBUG("Tablet cells distribution after balancing: %v",
                StateToString());
        }

        if (Provider_->IsVerboseLoggingEnabled()) {
            LOG_DEBUG("Tablet cell balancer request moves (before filter): %v",
                MakeFormattableRange(TabletCellMoveDescriptors_, [] (TStringBuilder* builder, const TTabletCellMoveDescriptor& action) {
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        action.Cell->GetId(),
                        action.PeerId,
                        action.Source ? action.Source->GetDefaultAddress() : "nullptr",
                        action.Target ? action.Target->GetDefaultAddress() : "nullptr");
                }));
        }

        FilterActions();

        if (Provider_->IsVerboseLoggingEnabled()) {
            LOG_DEBUG("Tablet cell balancer request moves: %v",
                MakeFormattableRange(TabletCellMoveDescriptors_, [] (TStringBuilder* builder, const TTabletCellMoveDescriptor& action) {
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        action.Cell->GetId(),
                        action.PeerId,
                        action.Source ? action.Source->GetDefaultAddress() : "nullptr",
                        action.Target ? action.Target->GetDefaultAddress() : "nullptr");
                }));
        }

        return std::move(TabletCellMoveDescriptors_);
    }

private:
    class TPeerTracker
    {
    public:
        void AddPeer(const TTabletCell* cell, int peerId, const TNode* peer)
        {
            Y_ASSERT(!IsPeer(cell, peer));

            auto& peers = Peers_[cell];
            if (peers.size() <= peerId) {
                peers.resize(peerId + 1, nullptr);
            }

            Y_ASSERT(peers[peerId] == nullptr);
            peers[peerId] = peer;
        }

        const TNode* FindPeer(const TTabletCell* cell, int peerId)
        {
            if (auto it = Peers_.find(cell)) {
                const auto& peers = it->second;
                if (peerId < peers.size()) {
                    return peers[peerId];
                }
            }

            return nullptr;
        }

        void RemovePeer(const TTabletCell* cell, int peerId, const TNode* peer)
        {
            Y_ASSERT(IsPeer(cell, peer));

            auto& peers = Peers_[cell];
            Y_ASSERT(peers[peerId] == peer);
            peers[peerId] = nullptr;
        }

        int MoveCell(const TTabletCell* cell, const TNode* src, const TNode* dst)
        {
            Y_ASSERT(IsPeer(cell, src));

            auto& peers = Peers_[cell];
            for (int peerId = 0; peerId < peers.size(); ++peerId) {
                if (peers[peerId] == src) {
                    peers[peerId] = dst;
                    return peerId;
                }
            }

            Y_UNREACHABLE();
        }

        bool IsPeer(const TTabletCell* cell, const TNode* node) const
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
        THashMap<const TTabletCell*, SmallVector<const TNode*, TypicalPeerCount>> Peers_;
    };

    const ITabletCellBalancerProviderPtr Provider_;

    bool Initialized_ = false;
    std::vector<TNodeHolder> Nodes_;
    THashMap<const TNode*, int> NodeToIndex_;
    TPeerTracker PeerTracker_;
    TPeerTracker BannedPeerTracker_;
    THashMap<const TTabletCellBundle*, std::vector<int>> FreeNodes_;

    std::vector<TTabletCellMoveDescriptor> TabletCellMoveDescriptors_;

    TString StateToString()
    {
        return Format("%v", MakeFormattableRange(Nodes_, [] (TStringBuilder* builder, const TNodeHolder& node) {
            builder->AppendFormat("<%v: %v>",
                node.GetNode()->GetDefaultAddress(),
                MakeFormattableRange(node.GetSlots(), [] (TStringBuilder* builder, const std::pair<const TTabletCell*, int>& pair) {
                    const auto* cell = pair.first;
                    int peerId = pair.second;
                    builder->AppendFormat("<%v,%v,%v>", cell->GetCellBundle()->GetName(), cell->GetId(), peerId);
                }));
            }));
    }

    void LazyInitNodes()
    {
        if (Initialized_) {
            return;
        }

        Initialized_ = true;

        Nodes_ = Provider_->GetNodes();

        for (int nodeIndex = 0; nodeIndex < Nodes_.size(); ++nodeIndex) {
            const auto& node = Nodes_[nodeIndex];
            NodeToIndex_[node.GetNode()] = nodeIndex;

            if (node.GetTotalSlots() > node.GetSlots().size()) {
                for (const auto& pair : Provider_->TabletCellBundles()) {
                    const auto* bundle = pair.second;
                    if (!IsObjectAlive(bundle)) {
                        continue;
                    }
                    if (Provider_->IsPossibleHost(node.GetNode(), bundle)) {
                        FreeNodes_[pair.second].push_back(nodeIndex);
                    }
                }
            }

            for (const auto& pair : node.GetSlots()) {
                const auto* cell = pair.first;
                int peerId = pair.second;
                PeerTracker_.AddPeer(cell, peerId, node.GetNode());
            }
        }
    }

    void FilterActions()
    {
        std::stable_sort(TabletCellMoveDescriptors_.begin(), TabletCellMoveDescriptors_.end());

        int last = -1;
        for (int index = 0; index < TabletCellMoveDescriptors_.size() ; ++index) {
            if (last < 0 || TabletCellMoveDescriptors_[last] != TabletCellMoveDescriptors_[index]) {
                if (last >= 0 && TabletCellMoveDescriptors_[last].Source == TabletCellMoveDescriptors_[last].Target && TabletCellMoveDescriptors_[last].Target) {
                    --last;
                }

                ++last;
                if (last != index) {
                    TabletCellMoveDescriptors_[last] = TabletCellMoveDescriptors_[index];
                }
            }
            if (TabletCellMoveDescriptors_[last] == TabletCellMoveDescriptors_[index]) {
                TabletCellMoveDescriptors_[last].Target = TabletCellMoveDescriptors_[index].Target;
            }
        }
        TabletCellMoveDescriptors_.resize(last + 1);
    }

    TNodeHolder* TryAllocateNode(const TTabletCell* cell)
    {
        auto it = FreeNodes_.find(cell->GetCellBundle());
        if (it == FreeNodes_.end()) {
            return nullptr;
        }

        auto& queue = it->second;
        for (int index = 0; index < queue.size(); ++index) {
            auto nodeIndex = queue[index];
            YCHECK(nodeIndex < Nodes_.size());
            auto* node = &Nodes_[nodeIndex];
            if (node->GetTotalSlots() == node->GetSlots().size()) {
                std::swap(queue[index], queue.back());
                queue.pop_back();
            } else if (!NodeInPeers(cell, node)) {
                return node;
            }
        }

        return nullptr;
    }

    void AddCell(TNodeHolder* dstNode, const TTabletCell* cell, int peerId)
    {
        dstNode->InsertCell(std::make_pair(cell, peerId));
        PeerTracker_.AddPeer(cell, peerId, dstNode->GetNode());
        TabletCellMoveDescriptors_.emplace_back(cell, peerId, nullptr, dstNode->GetNode());
    }

    void MoveCell(TNodeHolder* srcNode, int srcIndex, TNodeHolder* dstNode)
    {
        auto srcCell = srcNode->ExtractCell(srcIndex);
        dstNode->InsertCell(srcCell);
        // TODO(savrus) use peerId form ExtractCell.
        int srcPeerId = PeerTracker_.MoveCell(srcCell.first, srcNode->GetNode(), dstNode->GetNode());
        TabletCellMoveDescriptors_.emplace_back(srcCell.first, srcPeerId, srcNode->GetNode(), dstNode->GetNode());
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
        TabletCellMoveDescriptors_.emplace_back(srcCell.first, srcPeerId, srcNode->GetNode(), dstNode->GetNode());
        TabletCellMoveDescriptors_.emplace_back(dstCell.first, dstPeerId, dstNode->GetNode(), srcNode->GetNode());
    }

    bool NodeInPeers(const TTabletCell* cell, const TNodeHolder* node)
    {
        return PeerTracker_.IsPeer(cell, node->GetNode()) ||
            BannedPeerTracker_.IsPeer(cell, node->GetNode());
    };

    void SmoothNodes(TNodeHolder* srcNode, TNodeHolder* dstNode, const TTabletCellBundle* bundle, int limit)
    {
        if (srcNode->GetCellCount(bundle) < dstNode->GetCellCount(bundle)) {
            std::swap(srcNode, dstNode);
        }

        int srcIndex = 0;
        int dstIndex = 0;
        while (srcIndex < srcNode->GetSlots().size() &&
            dstIndex < dstNode->GetTotalSlots() &&
            srcNode->GetCellCount(bundle) != limit &&
            dstNode->GetCellCount(bundle) != limit)
        {
            auto* srcCell = srcNode->GetSlots()[srcIndex].first;
            if (srcCell->GetCellBundle() != bundle ||
                NodeInPeers(srcCell, dstNode))
            {
                ++srcIndex;
                continue;
            }

            if (dstNode->GetTotalSlots() > dstNode->GetSlots().size()) {
                MoveCell(srcNode, srcIndex, dstNode);
                continue;
            }

            const auto* dstCell = dstNode->GetSlots()[dstIndex].first;
            const auto* dstBundle = dstCell->GetCellBundle();
            if (dstBundle == bundle ||
                NodeInPeers(dstCell, srcNode) ||
                srcNode->GetCellCount(dstBundle) >= dstNode->GetCellCount(dstBundle) ||
                !Provider_->IsPossibleHost(srcNode->GetNode(), dstBundle))
            {
                ++dstIndex;
                continue;
            }

            ExchangeCells(srcNode, srcIndex, dstNode, dstIndex);
        }
    }

    void RebalanceBundle(const TTabletCellBundle* bundle)
    {
        const auto& tagFilter = bundle->NodeTagFilter();
        std::vector<TNodeHolder*> nodes;
        for (auto& node : Nodes_) {
            if (tagFilter.IsSatisfiedBy(node.GetNode()->Tags())) {
                nodes.push_back(&node);
            }
        }

        if (nodes.empty()) {
            return;
        }

        auto smooth = [&](std::vector<TNodeHolder*>& candidates, int limit, auto filter) {
            for (auto* srcNode : nodes) {
                if (!filter(srcNode)) {
                    continue;
                }

                int candidateIndex = 0;
                while (candidateIndex < candidates.size()) {
                    if (srcNode->GetCellCount(bundle) == limit) {
                        break;
                    }
                    auto* dstNode = candidates[candidateIndex];
                    SmoothNodes(srcNode, dstNode, bundle, limit);
                    if (dstNode->GetCellCount(bundle) == limit) {
                        candidates[candidateIndex] = candidates[candidates.size() - 1];
                        candidates.pop_back();
                    } else {
                        ++candidateIndex;
                    }
                }
            }
        };

        auto ceil = DivCeil<i64>(bundle->TabletCells().size(), nodes.size());
        auto floor = bundle->TabletCells().size() / nodes.size();

        auto aboveCeil = std::count_if(nodes.begin(), nodes.end(), [&] (const auto* node) {
            return node->GetCellCount(bundle) > ceil;
        });
        auto belowFloor = std::count_if(nodes.begin(), nodes.end(), [&] (const auto* node) {
            return node->GetCellCount(bundle) < floor;
        });

        if (Provider_->IsVerboseLoggingEnabled() && (aboveCeil > 0 || belowFloor > 0)) {
            LOG_DEBUG("Tablet cell balancer need to smooth bundle (Bundle: %v, Ceil: %v, Floor: %v, AboveCeilCount: %v, BelowFloorCount: %v)",
                bundle->GetName(),
                ceil,
                floor,
                aboveCeil,
                belowFloor);
        }

        std::vector<TNodeHolder*> candidates;
        std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(candidates), [&] (const auto* node) {
            return node->GetCellCount(bundle) < ceil;
        });
        smooth(candidates, ceil, [&] (const auto* node) {
            return node->GetCellCount(bundle) > ceil;
        });

        candidates.clear();
        std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(candidates), [&] (const auto* node) {
            return node->GetCellCount(bundle) > floor;
        });
        smooth(candidates, floor, [&] (const auto* node) {
            return node->GetCellCount(bundle) < floor;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ITabletCellBalancer> CreateTabletCellBalancer(
    ITabletCellBalancerProviderPtr provider)
{
    return std::make_unique<TTabletCellBalancer>(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

