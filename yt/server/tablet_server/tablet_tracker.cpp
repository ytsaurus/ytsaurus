#include "tablet_tracker.h"
#include "private.h"
#include "config.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"

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

using NTabletClient::TypicalTabletSlotCount;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    class THostilityChecker;
    struct TAction;
    struct ICandidateTracker;
    class TUnbalancedCandidateTracker;
    class TBalancedCandidateTracker;

    using TPeerSet = SmallSet<TString, TypicalPeerCount>;

    const TTabletManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;
    const NProfiling::TProfiler Profiler;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    TNullable<bool> LastEnabled_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    bool IsEnabled();
    void ScanCells();

    void ScheduleLeaderReassignment(TTabletCell* cell);
    void SchedulePeerAssignment(TTabletCell* cell, ICandidateTracker* candidates);
    void SchedulePeerRevocation(TTabletCell* cell, ICandidateTracker* candidates);

    bool IsFailed(const TTabletCell* cell, TPeerId peerId, TDuration timeout);
    static bool IsGood(const NNodeTrackerServer::TNode* node);
    static int FindGoodPeer(const TTabletCell* cell);
};

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TImpl::THostilityChecker
{
public:
    explicit THostilityChecker(const TNode* node)
        : Node_(node)
    { }

    bool IsPossibleHost(const TTabletCellBundle* bundle)
    {
        return CheckBundleCache(bundle);
    }

private:
    const TNode* Node_;
    THashMap<const TTabletCellBundle*, bool> BundleCache_;
    THashMap<TString, bool> FormulaCache_;

    bool CheckBundleCache(const TTabletCellBundle* bundle)
    {
        if (auto it = BundleCache_.find(bundle)) {
            return it->second;
        }

        return BundleCache_.insert(std::make_pair(bundle, CheckFormulaCache(bundle))).first->second;
    }

    bool CheckFormulaCache(const TTabletCellBundle* bundle)
    {
        const auto formula = bundle->NodeTagFilter().GetFormula();
        if (auto it = FormulaCache_.find(formula)) {
            return it->second;
        }

        return FormulaCache_.insert(std::make_pair(formula, CheckNode(bundle))).first->second;
    }

    bool CheckNode(const TTabletCellBundle* bundle)
    {
        return bundle->NodeTagFilter().IsSatisfiedBy(Node_->Tags());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletTracker::TImpl::TAction
{
    const TTabletCell* Cell;
    int PeerId;
    const TNode* Source;
    const TNode* Target;

    TAction() = default;

    //TODO(savrus) really?
    TAction(const TTabletCell* cell, int peerId, const TNode* source, const TNode* target)
        : Cell(cell)
        , PeerId(peerId)
        , Source(source)
        , Target(target)
    { }

    bool operator<(const TAction& other) const
    {
        return Cell == other.Cell
            ? PeerId < other.PeerId
            : Cell < other.Cell;
    }
};

struct TTabletTracker::TImpl::ICandidateTracker
{
    virtual ~ICandidateTracker() = default;

    virtual void AssignPeer(const TTabletCell* cell, int peerId, TPeerSet* forbiddenAddresses) = 0;
    virtual void RevokePeer(const TTabletCell* cell, int peerId) = 0;
    virtual std::vector<TAction> GetActions() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TImpl::TUnbalancedCandidateTracker
    : public TTabletTracker::TImpl::ICandidateTracker
{
public:
    explicit TUnbalancedCandidateTracker(const NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual void AssignPeer(const TTabletCell* cell, int peerId, TPeerSet* forbiddenAddresses) override
    {
        if (const auto* node = TryAllocate(cell, *forbiddenAddresses)) {
            forbiddenAddresses->insert(node->GetDefaultAddress());
            Actions_.emplace_back(cell, peerId, nullptr, node);
        }
    }

    virtual void RevokePeer(const TTabletCell* cell, int peerId) override
    {
        if (auto* node = cell->Peers()[peerId].Node) {
            Actions_.emplace_back(cell, peerId, node, nullptr);
        }
    }

    virtual std::vector<TAction> GetActions() override
    {
        std::sort(Actions_.begin(), Actions_.end());
        return std::move(Actions_);
    }

private:
    // Key is pair (nubmer of slots assigned to this bunde, minus number of spare slots),
    // value is node index in Nodes_ array.
    using TQueueType = std::multimap<std::pair<int,int>, int>;

    struct TNodeData
    {
        const TNode* Node;
        THashMap<const TTabletCellBundle*, TQueueType::iterator> Iterators;
    };

    const NCellMaster::TBootstrap* const Bootstrap_;

    std::vector<TAction> Actions_;

    bool Initialized_ = false;
    std::vector<TNodeData> Nodes_;
    THashMap<const TTabletCellBundle*, TQueueType> Queues_;

    const TNode* TryAllocate(
        const TTabletCell* cell,
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

    void AddNode(const TNode* node)
    {
        if (!IsGood(node)) {
            return;
        }

        TNodeData data{node};
        int index = Nodes_.size();
        int spare = node->GetTotalTabletSlots();
        THashMap<TTabletCellBundle*, int> cellCount;

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        if (const auto* cells = tabletManager->FindAssignedTabletCells(node->GetDefaultAddress())) {
            for (auto* cell : *cells) {
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

    void ChargeNode(int index, const TTabletCell* cell)
    {
        auto& node = Nodes_[index];
        SmallVector<const TTabletCellBundle*, TypicalTabletSlotCount> remove;

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

class TTabletTracker::TImpl::TBalancedCandidateTracker
    : public TTabletTracker::TImpl::ICandidateTracker
{
public:
    TBalancedCandidateTracker(const TBootstrap* bootstrap, bool verboseLogging)
        : Bootstrap_(bootstrap)
        , VerboseLogging_(verboseLogging)
    {
        InitNodes();
    }

    virtual void AssignPeer(const TTabletCell* cell, int peerId, TPeerSet* /*forbiddenAddresses*/) override
    {
        if (auto* node = TryAllocateNode(cell)) {
            AddCell(node, cell, peerId);
        }
    }

    virtual void RevokePeer(const TTabletCell* cell, int peerId) override
    {
        if (auto* node = cell->Peers()[peerId].Node) {
            //TODO(savrus) assign new peer
            BannedPeerTracker_.AddPeer(cell, peerId, node);
            PeerTracker_.RemovePeer(cell, peerId, node);
            Actions_.emplace_back(cell, peerId, node, nullptr);
        }
    }

    virtual std::vector<TAction> GetActions() override
    {
        if (VerboseLogging_) {
            LOG_DEBUG("Tablet cells distribution before balancing: %v",
                StateToString());
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            RebalanceBundle(pair.second);
        }

        if (VerboseLogging_) {
            LOG_DEBUG("Tablet cells distribution after balancing: %v",
                StateToString());
        }

        FilterActions();

        if (VerboseLogging_) {
            LOG_DEBUG("Tablet cell balancer request moves: %v",
                MakeFormattableRange(Actions_, [] (TStringBuilder* builder, const TAction& action) {
                    builder->AppendFormat("<%v,%v,%v,%v>",
                        action.Cell->GetId(),
                        action.PeerId,
                        action.Source ? action.Source->GetDefaultAddress() : "nullptr",
                        action.Target ? action.Target->GetDefaultAddress() : "nullptr");
                }));
        }

        return std::move(Actions_);
    }

private:
    class TNodeHolder
    {
    public:
        TNodeHolder(const TNode* node, int totalSlots, const TTabletCellSet& slots)
            : Node_(node)
            , TotalSlots_(totalSlots)
            , Slots_(slots)
        {
            CountCells();
        }

        const TNode* GetNode() const
        {
            return Node_;
        }

        int GetTotalSlots() const
        {
            return TotalSlots_;
        }

        const TTabletCellSet& GetSlots() const
        {
            return Slots_;
        }

        const TTabletCell* ExtractCell(int cellIndex)
        {
            Y_ASSERT(cellIndex < Slots_.size());

            auto* cell = Slots_[cellIndex];
            Slots_[cellIndex] = Slots_.back();
            Slots_.pop_back();
            --CellCount_[cell->GetCellBundle()];
            return cell;
        }

        void InsertCell(const TTabletCell* cell)
        {
            Slots_.push_back(cell);
            ++CellCount_[cell->GetCellBundle()];
        }

        int GetCellCount(const TTabletCellBundle* bundle) const
        {
            auto it = CellCount_.find(bundle);
            return it != CellCount_.end() ? it->second : 0;
        }

    private:
        const TNode* Node_;
        const int TotalSlots_;
        TTabletCellSet Slots_;
        THashMap<const TTabletCellBundle*, int> CellCount_;

        void CountCells()
        {
            for (const auto* cell : Slots_) {
                CellCount_[cell->GetCellBundle()] += 1;
            }
        }
    };

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

    const NCellMaster::TBootstrap* const Bootstrap_;
    const bool VerboseLogging_;

    std::vector<TNodeHolder> Nodes_;
    TPeerTracker PeerTracker_;
    TPeerTracker BannedPeerTracker_;
    THashMap<const TTabletCellBundle*, std::vector<int>> FreeNodes_;

    std::vector<TAction> Actions_;

    TString StateToString()
    {
        return Format("%v", MakeFormattableRange(Nodes_, [] (TStringBuilder* builder, const TNodeHolder& node) {
            builder->AppendFormat("<%v: %v>",
                node.GetNode()->GetDefaultAddress(),
                MakeFormattableRange(node.GetSlots(), [] (TStringBuilder* builder, const TTabletCell* cell) {
                    builder->AppendFormat("<%v,%v>", cell->GetCellBundle()->GetName(), cell->GetId());
                }));
            }));
    }

    void InitNodes()
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto isGood = [&] (const auto* node) {
            return IsGood(node) && node->GetTotalTabletSlots() > 0;
        };

        int nodeCount = 0;
        for (const auto& pair : nodeTracker->Nodes()) {
            if (isGood(pair.second)) {
                ++nodeCount;
            }
        }

        Nodes_.reserve(nodeCount);
        for (const auto& pair : nodeTracker->Nodes()) {
            const auto* node = pair.second;
            if (!isGood(node)) {
                continue;
            }

            const auto* cells = tabletManager->FindAssignedTabletCells(node->GetDefaultAddress());

            Nodes_.emplace_back(
                node,
                node->GetTotalTabletSlots(),
                cells ? *cells : TTabletCellSet());

            if (!cells || node->GetTotalTabletSlots() > cells->size()) {
                THostilityChecker hostility(node);
                for (const auto& pair : tabletManager->TabletCellBundles()) {
                    if (hostility.IsPossibleHost(pair.second)) {
                        FreeNodes_[pair.second].push_back(Nodes_.size() - 1);
                    }
                }
            }
        }

        for (const auto& pair : tabletManager->TabletCells()) {
            const auto* cell = pair.second;
            for (int peerId = 0; peerId < cell->Peers().size(); ++peerId) {
                const auto& descriptor = cell->Peers()[peerId].Descriptor;
                if (descriptor.IsNull()) {
                    continue;
                }

                const auto* node = nodeTracker->GetNodeByAddress(descriptor.GetDefaultAddress());
                PeerTracker_.AddPeer(cell, peerId, node);
            }
        }
    }

    void FilterActions()
    {
        std::stable_sort(Actions_.begin(), Actions_.end());

        int last = -1;
        for (int index = 0; index < Actions_.size() ; ++index) {
            if (last < 0 || Actions_[last].Cell != Actions_[index].Cell) {
                ++last;
                if (last != index) {
                    Actions_[last] = Actions_[index];
                }
            }
            if (Actions_[last].Cell == Actions_[index].Cell) {
                if (Actions_[last].Source == Actions_[index].Target) {
                    --last;
                } else {
                    Actions_[last].Target = Actions_[index].Target;
                }
            }
        }
        Actions_.resize(last + 1);
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
        dstNode->InsertCell(cell);
        PeerTracker_.AddPeer(cell, peerId, dstNode->GetNode());
        Actions_.emplace_back(cell, peerId, nullptr, dstNode->GetNode());
    }

    void MoveCell(TNodeHolder* srcNode, int srcIndex, TNodeHolder* dstNode)
    {
        const auto* srcCell = srcNode->ExtractCell(srcIndex);
        dstNode->InsertCell(srcCell);
        int srcPeerId = PeerTracker_.MoveCell(srcCell, srcNode->GetNode(), dstNode->GetNode());
        Actions_.emplace_back(srcCell, srcPeerId, srcNode->GetNode(), dstNode->GetNode());
    }

    void ExchangeCells(TNodeHolder* srcNode, int srcIndex, TNodeHolder* dstNode, int dstIndex)
    {
        const auto* srcCell = srcNode->ExtractCell(srcIndex);
        const auto* dstCell = dstNode->ExtractCell(dstIndex);
        srcNode->InsertCell(dstCell);
        dstNode->InsertCell(srcCell);
        int srcPeerId = PeerTracker_.MoveCell(srcCell, srcNode->GetNode(), dstNode->GetNode());
        int dstPeerId = PeerTracker_.MoveCell(dstCell, dstNode->GetNode(), srcNode->GetNode());
        Actions_.emplace_back(srcCell, srcPeerId, srcNode->GetNode(), dstNode->GetNode());
        Actions_.emplace_back(dstCell, dstPeerId, dstNode->GetNode(), srcNode->GetNode());
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

        THostilityChecker hostility(srcNode->GetNode());
        int srcIndex = 0;
        int dstIndex = 0;
        while (srcIndex < srcNode->GetSlots().size() &&
            dstIndex < dstNode->GetTotalSlots() &&
            srcNode->GetCellCount(bundle) != limit &&
            dstNode->GetCellCount(bundle) != limit)
        {
            auto* srcCell = srcNode->GetSlots()[srcIndex];
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

            const auto* dstCell = dstNode->GetSlots()[dstIndex];
            const auto* dstBundle = dstCell->GetCellBundle();
            if (dstBundle == bundle ||
                NodeInPeers(dstCell, srcNode) ||
                srcNode->GetCellCount(dstBundle) >= dstNode->GetCellCount(dstBundle) ||
                !hostility.IsPossibleHost(dstBundle))
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

        if (VerboseLogging_ && (aboveCeil > 0 || belowFloor > 0)) {
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

TTabletTracker::TImpl::TImpl(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , Profiler("/tablet_server/cell_balancer")
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
}

void TTabletTracker::TImpl::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    YCHECK(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletTracker),
        BIND(&TTabletTracker::TImpl::ScanCells, MakeWeak(this)),
        Config_->CellScanPeriod);
    PeriodicExecutor_->Start();
}

void TTabletTracker::TImpl::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }
}

bool TTabletTracker::TImpl::IsEnabled()
{
    // This method also logs state changes.

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    int needOnline = Config_->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();

    if (gotOnline < needOnline) {
        if (!LastEnabled_ || *LastEnabled_) {
            LOG_INFO("Tablet tracker disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
            LastEnabled_ = false;
        }
        return false;
    }

    if (!LastEnabled_ || !*LastEnabled_) {
        LOG_INFO("Tablet tracker enabled");
        LastEnabled_ = true;
    }

    return true;
}

void TTabletTracker::TImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsEnabled())
        return;

    PROFILE_TIMING("/scan_cells") {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletCellBalancer;
        std::unique_ptr<ICandidateTracker> tracker;
        if (config->EnableTabletCellBalancer) {
            tracker = std::make_unique<TBalancedCandidateTracker>(Bootstrap_, config->EnableVerboseLogging);
        } else {
            tracker = std::make_unique<TUnbalancedCandidateTracker>(Bootstrap_);
        }

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& tabletManger = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManger->TabletCells()) {
            auto* cell = pair.second;
            if (!IsObjectAlive(cell))
                continue;

            ScheduleLeaderReassignment(cell);
            SchedulePeerAssignment(cell, tracker.get());
            SchedulePeerRevocation(cell, tracker.get());
        }

        auto actions  = tracker->GetActions();

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

            for (const auto& action : actions) {
                if (action.Source) {
                    commit(action.Cell);
                    request.add_peer_ids(action.PeerId);
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

            for (const auto& action : actions) {
                if (action.Target) {
                    commit(action.Cell);
                    auto* peerInfo = request.add_peer_infos();
                    peerInfo->set_peer_id(action.PeerId);
                    ToProto(peerInfo->mutable_node_descriptor(), action.Target->GetDescriptor());
                }
            }

            commit(nullptr);
        }
    }
}

void TTabletTracker::TImpl::ScheduleLeaderReassignment(TTabletCell* cell)
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

void TTabletTracker::TImpl::SchedulePeerAssignment(TTabletCell* cell, ICandidateTracker* tracker)
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

    SmallSet<TString, TypicalPeerCount> forbiddenAddresses;
    for (const auto& peer : peers) {
        if (!peer.Descriptor.IsNull()) {
            forbiddenAddresses.insert(peer.Descriptor.GetDefaultAddress());
        }
    }

    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        if (peers[id].Descriptor.IsNull()) {
            tracker->AssignPeer(cell, id, &forbiddenAddresses);
        }
    }
}

void TTabletTracker::TImpl::SchedulePeerRevocation(TTabletCell* cell, ICandidateTracker* tracker)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime_ + Config_->PeerRevocationTimeout) {
        return;
    }

    for (TPeerId peerId = 0; peerId < cell->Peers().size(); ++peerId) {
        if (IsFailed(cell, peerId, Config_->PeerRevocationTimeout)) {
            tracker->RevokePeer(cell, peerId);
        }
    }
}

bool TTabletTracker::TImpl::IsFailed(const TTabletCell* cell, TPeerId peerId, TDuration timeout)
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

bool TTabletTracker::TImpl::IsGood(const TNode* node)
{
    if (!IsObjectAlive(node)) {
        return false;
    }

    if (node->GetAggregatedState() != ENodeState::Online) {
        return false;
    }

    if (node->GetBanned()) {
        return false;
    }

    if (node->GetDecommissioned()) {
        return false;
    }

    if (node->GetDisableTabletCells()) {
        return false;
    }

    return true;
}

int TTabletTracker::TImpl::FindGoodPeer(const TTabletCell* cell)
{
    for (TPeerId id = 0; id < static_cast<int>(cell->Peers().size()); ++id) {
        const auto& peer = cell->Peers()[id];
        if (IsGood(peer.Node)) {
            return id;
        }
    }
    return InvalidPeerId;
}

////////////////////////////////////////////////////////////////////////////////

TTabletTracker::TTabletTracker(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

void TTabletTracker::Start()
{
    Impl_->Start();
}

void TTabletTracker::Stop()
{
    Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
