#include "chunk_placement.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/server/master/node_tracker_server/rack.h>

#include <yt/server/master/object_server/object.h>

#include <yt/core/misc/small_set.h>

#include <util/random/random.h>

#include <array>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement::TTargetCollector
{
public:
    TTargetCollector(
        const TMedium* medium,
        const TChunk* chunk,
        int maxReplicasPerRack,
        bool allowMultipleReplicasPerNode,
        const TNodeList* forbiddenNodes)
        : MaxReplicasPerRack_(maxReplicasPerRack)
        , AllowMultipleReplicasPerNode_(allowMultipleReplicasPerNode)
    {
        if (forbiddenNodes) {
            ForbiddenNodes_ = *forbiddenNodes;
        }

        int mediumIndex = medium->GetIndex();
        for (auto replica : chunk->StoredReplicas()) {
            if (replica.GetMediumIndex() == mediumIndex) {
                auto* node = replica.GetPtr();
                ForbiddenNodes_.push_back(node);
                if (!replica.GetPtr()->GetDecommissioned()) {
                    IncreaseRackUsage(node);
                }
            }
        }

        std::sort(ForbiddenNodes_.begin(), ForbiddenNodes_.end());
    }

    bool CheckNode(TNode* node, bool enableRackAwareness) const
    {
        if (std::find(ForbiddenNodes_.begin(), ForbiddenNodes_.end(), node) != ForbiddenNodes_.end()) {
            return false;
        }

        if (enableRackAwareness) {
            const auto* rack = node->GetRack();
            if (rack && PerRackCounters_[rack->GetIndex()] >= MaxReplicasPerRack_) {
                return false;
            }
        }

        return true;
    }

    void AddNode(TNode* node)
    {
        IncreaseRackUsage(node);
        AddedNodes_.push_back(node);
        if (!AllowMultipleReplicasPerNode_) {
            ForbiddenNodes_.push_back(node);
        }
    }

    const TNodeList& GetAddedNodes() const
    {
        return AddedNodes_;
    }

private:
    const int MaxReplicasPerRack_;
    const bool AllowMultipleReplicasPerNode_;

    std::array<i8, RackIndexBound> PerRackCounters_{};
    TNodeList ForbiddenNodes_;
    TNodeList AddedNodes_;

private:
    void IncreaseRackUsage(TNode* node)
    {
        const auto* rack = node->GetRack();
        if (rack) {
            ++PerRackCounters_[rack->GetIndex()];
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YT_VERIFY(Config_);
    YT_VERIFY(Bootstrap_);

    for (auto [_, node] : Bootstrap_->GetNodeTracker()->Nodes()) {
        OnNodeUpdated(node);
    }
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    if (node->GetLocalState() != ENodeState::Online) {
        return;
    }

    InsertToLoadFactorMaps(node);
    InsertToFillFactorMaps(node);
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    RemoveFromLoadFactorMaps(node);
    RemoveFromFillFactorMaps(node);
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    node->ClearSessionHints();

    OnNodeUnregistered(node);
    OnNodeRegistered(node);
}

void TChunkPlacement::OnNodeDataCenterChanged(TNode* node, const TDataCenter* oldDataCenter)
{
    RemoveFromLoadFactorMaps(node, oldDataCenter);
    RemoveFromFillFactorMaps(node, oldDataCenter);

    OnNodeRegistered(node);
}

void TChunkPlacement::OnNodeDisposed(TNode* node)
{
    for (const auto& item : node->LoadFactorIterators()) {
        YT_VERIFY(!item.second);
    }
    for (const auto& item : node->FillFactorIterators()) {
        YT_VERIFY(!item.second);
    }
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const std::optional<TString>& preferredHostName,
    ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        desiredCount,
        minCount,
        sessionType == ESessionType::Replication,
        replicationFactorOverride,
        nullptr,
        forbiddenNodes,
        preferredHostName);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

void TChunkPlacement::InsertToFillFactorMaps(TNode* node)
{
    RemoveFromFillFactorMaps(node);

    auto* dataCenter = node->GetDataCenter();

    // Iterate through IOWeights because IsValidBalancingTargetToInsert check if IOWeights contains medium
    for (const auto& [mediumIndex, _] : node->IOWeights()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!IsValidBalancingTargetToInsert(medium, node)) {
            continue;
        }

        auto fillFactor = node->GetFillFactor(mediumIndex);
        if (!fillFactor) {
            continue;
        }

        auto it = DomainToFillFactorToNode_[TPlacementDomain{dataCenter, medium}].emplace(*fillFactor, node);
        node->SetFillFactorIterator(mediumIndex, it);
    }
}

void TChunkPlacement::RemoveFromFillFactorMaps(
    TNode* node,
    std::optional<const TDataCenter*> overrideDataCenter)
{
    auto* dataCenter = overrideDataCenter ? *overrideDataCenter : node->GetDataCenter();

    for (const auto& [mediumIndex, factorMapIter] : node->FillFactorIterators()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!factorMapIter || !medium) {
            continue;
        }

        auto domainToFactorMapIter = DomainToFillFactorToNode_.find(TPlacementDomain{dataCenter, medium});
        YT_VERIFY(domainToFactorMapIter != DomainToFillFactorToNode_.end());

        auto& factorMap = domainToFactorMapIter->second;
        factorMap.erase(*factorMapIter);
        node->SetFillFactorIterator(mediumIndex, std::nullopt);

        if (factorMap.empty()) {
            DomainToFillFactorToNode_.erase(domainToFactorMapIter);
        }
    }
}

void TChunkPlacement::InsertToLoadFactorMaps(TNode* node)
{
    RemoveFromLoadFactorMaps(node);

    auto* dataCenter = node->GetDataCenter();

    // Iterate through IOWeights because IsValidBalancingTargetToInsert check if IOWeights contains medium
    for (const auto& [mediumIndex, _] : node->IOWeights()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!IsValidWriteTargetToInsert(medium, node)) {
            continue;
        }

        auto loadFactor = node->GetLoadFactor(mediumIndex);
        if (!loadFactor) {
            continue;
        }

        auto it = DomainToLoadFactorToNode_[TPlacementDomain{dataCenter, medium}].emplace(*loadFactor, node);
        node->SetLoadFactorIterator(mediumIndex, it);
    }
}

void TChunkPlacement::RemoveFromLoadFactorMaps(
    TNode* node,
    std::optional<const TDataCenter*> overrideDataCenter)
{
    auto* dataCenter = overrideDataCenter ? *overrideDataCenter : node->GetDataCenter();

    for (const auto& [mediumIndex, factorMapIter] : node->LoadFactorIterators()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!factorMapIter || !medium) {
            continue;
        }

        auto domainToFactorMapIter = DomainToLoadFactorToNode_.find(TPlacementDomain{dataCenter, medium});
        YT_VERIFY(domainToFactorMapIter != DomainToLoadFactorToNode_.end());

        auto& factorMap = domainToFactorMapIter->second;
        factorMap.erase(*factorMapIter);
        node->SetLoadFactorIterator(mediumIndex, std::nullopt);

        if (factorMap.empty()) {
            DomainToLoadFactorToNode_.erase(domainToFactorMapIter);
        }
    }
}

TNodeList TChunkPlacement::GetWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    bool forceRackAwareness,
    std::optional<int> replicationFactorOverride,
    const TDataCenterSet* dataCenters,
    const TNodeList* forbiddenNodes,
    const std::optional<TString>& preferredHostName)
{
    if (dataCenters && dataCenters->empty()) {
        return TNodeList();
    }

    PrepareLoadFactorIterator(dataCenters, medium);
    if (!LoadFactorToNodeIterator_.IsValid()) {
        return TNodeList();
    }

    int maxReplicasPerRack = GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride);
    TTargetCollector collector(
        medium,
        chunk,
        maxReplicasPerRack,
        Config_->AllowMultipleErasurePartsPerNode && chunk->IsErasure(),
        forbiddenNodes);

    auto tryAdd = [&] (TNode* node, bool enableRackAwareness) {
        if (!IsValidWriteTargetToAllocate(node, &collector, enableRackAwareness)) {
            return false;
        }
        collector.AddNode(node);
        return true;
    };

    auto hasEnoughTargets = [&] {
        return collector.GetAddedNodes().size() == desiredCount;
    };

    auto tryAddAll = [&] (bool enableRackAwareness) {
        YT_VERIFY(!hasEnoughTargets());

        bool hasProgress = false;
        if (!LoadFactorToNodeIterator_.IsValid()) {
            PrepareLoadFactorIterator(dataCenters, medium);
        }
        for ( ; !hasEnoughTargets() && LoadFactorToNodeIterator_.IsValid(); ++LoadFactorToNodeIterator_) {
            auto* node = LoadFactorToNodeIterator_->second;
            hasProgress |= tryAdd(node, enableRackAwareness);
        }
        return hasProgress;
    };

    if (preferredHostName) {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode &&
            IsValidPreferredWriteTargetToAllocate(preferredNode, medium, dataCenters))
        {
            tryAdd(preferredNode, true);
        }
    }

    if (!hasEnoughTargets()) {
        tryAddAll(true);
    }

    if (!forceRackAwareness) {
        while (!hasEnoughTargets()) {
            if (!tryAddAll(false)) {
                break;
            }
            if (!chunk->IsErasure() || !Config_->AllowMultipleErasurePartsPerNode) {
                break;
            }
        }
    }

    const auto& nodes = collector.GetAddedNodes();
    return nodes.size() < minCount ? TNodeList() : nodes;
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    const TDataCenterSet& dataCenters,
    ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        desiredCount,
        minCount,
        sessionType == ESessionType::Replication,
        replicationFactorOverride,
        &dataCenters);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

TNode* TChunkPlacement::GetRemovalTarget(TChunkPtrWithIndexes chunkWithIndexes)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto mediumIndex = chunkWithIndexes.GetMediumIndex();
    int maxReplicasPerRack = GetMaxReplicasPerRack(mediumIndex, chunk, std::nullopt);

    std::array<i8, RackIndexBound> perRackCounters{};
    for (auto replica : chunk->StoredReplicas()) {
        if (replica.GetMediumIndex() != mediumIndex)
            continue;

        const auto* rack = replica.GetPtr()->GetRack();
        if (rack) {
            ++perRackCounters[rack->GetIndex()];
        }
    }

    // An arbitrary node from a rack with too many replicas.
    TNode* rackWinner = nullptr;
    // A node with the largest fill factor.
    TNode* fillFactorWinner = nullptr;

    for (auto replica : chunk->StoredReplicas()) {
        if ((replica.GetMediumIndex() == mediumIndex) &&
            (chunk->IsRegular() ||
             chunk->IsErasure() && replica.GetReplicaIndex() == replicaIndex ||
             chunk->IsJournal())) // allow removing arbitrary journal replicas
        {
            auto* node = replica.GetPtr();
            if (!IsValidRemovalTarget(node))
                continue;

            const auto* rack = node->GetRack();
            if (rack && perRackCounters[rack->GetIndex()] > maxReplicasPerRack) {
                rackWinner = node;
            }

            auto nodeFillFactor = node->GetFillFactor(mediumIndex);

            if (nodeFillFactor &&
                (!fillFactorWinner ||
                 *nodeFillFactor > *fillFactorWinner->GetFillFactor(mediumIndex)))
            {
                fillFactorWinner = node;
            }
        }
    }

    return rackWinner ? rackWinner : fillFactorWinner;
}

bool TChunkPlacement::HasBalancingTargets(const TDataCenterSet& dataCenters, TMedium* medium, double maxFillFactor)
{
    if (maxFillFactor < 0) {
        return false;
    }

    PrepareFillFactorIterator(&dataCenters, medium);
    if (!FillFactorToNodeIterator_.IsValid()) {
        return false;
    }

    auto* node = FillFactorToNodeIterator_->second;
    auto nodeFillFactor = node->GetFillFactor(medium->GetIndex());
    YT_VERIFY(nodeFillFactor);
    return *nodeFillFactor < maxFillFactor;
}

TNode* TChunkPlacement::AllocateBalancingTarget(
    TMedium* medium,
    TChunk* chunk,
    double maxFillFactor,
    const TDataCenterSet& dataCenters)
{
    auto* target = GetBalancingTarget(medium, &dataCenters, chunk, maxFillFactor);

    if (target) {
        AddSessionHint(target, medium->GetIndex(), ESessionType::Replication);
    }

    return target;
}

TNode* TChunkPlacement::GetBalancingTarget(
    TMedium* medium,
    const TDataCenterSet* dataCenters,
    TChunk* chunk,
    double maxFillFactor)
{
    if (dataCenters && dataCenters->empty()) {
        return nullptr;
    }

    int maxReplicasPerRack = GetMaxReplicasPerRack(medium, chunk, std::nullopt);
    TTargetCollector collector(
        medium,
        chunk,
        maxReplicasPerRack,
        Config_->AllowMultipleErasurePartsPerNode && chunk->IsErasure(),
        nullptr);

    PrepareFillFactorIterator(dataCenters, medium);
    for ( ; FillFactorToNodeIterator_.IsValid(); ++FillFactorToNodeIterator_) {
        auto* node = FillFactorToNodeIterator_->second;
        auto nodeFillFactor = node->GetFillFactor(medium->GetIndex());
        YT_VERIFY(nodeFillFactor);
        if (*nodeFillFactor > maxFillFactor) {
            break;
        }
        if (IsValidBalancingTargetToAllocate(node, &collector, true)) {
            return node;
        }
    }

    return nullptr;
}

bool TChunkPlacement::IsValidWriteTargetToInsert(TMedium* medium, TNode* node)
{
    if (medium->GetCache()) {
        // Direct writing to cache locations is not allowed.
        return false;
    }

    if (!node->IsWriteEnabled(medium->GetIndex())) {
        // Do not write anything to nodes not accepting writes.
        return false;
    }

    return IsValidWriteTargetCore(node);
}

bool TChunkPlacement::IsValidPreferredWriteTargetToAllocate(
    TNode* node,
    TMedium* medium,
    const TDataCenterSet* dataCenters)
{
    if (medium->GetCache()) {
        return false;
    }

    if (!node->IsWriteEnabled(medium->GetIndex())) {
        return false;
    }

    if (dataCenters && dataCenters->count(node->GetDataCenter()) == 0) {
        return false;
    }

    return true;
}

bool TChunkPlacement::IsValidWriteTargetToAllocate(
    TNode* node,
    TTargetCollector* collector,
    bool enableRackAwareness)
{
    // Check node first.
    if (!IsValidWriteTargetCore(node)) {
        return false;
    }

    if (!collector->CheckNode(node, enableRackAwareness)) {
        // The collector does not like this node.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidWriteTargetCore(TNode* node)
{
    if (node->GetLocalState() != ENodeState::Online) {
        // Do not write anything to a node before its first heartbeat or after it is unregistered.
        return false;
    }

    if (node->GetDecommissioned()) {
        // Do not write anything to decommissioned nodes.
        return false;
    }

    if (node->GetEffectiveDisableWriteSessions()) {
        // Do not start new sessions if they are explicitly disabled.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTargetToInsert(TMedium* medium, TNode* node)
{
    // Balancing implies write, after all.
    if (!IsValidWriteTargetToInsert(medium, node)) {
        return false;
    }

    return IsValidBalancingTargetCore(node);
}

bool TChunkPlacement::IsValidBalancingTargetToAllocate(
    TNode* node,
    TTargetCollector* collector,
    bool enableRackAwareness)
{
    // Check node first.
    if (!IsValidBalancingTargetCore(node)) {
        return false;
    }

    // Balancing implies write, after all.
    if (!IsValidWriteTargetToAllocate(node, collector, enableRackAwareness)) {
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTargetCore(TNode* node)
{
    if (node->GetSessionCount(ESessionType::Replication) >= GetDynamicConfig()->MaxReplicationWriteSessions) {
        // Do not write anything to a node with too many write sessions.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidRemovalTarget(TNode* node)
{
    if (node->GetLocalState() != ENodeState::Online) {
        // Do not remove anything from a node before its first heartbeat or after it is unregistered.
        return false;
    }

    return true;
}

std::vector<TChunkPtrWithIndexes> TChunkPlacement::GetBalancingChunks(
    TMedium* medium,
    TNode* node,
    int replicaCount)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto epoch = objectManager->GetCurrentEpoch();

    std::vector<TChunkPtrWithIndexes> result;
    result.reserve(replicaCount);

    // Let's bound the number of iterations somehow.
    // Never consider more chunks than the node has to avoid going into a loop (cf. YT-4258).
    int mediumIndex = medium->GetIndex();
    auto replicas = node->Replicas().find(mediumIndex);
    int iterationCount = std::min(replicaCount * 2, static_cast<int>(replicas == node->Replicas().end() ? 0 : replicas->second.size()));
    for (int index = 0; index < iterationCount; ++index) {
        auto replica = node->PickRandomReplica(mediumIndex);
        YT_ASSERT(replica.GetMediumIndex() == mediumIndex);
        auto* chunk = replica.GetPtr();
        if (!IsObjectAlive(chunk)) {
            break;
        }
        if (static_cast<int>(result.size()) >= replicaCount) {
            break;
        }
        if (!chunk->GetMovable()) {
            continue;
        }
        if (!chunk->IsSealed()) {
            continue;
        }
        if (chunk->GetScanFlag(EChunkScanKind::Refresh, epoch)) {
            continue;
        }
        if (chunk->IsJobScheduled()) {
            continue;
        }
        if (chunk->IsJournal() && replica.GetReplicaIndex() == UnsealedChunkReplicaIndex) {
            continue;
        }
        result.push_back(replica);
    }

    return result;
}

void TChunkPlacement::AddSessionHint(TNode* node, int mediumIndex, ESessionType sessionType)
{
    node->AddSessionHint(mediumIndex, sessionType);

    RemoveFromLoadFactorMaps(node);
    InsertToLoadFactorMaps(node);

    if (node->GetSessionCount(ESessionType::Replication) >= GetDynamicConfig()->MaxReplicationWriteSessions) {
        RemoveFromFillFactorMaps(node);
    }
}

int TChunkPlacement::GetMaxReplicasPerRack(
    const TMedium* medium,
    TChunk* chunk,
    std::optional<int> replicationFactorOverride)
{
    auto result = chunk->GetMaxReplicasPerRack(
        medium->GetIndex(),
        replicationFactorOverride,
        Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry());
    const auto& config = medium->Config();
    result = std::min(result, config->MaxReplicasPerRack);

    switch (chunk->GetType()) {
        case EObjectType::Chunk:         result = std::min(result, config->MaxRegularReplicasPerRack); break;
        case EObjectType::ErasureChunk:  result = std::min(result, config->MaxErasureReplicasPerRack); break;
        case EObjectType::JournalChunk:  result = std::min(result, config->MaxJournalReplicasPerRack); break;
        default:                         YT_ABORT();
    }
    return result;
}

int TChunkPlacement::GetMaxReplicasPerRack(
    int mediumIndex,
    TChunk* chunk,
    std::optional<int> replicationFactorOverride)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
    return GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride);
}

void TChunkPlacement::PrepareFillFactorIterator(const TDataCenterSet* dataCenters, const TMedium* medium)
{
    FillFactorToNodeIterator_.Reset();
    ForEachDataCenter(dataCenters, [&] (const TDataCenter* dataCenter) {
        auto it = DomainToFillFactorToNode_.find(TPlacementDomain{dataCenter, medium});
        if (it != DomainToFillFactorToNode_.end()) {
            FillFactorToNodeIterator_.AddRange(it->second);
        }
    });
}

void TChunkPlacement::PrepareLoadFactorIterator(const TDataCenterSet* dataCenters, const TMedium* medium)
{
    LoadFactorToNodeIterator_.Reset();
    ForEachDataCenter(dataCenters, [&] (const TDataCenter* dataCenter) {
        auto it = DomainToLoadFactorToNode_.find(TPlacementDomain{dataCenter, medium});
        if (it != DomainToLoadFactorToNode_.end()) {
            LoadFactorToNodeIterator_.AddRange(it->second);
        }
    });
}

const TDynamicChunkManagerConfigPtr& TChunkPlacement::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
