#ifndef CHUNK_PLACEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_placement.h"
// For the sake of sane code completion.
#include "chunk_placement.h"
#endif

#include <yt/yt/server/master/node_tracker_server/rack.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <typename TGenericChunk>
class TChunkPlacement::TTargetCollector
{
public:
    TTargetCollector(
        const TChunkPlacement* chunkPlacement,
        const TDomesticMedium* medium,
        const TGenericChunk* chunk,
        const TChunkLocationPtrWithReplicaInfoList& replicas,
        std::optional<int> replicationFactorOverride,
        bool allowMultipleReplicasPerNode,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica)
        : ChunkPlacement_(chunkPlacement)
        , Medium_(medium)
        , Chunk_(chunk)
        , MaxReplicasPerRack_(ChunkPlacement_->GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride))
        , ReplicationFactorOverride_(replicationFactorOverride)
        , AllowMultipleReplicasPerNode_(allowMultipleReplicasPerNode)
    {
        if (forbiddenNodes) {
            ForbiddenNodes_ = *forbiddenNodes;
        }
        if (allocatedNodes) {
            ForbiddenNodes_.insert(
                ForbiddenNodes_.end(),
                allocatedNodes->begin(),
                allocatedNodes->end());
        }

        SortUnique(ForbiddenNodes_);

        auto processAllocatedNode = [&] (TNode* node) {
            IncreaseRackUsage(node);
            IncreaseDataCenterUsage(node);
        };

        int mediumIndex = medium->GetIndex();
        for (auto replica : replicas) {
            if (replica.GetPtr()->GetEffectiveMediumIndex() == mediumIndex) {
                auto node = replica.GetPtr()->GetNode();
                if (!AllowMultipleReplicasPerNode_) {
                    ForbiddenNodes_.push_back(node);
                }
                // NB: When running replication job for unsafely placed chunk we do not increment
                // counters for unsafely placed replica because it will be removed anyway. Otherwise
                // it is possible that no feasible replica will be found. Consider case with three
                // storage data centers and RS(3, 3) chunk. Data center replica count limit forbids to
                // put more than two replicas in every data center, so it's impossible to allocate extra
                // replica to move unsafely placed replica there.
                if (!node->IsDecommissioned() && replica != unsafelyPlacedReplica) {
                    processAllocatedNode(node);
                }
            }
        }

        if (allocatedNodes) {
            for (auto* node : *allocatedNodes) {
                processAllocatedNode(node);
            }
        }
    }

    bool CheckNode(
        TNode* node,
        bool enableRackAwareness,
        bool enableDataCenterAwareness,
        bool enableNodeWriteSessionLimit) const
    {
        if (std::find(ForbiddenNodes_.begin(), ForbiddenNodes_.end(), node) != ForbiddenNodes_.end()) {
            return false;
        }

        if (enableNodeWriteSessionLimit && !CheckWriteSessionUsage(node)) {
            return false;
        }

        if (enableRackAwareness && !CheckRackUsage(node)) {
            return false;
        }

        if (enableDataCenterAwareness && !CheckDataCenterUsage(node)) {
            return false;
        }

        return true;
    }

    void AddNode(TNode* node)
    {
        IncreaseRackUsage(node);
        IncreaseDataCenterUsage(node);
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
    const TChunkPlacement* const ChunkPlacement_;
    const TDomesticMedium* const Medium_;
    const TGenericChunk* const Chunk_;

    const int MaxReplicasPerRack_;
    const std::optional<int> ReplicationFactorOverride_;
    const bool AllowMultipleReplicasPerNode_;

    std::array<i8, NNodeTrackerServer::RackIndexBound> PerRackCounters_{};

    // TODO(gritukan): YT-16557
    TCompactFlatMap<const NNodeTrackerServer::TDataCenter*, i8, 4> PerDataCenterCounters_;

    TNodeList ForbiddenNodes_;
    TNodeList AddedNodes_;

    void IncreaseRackUsage(TNode* node)
    {
        const auto* rack = node->GetRack();
        if (rack) {
            ++PerRackCounters_[rack->GetIndex()];
        }
    }

    bool CheckRackUsage(TNode* node) const
    {
        if (const auto* rack = node->GetRack()) {
            auto usage = PerRackCounters_[rack->GetIndex()];
            return usage < MaxReplicasPerRack_;
        } else {
            return true;
        }
    }

    void IncreaseDataCenterUsage(TNode* node)
    {
        if (const auto* dataCenter = node->GetDataCenter()) {
            auto counterIt = PerDataCenterCounters_.find(dataCenter);
            if (counterIt == PerDataCenterCounters_.end()) {
                PerDataCenterCounters_.emplace(dataCenter, 1);
            } else {
                ++counterIt->second;
            }
        }
    }

    bool CheckDataCenterUsage(TNode* node) const
    {
        auto* dataCenter = node->GetDataCenter();
        YT_ASSERT(dataCenter);
        auto counterIt = PerDataCenterCounters_.find(dataCenter);
        if (counterIt == PerDataCenterCounters_.end()) {
            return true;
        }

        auto counter = counterIt->second;
        auto maxReplicasPerDataCenter = GetMaxReplicasPerDataCenter(dataCenter);
        return counter < maxReplicasPerDataCenter;
    }

    int GetMaxReplicasPerDataCenter(NNodeTrackerServer::TDataCenter* dataCenter) const
    {
        return ChunkPlacement_->GetMaxReplicasPerDataCenter(
            Medium_,
            Chunk_,
            dataCenter,
            ReplicationFactorOverride_);
    }

    bool CheckWriteSessionUsage(TNode* node) const
    {
        auto limitFraction = ChunkPlacement_->GetDynamicConfig()->NodeWriteSessionLimitFractionOnWriteTargetAllocation;
        const auto& multicellManager = ChunkPlacement_->Bootstrap_->GetMulticellManager();
        auto chunkHostMasterCellCount = multicellManager->GetRoleMasterCellCount(
            NCellMaster::EMasterCellRole::ChunkHost);

        if (auto maxNodeWriteSessions = node->GetWriteSessionLimit()) {
            auto sessionCount = node->GetTotalHintedSessionCount(chunkHostMasterCellCount);
            if (sessionCount >= *maxNodeWriteSessions * limitFraction) {
                return false;
            }
        }

        auto mediumIndex = Medium_->GetIndex();
        auto it = node->MediumToWriteSessionCountLimit().find(mediumIndex);
        if (it != node->MediumToWriteSessionCountLimit().end()) {
            auto maxSessionPerMediumLocation = it->second;
            auto mediumIOWeight = GetOrDefault(node->IOWeights(), mediumIndex, 0.0);
            auto mediumSessionCount = node->GetHintedSessionCount(mediumIndex, chunkHostMasterCellCount);
            if (mediumSessionCount >= mediumIOWeight * maxSessionPerMediumLocation * limitFraction) {
                return false;
            }
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TGenericChunk>
TNodeList TChunkPlacement::AllocateWriteTargets(
    TDomesticMedium* medium,
    TGenericChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    const std::optional<std::string>& preferredHostName,
    NChunkClient::ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        replicas,
        /*replicaIndexes*/ {},
        desiredCount,
        minCount,
        sessionType,
        replicationFactorOverride,
        forbiddenNodes,
        allocatedNodes,
        preferredHostName);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

template <typename TGenericChunk>
TNodeList TChunkPlacement::GetWriteTargets(
    TDomesticMedium* medium,
    TGenericChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    NChunkClient::ESessionType sessionType,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    const std::optional<std::string>& preferredHostName,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica,
    bool systemAllocation)
{
    auto* preferredNode = FindPreferredNode(preferredHostName, medium);

    std::optional<TNodeList> consistentPlacementWriteTargets;
    if constexpr (TGenericChunk::SupportsConsistentPlacement) {
        consistentPlacementWriteTargets = FindConsistentPlacementWriteTargets(
            medium,
            chunk,
            replicas,
            replicaIndexes,
            desiredCount,
            minCount,
            forbiddenNodes,
            allocatedNodes,
            preferredNode);
    }

    // We may have trouble placing replicas consistently. In that case, ignore
    // CRP for the time being.
    // This may happen when:
    //   - #forbiddenNodes are specified (which means a writer already has trouble);
    //   - a target node dictated by CRP is unavailable (and more time is required
    //     by CRP to react to that);
    //   - etc.
    // In any such case we rely on the replicator to do its job later.
    if (consistentPlacementWriteTargets) {
        return *consistentPlacementWriteTargets;
    }

    const TLoadFactorToNodeMap* loadFactorToNodeMap = nullptr;
    TNodeToLoadFactorMap* nodeToLoadFactorMap = nullptr;

    if (EnableTwoRandomChoicesWriteTargetAllocation_) {
        auto it = MediumToNodeToLoadFactor_.find(medium);
        if (it == MediumToNodeToLoadFactor_.end()) {
            return TNodeList();
        } else {
            nodeToLoadFactorMap = &it->second;
        }
    } else {
        auto it = MediumToLoadFactorToNode_.find(medium);
        if (it == MediumToLoadFactorToNode_.end()) {
            return TNodeList();
        } else {
            loadFactorToNodeMap = &it->second;
        }
    }

    TTargetCollector collector(
        this,
        medium,
        chunk,
        replicas,
        replicationFactorOverride,
        Config_->AllowMultipleErasurePartsPerNode && chunk->IsErasure(),
        forbiddenNodes,
        allocatedNodes,
        unsafelyPlacedReplica);

    auto tryAdd = [&] (
        TNode* node,
        bool enableRackAwareness,
        bool enableDataCenterAwareness,
        bool enableNodeWriteSessionLimit)
    {
        if (!IsValidWriteTargetToAllocate(
            node,
            &collector,
            enableRackAwareness,
            enableDataCenterAwareness,
            enableNodeWriteSessionLimit))
        {
            return false;
        }
        collector.AddNode(node);
        return true;
    };

    auto hasEnoughTargets = [&] {
        return std::ssize(collector.GetAddedNodes()) == desiredCount;
    };

    TLoadFactorToNodeMap::const_iterator loadFactorToNodeIterator;
    if (!EnableTwoRandomChoicesWriteTargetAllocation_) {
        loadFactorToNodeIterator = loadFactorToNodeMap->begin();
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto tryAddAll = [&] (
        bool enableRackAwareness,
        bool enableDataCenterAwareness,
        bool enableNodeWriteSessionLimit)
    {
        YT_VERIFY(!hasEnoughTargets());

        bool hasProgress = false;
        if (EnableTwoRandomChoicesWriteTargetAllocation_) {
            auto allocationSession = nodeToLoadFactorMap->StartAllocationSession(NodesToCheckBeforeGivingUpOnWriteTargetAllocation_);
            while (!allocationSession.HasFailed() && !hasEnoughTargets()) {
                auto nodeId = allocationSession.PickRandomNode();
                auto* node = nodeTracker->GetNode(nodeId);
                hasProgress |= tryAdd(
                    node,
                    enableRackAwareness,
                    enableDataCenterAwareness,
                    enableNodeWriteSessionLimit);
            }
            return hasProgress;
        } else {
            if (loadFactorToNodeIterator == loadFactorToNodeMap->end()) {
                loadFactorToNodeIterator = loadFactorToNodeMap->begin();
            }

            for ( ; !hasEnoughTargets() && loadFactorToNodeIterator != loadFactorToNodeMap->end(); ++loadFactorToNodeIterator) {
                auto* node = loadFactorToNodeIterator->second;
                hasProgress |= tryAdd(
                    node,
                    enableRackAwareness,
                    enableDataCenterAwareness,
                    enableNodeWriteSessionLimit);
            }
        }
        return hasProgress;
    };

    auto enableNodeWriteSessionLimit = systemAllocation || IsNodeWriteSessionLimitForUserAllocationEnabled_;

    if (preferredNode) {
        tryAdd(
            preferredNode,
            /*enableRackAwareness*/ true,
            /*enableDataCenterAwareness*/ IsDataCenterAware_,
            /*enableNodeWriteSessionLimit*/ enableNodeWriteSessionLimit && IsNodeWriteSessionLimitEnabled_);
    }

    if (!hasEnoughTargets()) {
        tryAddAll(
            /*enableRackAwareness*/ true,
            /*enableDataCenterAwareness*/ IsDataCenterAware_,
            /*enableNodeWriteSessionLimit*/ enableNodeWriteSessionLimit && IsNodeWriteSessionLimitEnabled_);
    }

    bool forceRackAwareness = sessionType == NChunkClient::ESessionType::Replication ||
        (chunk->IsErasure() && GetDynamicConfig()->ForceRackAwarenessForErasureParts);

    if (!forceRackAwareness) {
        while (!hasEnoughTargets()) {
            // Disabling rack awareness also disables data center awareness.
            bool hasProgress = tryAddAll(
                /*enableRackAwareness*/ false,
                /*enableDataCenterAwareness*/ false,
                /*enableNodeWriteSessionLimit*/ enableNodeWriteSessionLimit && IsNodeWriteSessionLimitEnabled_);
            if (!hasProgress) {
                break;
            }
            if (!chunk->IsErasure() || !Config_->AllowMultipleErasurePartsPerNode) {
                break;
            }
        }
    }

    const auto& nodes = collector.GetAddedNodes();
    return std::ssize(nodes) < minCount ? TNodeList() : nodes;
}

template <typename TGenericChunk>
TNodeList TChunkPlacement::AllocateWriteTargets(
    TDomesticMedium* medium,
    TGenericChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    NChunkClient::ESessionType sessionType,
    TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica)
{
    auto targetNodes = GetWriteTargets(
        medium,
        chunk,
        replicas,
        replicaIndexes,
        desiredCount,
        minCount,
        sessionType,
        replicationFactorOverride,
        /*forbiddenNodes*/ nullptr,
        /*allocatedNodes*/ nullptr,
        /*preferredHostName*/ std::nullopt,
        unsafelyPlacedReplica,
        /*systemAllocation*/ true);

    for (auto* target : targetNodes) {
        AddSessionHint(target, medium->GetIndex(), sessionType);
    }

    return targetNodes;
}

template <typename TGenericChunk>
bool TChunkPlacement::IsValidWriteTargetToAllocate(
    TNode* node,
    TTargetCollector<TGenericChunk>* collector,
    bool enableRackAwareness,
    bool enableDataCenterAwareness,
    bool enableNodeWriteSessionLimit)
{
    // Check node first.
    if (!IsValidWriteTargetCore(node)) {
        return false;
    }

    // If replicator is data center aware, unaware nodes are not allowed.
    if (enableDataCenterAwareness && !node->GetDataCenter()) {
        return false;
    }

    if (!collector->CheckNode(node, enableRackAwareness, enableDataCenterAwareness, enableNodeWriteSessionLimit)) {
        // The collector does not like this node.
        return false;
    }

    // Seems OK :)
    return true;
}


template <typename TGenericChunk>
int TChunkPlacement::GetMaxReplicasPerRack(
    const TMedium* medium,
    const TGenericChunk* chunk,
    std::optional<int> replicationFactorOverride) const
{
    // For now, replication factor on offshore medium is always 1.
    if (medium->IsOffshore()) {
        return 1;
    }

    auto result = chunk->GetMaxReplicasPerFailureDomain(
        medium->GetIndex(),
        replicationFactorOverride,
        Bootstrap_->GetChunkManager()->GetChunkRequisitionRegistry());
    return CapPerRackReplicationFactor(result, medium, chunk);
}

template <typename TGenericChunk>
int TChunkPlacement::GetMaxReplicasPerRack(
    int mediumIndex,
    const TGenericChunk* chunk,
    std::optional<int> replicationFactorOverride) const
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
    return GetMaxReplicasPerRack(medium, chunk, replicationFactorOverride);
}

template <typename TGenericChunk>
int TChunkPlacement::GetMaxReplicasPerDataCenter(
    const TDomesticMedium* medium,
    const TGenericChunk* chunk,
    const NNodeTrackerServer::TDataCenter* dataCenter,
    std::optional<int> replicationFactorOverride) const
{
    return GetMaxReplicasPerDataCenter(medium->GetIndex(), chunk, dataCenter, replicationFactorOverride);
}

template <typename TGenericChunk>
int TChunkPlacement::GetMaxReplicasPerDataCenter(
    int mediumIndex,
    const TGenericChunk* chunk,
    const NNodeTrackerServer::TDataCenter* dataCenter,
    std::optional<int> replicationFactorOverride) const
{
    if (!IsDataCenterAware_) {
        return Max<int>();
    }

    if (!IsDataCenterFeasible(dataCenter)) {
        return 0;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* chunkRequisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

    const auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
    auto replicaCount = replicationFactorOverride.value_or(
        chunk->GetPhysicalReplicationFactor(mediumIndex, chunkRequisitionRegistry));
    replicaCount = CapTotalReplicationFactor(replicaCount, chunk, medium);
    auto aliveStorageDataCenterCount = std::ssize(AliveStorageDataCenters_);
    if (aliveStorageDataCenterCount == 0) {
        // Dividing by zero is bad, so case of zero alive data centers is handled separately.
        // Actually, in this case replica allocation is impossible, so we can return any possible value.
        return replicaCount;
    }

    auto maxReplicasPerDataCenter = DivCeil<int>(replicaCount, aliveStorageDataCenterCount);
    auto maxReplicasPerFailureDomain = chunk->GetMaxReplicasPerFailureDomain(
        mediumIndex,
        replicationFactorOverride,
        chunkRequisitionRegistry);

    // Typically it's impossible to store chunk in such a way that after data center loss it is still
    // available when one data center is already banned, so we do not consider data center as a failure
    // domain when there are banned data centers.
    // Consider a cluster with 3 data centers and chunk with erasure codec RS(6, 3). When one data center
    // is lost, at least one data center will store at least 5 of its replicas which is too much to repair
    // chunk from the rest parts.
    if (BannedStorageDataCenters_.empty()) {
        maxReplicasPerDataCenter = std::min<int>(maxReplicasPerDataCenter, maxReplicasPerFailureDomain);
    }

    return maxReplicasPerDataCenter;
}

template <typename TGenericChunk>
int TChunkPlacement::CapTotalReplicationFactor(
    int replicationFactor,
    const TGenericChunk* chunk,
    const TMedium* medium) const
{
    const auto& config = medium->AsDomestic()->Config();
    return chunk->CapTotalReplicationFactor(replicationFactor, config);
}

template <typename TGenericChunk>
int TChunkPlacement::CapPerRackReplicationFactor(
    int replicationFactor,
    const TMedium* medium,
    const TGenericChunk* chunk) const
{
    const auto& config = medium->AsDomestic()->Config();
    // TODO(danilalexeev): introduce bounds to the chunk server config options.
    replicationFactor = std::min(replicationFactor, NChunkClient::MaxReplicationFactor);
    return chunk->CapPerRackReplicationFactor(replicationFactor, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
