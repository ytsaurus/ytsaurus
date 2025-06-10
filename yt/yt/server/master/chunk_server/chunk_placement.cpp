#include "chunk_placement.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "config.h"
#include "chunk_location.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/host.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/server/master/object_server/object.h>

#include <util/random/fast.h>

#include <ranges>
#include <array>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TNodeToLoadFactorMap::TNodeToLoadFactorMap()
    : Rng_(TReallyFastRng32(TInstant::Now().MicroSeconds()))
{ }

void TChunkPlacement::TNodeToLoadFactorMap::InsertNodeOrCrash(TNodeId nodeId, double loadFactor)
{
    auto lastPosition = Values_.size();
    Values_.emplace_back(nodeId, loadFactor);
    EmplaceOrCrash(NodeToIndex_, nodeId, lastPosition);
}

void TChunkPlacement::TNodeToLoadFactorMap::RemoveNode(TNodeId nodeId)
{
    auto nodeIt = GetIteratorOrCrash(NodeToIndex_, nodeId);
    auto nodeIndex = nodeIt->second;
    auto lastNodeIndex = Values_.size() - 1;

    auto nodeToRemoveIt = Values_.begin() + nodeIndex;
    auto lastNodeIt = Values_.begin() + lastNodeIndex;

    NodeToIndex_[lastNodeIt->first] = nodeIndex;
    std::iter_swap(nodeToRemoveIt, lastNodeIt);

    NodeToIndex_.erase(nodeIt);
    Values_.pop_back();
}

bool TChunkPlacement::TNodeToLoadFactorMap::Contains(TNodeId nodeId) const
{
    return NodeToIndex_.find(nodeId) != NodeToIndex_.end();
}

bool TChunkPlacement::TNodeToLoadFactorMap::Empty() const
{
    YT_VERIFY(NodeToIndex_.size() == Values_.size());
    return Values_.empty();
}

i64 TChunkPlacement::TNodeToLoadFactorMap::Size() const
{
    YT_VERIFY(NodeToIndex_.size() == Values_.size());
    return Values_.size();
}

TNodeId TChunkPlacement::TNodeToLoadFactorMap::PickRandomNode(int nodesChecked)
{
    // NB: It's ok if the same node is picked twice.
    // The chance of having the worst outcome in such case is 1 / 2(n^2), which is not noticeable on big clusters,
    // and small clusters already have to iterate over the whole vector to guarantee that chunk can be placed.
    auto firstPickIndex = Rng_.Uniform(nodesChecked, Values_.size());
    auto secondPickIndex = Rng_.Uniform(nodesChecked, Values_.size());

    const auto& firstPick = Values_[firstPickIndex];
    const auto& secondPick = Values_[secondPickIndex];
    auto resultingPickIndex = firstPick.second < secondPick.second
        ? firstPickIndex
        : secondPickIndex;
    auto resultingNodeId = Values_[resultingPickIndex].first;

    SwapNodes(nodesChecked, resultingPickIndex);
    return resultingNodeId;
}

void TChunkPlacement::TNodeToLoadFactorMap::SwapNodes(int firstIndex, int secondIndex)
{
    auto firstElemIt = Values_.begin() + firstIndex;
    auto secondElemIt = Values_.begin() + secondIndex;

    NodeToIndex_[firstElemIt->first] = secondIndex;
    NodeToIndex_[secondElemIt->first] = firstIndex;

    std::iter_swap(firstElemIt, secondElemIt);
}

TChunkPlacement::TAllocationSession TChunkPlacement::TNodeToLoadFactorMap::StartAllocationSession(int nodesToCheckBeforeGivingUpOnWriteTargetAllocation)
{
    return TAllocationSession(
        this,
        std::min<int>(nodesToCheckBeforeGivingUpOnWriteTargetAllocation, Values_.size()));
}

TChunkPlacement::TAllocationSession::TAllocationSession(
    TNodeToLoadFactorMap* associatedMap,
    int nodesToCheckBeforeGivingUpOnWriteTargetAllocation)
    : AssociatedMap_(associatedMap)
    , NodesToCheckBeforeFailing_(nodesToCheckBeforeGivingUpOnWriteTargetAllocation)
{ }

bool TChunkPlacement::TAllocationSession::HasFailed() const
{
    return NodesChecked_ >= NodesToCheckBeforeFailing_;
}

TNodeId TChunkPlacement::TAllocationSession::PickRandomNode()
{
    YT_ASSERT(!HasFailed());
    return AssociatedMap_->PickRandomNode(NodesChecked_++);
}

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TBootstrap* bootstrap,
    TConsistentChunkPlacementPtr consistentPlacement)
    : Bootstrap_(bootstrap)
    , Config_(bootstrap->GetConfig()->ChunkManager)
    , ConsistentPlacement_(std::move(consistentPlacement))
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChunkPlacement::OnDynamicConfigChanged, MakeWeak(this)));
}

void TChunkPlacement::Clear()
{
    MediumToNodeToLoadFactor_.clear();
    EnableTwoRandomChoicesWriteTargetAllocation_ = false;
    NodesToCheckBeforeGivingUpOnWriteTargetAllocation_ = 0;

    MediumToLoadFactorToNode_.clear();
    IsDataCenterAware_ = false;
    IsDataCenterFailureDetectorEnabled_ = false;
    IsNodeWriteSessionLimitEnabled_ = false;
    IsNodeWriteSessionLimitForUserAllocationEnabled_ = false;
    StorageDataCenters_.clear();
    BannedStorageDataCenters_.clear();
    FaultyStorageDataCenters_.clear();
    AliveStorageDataCenters_.clear();
    DataCenterSetErrors_.clear();
    DataCenterFaultErrors_.clear();
}

void TChunkPlacement::Initialize()
{
    const auto& nodes = Bootstrap_->GetNodeTracker()->Nodes();
    for (auto [nodeId, node] : nodes) {
        if (!IsObjectAlive(node)) {
            continue;
        }

        OnNodeUpdated(node);
    }
}

void TChunkPlacement::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    IsDataCenterAware_ = GetDynamicConfig()->UseDataCenterAwareReplicator;
    IsDataCenterFailureDetectorEnabled_ = GetDynamicConfig()->DataCenterFailureDetector->Enable;
    EnableTwoRandomChoicesWriteTargetAllocation_ = GetDynamicConfig()->EnableTwoRandomChoicesWriteTargetAllocation;
    NodesToCheckBeforeGivingUpOnWriteTargetAllocation_ = GetDynamicConfig()->NodesToCheckBeforeGivingUpOnWriteTargetAllocation;
    IsNodeWriteSessionLimitEnabled_ = GetDynamicConfig()->EnableNodeWriteSessionLimitOnWriteTargetAllocation;
    IsNodeWriteSessionLimitForUserAllocationEnabled_ = GetDynamicConfig()->EnableNodeWriteSessionLimitForUserOnWriteTargetAllocation;

    if (!IsDataCenterFailureDetectorEnabled_) {
        FaultyStorageDataCenters_.clear();
        DataCenterFaultErrors_.clear();
    }
    RecomputeDataCenterSets();
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    RegisterNode(node);
}

void TChunkPlacement::RegisterNode(TNode* node)
{
    if (!node->ReportedDataNodeHeartbeat()) {
        return;
    }

    InsertToLoadFactorMaps(node);
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    UnregisterNode(node);
    RegisterNode(node);
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    UnregisterNode(node);
}

void TChunkPlacement::UnregisterNode(TNode* node)
{
    node->ClearSessionHints();

    RemoveFromLoadFactorMaps(node);
}

void TChunkPlacement::OnNodeDisposed(TNode* node)
{
    for (const auto& item : node->LoadFactorIterators()) {
        YT_VERIFY(!item.second);
    }

    for (const auto& [_, nodeToLoadFactorMap] : MediumToNodeToLoadFactor_) {
        YT_VERIFY(!nodeToLoadFactorMap.Contains(node->GetId()));
    }
}

void TChunkPlacement::OnDataCenterChanged(TDataCenter* /*dataCenter*/)
{
    RecomputeDataCenterSets();
}

bool TChunkPlacement::IsDataCenterFeasible(const TDataCenter* dataCenter) const
{
    return AliveStorageDataCenters_.contains(dataCenter);
}

THashSet<std::string> TChunkPlacement::GetFaultyStorageDataCenterNames() const
{
    THashSet<std::string> result;
    for (const auto* faultyDataCenter : FaultyStorageDataCenters_) {
        result.insert(faultyDataCenter->GetName());
    }
    return result;
}

void TChunkPlacement::CheckFaultyDataCentersOnPrimaryMaster()
{
    // If replicator is not data center aware, data center sets are not required.
    if (!IsDataCenterAware_ || !IsDataCenterFailureDetectorEnabled_) {
        return;
    }

    DataCenterFaultErrors_.clear();
    auto oldFaultyStorageDataCenters = std::exchange(FaultyStorageDataCenters_, {});

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& dataCenterName : GetDynamicConfig()->StorageDataCenters) {
        auto* dataCenter = nodeTracker->FindDataCenterByName(dataCenterName);
        if (!IsObjectAlive(dataCenter)) {
            auto error = TError("Storage data center %Qv not found",
                dataCenterName);
            DataCenterFaultErrors_.push_back(std::move(error));
            continue;
        }

        auto dataCenterIsEnabled = !oldFaultyStorageDataCenters.contains(dataCenter);

        auto dataCenterStatistics = nodeTracker->GetDataCenterFlavoredNodeStatistics(dataCenter, ENodeFlavor::Data);
        if (!dataCenterStatistics) {
            auto error = TError("Storage data center %Qv doesn't have statistics",
                dataCenterName);
            DataCenterFaultErrors_.push_back(std::move(error));

            if (!dataCenterIsEnabled) {
                InsertOrCrash(FaultyStorageDataCenters_, dataCenter);
            }
            continue;
        }

        auto error = ComputeDataCenterFaultiness(dataCenter, *dataCenterStatistics, dataCenterIsEnabled);
        if (!error.IsOK()) {
            if (!BannedStorageDataCenters_.contains(dataCenter)) {
                DataCenterFaultErrors_.push_back(std::move(error));
            }
            InsertOrCrash(FaultyStorageDataCenters_, dataCenter);
        }
    }

    RecomputeDataCenterSets();
}

void TChunkPlacement::SetFaultyDataCentersOnSecondaryMaster(const THashSet<std::string>& faultyDataCenters)
{
    // If replicator is not data center aware, data center sets are not required.
    if (!IsDataCenterAware_ || !IsDataCenterFailureDetectorEnabled_) {
        return;
    }

    FaultyStorageDataCenters_.clear();
    DataCenterFaultErrors_.clear();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& faultyDataCenterName : faultyDataCenters) {
        auto* dataCenter = nodeTracker->FindDataCenterByName(faultyDataCenterName);
        if (IsObjectAlive(dataCenter)) {
            InsertOrCrash(FaultyStorageDataCenters_, dataCenter);
        } else {
            auto error = TError("Received faulty storage data center %Qv not found",
                dataCenter);
            DataCenterFaultErrors_.push_back(std::move(error));
        }
    }

    RecomputeDataCenterSets();
}

TNodeList TChunkPlacement::GetConsistentPlacementWriteTargets(const TChunk* chunk, int mediumIndex)
{
    YT_ASSERT(IsConsistentChunkPlacementEnabled());
    YT_VERIFY(chunk->HasConsistentReplicaPlacementHash());
    return ConsistentPlacement_->GetWriteTargets(chunk, mediumIndex);
}

void TChunkPlacement::InsertToLoadFactorMaps(TNode* node)
{
    RemoveFromLoadFactorMaps(node);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto chunkHostMasterCellCount = multicellManager->GetRoleMasterCellCount(EMasterCellRole::ChunkHost);

    // Iterate through IOWeights because IsValidWriteTargetToInsert check if IOWeights contains medium.
    for (const auto& [mediumIndex, _] : node->IOWeights()) {
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (medium->IsOffshore()) {
            continue;
        }
        auto* domesticMedium = medium->AsDomestic();

        if (!IsValidWriteTargetToInsert(domesticMedium, node)) {
            continue;
        }

        auto loadFactor = node->GetLoadFactor(mediumIndex, chunkHostMasterCellCount);
        if (!loadFactor) {
            continue;
        }

        auto it = MediumToLoadFactorToNode_[domesticMedium].emplace(*loadFactor, node);
        node->SetLoadFactorIterator(mediumIndex, it);

        MediumToNodeToLoadFactor_[domesticMedium].InsertNodeOrCrash(node->GetId(), *loadFactor);
    }
}

void TChunkPlacement::RemoveFromLoadFactorMaps(TNode* node)
{
    for (const auto& [mediumIndex, factorMapIter] : node->LoadFactorIterators()) {
        auto* medium = Bootstrap_->GetChunkManager()->FindMediumByIndex(mediumIndex);

        if (!factorMapIter || !medium || medium->IsOffshore()) {
            continue;
        }

        auto mediumToFactorMapIter = MediumToLoadFactorToNode_.find(medium->AsDomestic());
        YT_VERIFY(mediumToFactorMapIter != MediumToLoadFactorToNode_.end());

        auto& factorMap = mediumToFactorMapIter->second;
        factorMap.erase(*factorMapIter);
        node->SetLoadFactorIterator(mediumIndex, std::nullopt);

        if (factorMap.empty()) {
            MediumToLoadFactorToNode_.erase(mediumToFactorMapIter);
        }
    }

    for (auto it = MediumToNodeToLoadFactor_.begin(); it != MediumToNodeToLoadFactor_.end(); ) {
        auto& nodeToLoadFactorMap = it->second;

        if (nodeToLoadFactorMap.Contains(node->GetId())) {
            nodeToLoadFactorMap.RemoveNode(node->GetId());
        }

        if (nodeToLoadFactorMap.Empty()) {
            MediumToNodeToLoadFactor_.erase(it++);
        } else {
            ++it;
        }
    }
}

TNode* TChunkPlacement::FindPreferredNode(
    const std::optional<std::string>& preferredHostName,
    TDomesticMedium* medium)
{
    if (!preferredHostName) {
        return nullptr;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto* preferredHost = nodeTracker->FindHostByName(*preferredHostName);
    // COMPAT(gritukan)
    if (!preferredHost) {
        if (auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName)) {
            preferredHost = preferredNode->GetHost();
        }
    }

    if (!preferredHost) {
        return nullptr;
    }

    for (auto* node : preferredHost->GetNodesWithFlavor(ENodeFlavor::Data)) {
        if (IsValidPreferredWriteTargetToAllocate(node, medium)) {
            // NB: Assuming a single data node per host here.
            return node;
        }
    }

    return nullptr;
}

std::optional<TNodeList> TChunkPlacement::FindConsistentPlacementWriteTargets(
    TDomesticMedium* medium,
    TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& replicas,
    const TChunkReplicaIndexList& replicaIndexes,
    int desiredCount,
    int minCount,
    const TNodeList* forbiddenNodes,
    const TNodeList* allocatedNodes,
    TNode* preferredNode)
{
    YT_ASSERT(replicaIndexes.empty() || std::ssize(replicaIndexes) == minCount);
    YT_ASSERT(std::find(replicaIndexes.begin(), replicaIndexes.end(), NChunkClient::GenericChunkReplicaIndex) == replicaIndexes.end());
    YT_ASSERT(replicaIndexes.empty() || chunk->IsErasure());

    if (!chunk->HasConsistentReplicaPlacementHash()) {
        return std::nullopt;
    }

    if (!IsConsistentChunkPlacementEnabled()) {
        return std::nullopt;
    }

    auto mediumIndex = medium->GetIndex();
    auto result = GetConsistentPlacementWriteTargets(chunk, mediumIndex);

    if (result.empty()) {
        return std::nullopt; // No online nodes.
    }

    if (minCount > std::ssize(result) || desiredCount > std::ssize(result)) {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& dataNodeStatistics = nodeTracker->GetFlavoredNodeStatistics(NNodeTrackerClient::ENodeFlavor::Data);
        if (desiredCount > dataNodeStatistics.OnlineNodeCount) {
            YT_LOG_WARNING("Requested to allocate too many consistently placed chunk replica targets "
                "(ChunkId: %v, ReplicaIndexes: %v, MediumIndex: %v, MinReplicaCount: %v, DesiredReplicaCount: %v, ConsistentPlacementReplicaCount: %v, OnlineDataNodeCount: %v)",
                chunk->GetId(),
                replicaIndexes,
                mediumIndex,
                minCount,
                desiredCount,
                std::ssize(result),
                dataNodeStatistics.OnlineNodeCount);
        }
        return std::nullopt;
    }

    // NB: replicaIndexes may be empty.
    if (std::find_if(
        replicaIndexes.begin(),
        replicaIndexes.end(),
        [&] (int replicaIndex) {
            return replicaIndex >= std::ssize(result);
        })!= replicaIndexes.end())
    {
        YT_LOG_ALERT("Target nodes dictated by consistent chunk placement are fewer than the specified replica index (ChunkId: %v, MediumIndex: %v, ConsistentPlacementTargetNodeCount: %v, ReplicaIndexes: %v)",
            chunk->GetId(),
            mediumIndex,
            std::ssize(result),
            replicaIndexes);
        return std::nullopt;
    }

    if (!replicaIndexes.empty()) {
        TNodeList filteredResult;
        filteredResult.reserve(replicaIndexes.size());
        for (auto replicaIndex : replicaIndexes) {
            filteredResult.push_back(result[replicaIndex]);
        }
        result = std::move(filteredResult);
        YT_ASSERT(std::ssize(replicaIndexes) == std::ssize(result));
    }

    YT_ASSERT(std::all_of(
        result.begin(),
        result.end(),
        [&] (TNode* node) {
            return node->IsValidWriteTarget();
        }));

    auto isNodeForbidden = [&] (TNode* node) {
        if (forbiddenNodes &&
            std::find(forbiddenNodes->begin(), forbiddenNodes->end(), node) != forbiddenNodes->end())
        {
            return true;
        }

        if (allocatedNodes &&
            std::find(allocatedNodes->begin(), allocatedNodes->end(), node) != allocatedNodes->end())
        {
            return true;
        }

        return false;
    };

    auto isNodeConsistent = [&] (TNode* node, int replicaIndex) {
        for (auto replica : replicas) {
            if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
                continue;
            }

            if (replicaIndex == NChunkClient::GenericChunkReplicaIndex) {
                if (replica.GetPtr()->GetNode() == node) {
                    return true;
                }
            } else if (replica.GetReplicaIndex() == replicaIndex) {
                return replica.GetPtr()->GetNode() == node;
            }
        }

        return false;
    };

    // Regular and erasure chunks are fundamentally different: for the former,
    // it's ok to reorder replicas and therefore we're allowed to filter out
    // some target nodes if necessary. For erasure chunks, a need to filter a
    // target node out means failing to place replicas consistently.

    // NB: The code below is quadratic, but all factors are small.
    if (chunk->IsErasure()) {
        for (auto* node : result) {
            if (isNodeForbidden(node)) {
                return std::nullopt;
            }

            if (replicaIndexes.empty()) {
                for (auto replicaIndex = 0; replicaIndex < std::ssize(result); ++replicaIndex) {
                    auto* node = result[replicaIndex];
                    if (isNodeConsistent(node, replicaIndex)) {
                        return std::nullopt;
                    }
                }
            } else {
                for (auto i = 0; i < std::ssize(result); ++i) {
                    auto* node = result[i];
                    auto replicaIndex = replicaIndexes[i];
                    if (isNodeConsistent(node, replicaIndex)) {
                        return std::nullopt;
                    }
                }
            }
        }
    } else {
        result.erase(
            std::remove_if(
                result.begin(),
                result.end(),
                [&] (TNode* node) {
                    return isNodeForbidden(node) || isNodeConsistent(node, NChunkClient::GenericChunkReplicaIndex);
                }),
            result.end());
    }

    if (std::ssize(result) < minCount) {
        return std::nullopt;
    }

    YT_VERIFY(!result.empty());

    YT_ASSERT(desiredCount >= std::ssize(replicaIndexes));
    if (desiredCount < std::ssize(result)) {
        // Make sure the preferred node makes it to the result after trimming.
        if (preferredNode && !chunk->IsErasure()) {
            auto it = std::find(result.begin(), result.end(), preferredNode);
            if (it != result.end()) {
                std::swap(result.front(), *it);
            }
        }
        // Trim the result.
        auto tailIt = result.begin();
        std::advance(tailIt, desiredCount);
        result.erase(tailIt, result.end());
    }

    return result;
}

TChunkLocation* TChunkPlacement::GetRemovalTarget(
    TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes,
    const TChunkLocationPtrWithReplicaInfoList& replicas)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
    auto mediumIndex = chunkWithIndexes.GetMediumIndex();
    auto maxReplicasPerRack = GetMaxReplicasPerRack(mediumIndex, chunk);

    std::array<i8, RackIndexBound> perRackCounters{};
    // TODO(gritukan): YT-16557.
    TCompactFlatMap<const TDataCenter*, i8, 4> perDataCenterCounters;

    for (auto replica : replicas) {
        if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
            continue;
        }

        if (const auto* rack = replica.GetPtr()->GetNode()->GetRack()) {
            ++perRackCounters[rack->GetIndex()];
            if (auto dataCenter = rack->GetDataCenter()) {
                ++perDataCenterCounters[dataCenter];
            }
        }
    }

    // An arbitrary node that violates consistent placement requirements.
    TChunkLocation* consistentPlacementWinner = nullptr;
    // An arbitrary node from a rack with too many replicas.
    TChunkLocation* rackWinner = nullptr;
    // An arbitrary node from a data center with too many replicas.
    TChunkLocation* dataCenterWinner = nullptr;
    // A node with the largest fill factor.
    TChunkLocation* fillFactorWinner = nullptr;

    TNodeList consistentPlacementNodes;
    if (chunk->HasConsistentReplicaPlacementHash() && IsConsistentChunkPlacementEnabled()) {
        // NB: Do not ask for consistent chunk placement on unexpected medium.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        const auto& replication = chunk->GetAggregatedReplication(requisitionRegistry);
        for (const auto& entry : replication) {
            if (entry.GetMediumIndex() == mediumIndex) {
                consistentPlacementNodes = GetConsistentPlacementWriteTargets(chunk, mediumIndex);
                break;
            }
        }
    }

    auto isInconsistentlyPlaced = [&] (TNode* node) {
        if (!chunk->HasConsistentReplicaPlacementHash()) {
            return false;
        }

        if (!IsConsistentChunkPlacementEnabled()) {
            return false;
        }

        if (consistentPlacementNodes.empty()) {
            return false; // No online nodes.
        }

        return replicaIndex == GenericChunkReplicaIndex
            ? std::find(consistentPlacementNodes.begin(), consistentPlacementNodes.end(), node) == consistentPlacementNodes.end()
            : consistentPlacementNodes[replicaIndex] != node;
    };

    for (auto replica : replicas) {
        if (chunk->IsJournal() && replica.GetReplicaState() != EChunkReplicaState::Sealed) {
            continue;
        }

        if (replica.GetPtr()->GetEffectiveMediumIndex() != mediumIndex) {
            continue;
        }

        if (replica.GetReplicaIndex() != replicaIndex) {
            continue;
        }

        auto* location = replica.GetPtr();
        auto node = location->GetNode();
        if (!IsValidRemovalTarget(node)) {
            continue;
        }

        if (isInconsistentlyPlaced(node)) {
            consistentPlacementWinner = location;
        }

        if (const auto* rack = node->GetRack()) {
            if (perRackCounters[rack->GetIndex()] > maxReplicasPerRack) {
                rackWinner = location;
            }

            if (auto dataCenter = rack->GetDataCenter()) {
                auto maxReplicasPerDataCenter = GetMaxReplicasPerDataCenter(mediumIndex, chunk, dataCenter);
                if (perDataCenterCounters[dataCenter] > maxReplicasPerDataCenter) {
                    dataCenterWinner = location;
                }
            }
        }

        auto nodeFillFactor = node->GetFillFactor(mediumIndex);

        if (nodeFillFactor &&
            (!fillFactorWinner ||
                *nodeFillFactor > *fillFactorWinner->GetNode()->GetFillFactor(mediumIndex)))
        {
            fillFactorWinner = location;
        }
    }

    if (consistentPlacementWinner) {
        return consistentPlacementWinner;
    } else if (rackWinner) {
        return rackWinner;
    } else if (dataCenterWinner) {
        return dataCenterWinner;
    } else {
        return fillFactorWinner;
    }
}

bool TChunkPlacement::IsValidWriteTargetToInsert(TDomesticMedium* medium, TNode* node)
{
    if (!node->IsWriteEnabled(medium->GetIndex())) {
        // Do not write anything to nodes not accepting writes.
        return false;
    }

    return IsValidWriteTargetCore(node);
}

bool TChunkPlacement::IsValidPreferredWriteTargetToAllocate(TNode* node, TDomesticMedium* medium)
{
    if (!node->IsWriteEnabled(medium->GetIndex())) {
        return false;
    }

    return true;
}

bool TChunkPlacement::IsValidWriteTargetCore(TNode* node)
{
    if (!node->IsValidWriteTarget()) {
        return false;
    }

    // The above only checks DisableWriteSessions, not Effective*.
    if (node->GetEffectiveDisableWriteSessions()) {
        return false;
    }

    if (IsDataCenterAware_) {
        const auto* dataCenter = node->GetDataCenter();
        if (!dataCenter || !IsDataCenterFeasible(dataCenter)) {
            return false;
        }
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidRemovalTarget(TNode* node)
{
    if (!node->ReportedDataNodeHeartbeat()) {
        // Do not remove anything from a node before its first heartbeat or after it is unregistered.
        return false;
    }

    return true;
}

void TChunkPlacement::AddSessionHint(TNode* node, int mediumIndex, ESessionType sessionType)
{
    node->AddSessionHint(mediumIndex, sessionType);

    // NB: Insert removes node if it's present.
    InsertToLoadFactorMaps(node);
}

const std::vector<TError>& TChunkPlacement::GetAlerts() const
{
    return DataCenterSetErrors_;
}

const TDynamicChunkManagerConfigPtr& TChunkPlacement::GetDynamicConfig() const
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

bool TChunkPlacement::IsConsistentChunkPlacementEnabled() const
{
    return GetDynamicConfig()->ConsistentReplicaPlacement->Enable;
}

void TChunkPlacement::RecomputeDataCenterSets()
{
    // At first, clear everything.
    auto oldStorageDataCenters = std::exchange(StorageDataCenters_, {});
    auto oldBannedStorageDataCenters = std::exchange(BannedStorageDataCenters_, {});
    auto oldAliveStorageDataCenters = std::exchange(AliveStorageDataCenters_, {});
    DataCenterSetErrors_.clear();

    auto refreshGuard = Finally([&] () noexcept {
        if (StorageDataCenters_ != oldStorageDataCenters ||
            BannedStorageDataCenters_ != oldBannedStorageDataCenters ||
            AliveStorageDataCenters_ != oldAliveStorageDataCenters)
        {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ScheduleGlobalChunkRefresh();
        }
    });

    // If replicator is not data center aware, data center sets are not required.
    if (!IsDataCenterAware_) {
        return;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& storageDataCenter : GetDynamicConfig()->StorageDataCenters) {
        if (auto* dataCenter = nodeTracker->FindDataCenterByName(storageDataCenter); IsObjectAlive(dataCenter)) {
            InsertOrCrash(StorageDataCenters_, dataCenter);
        } else {
            auto error = TError("Storage data center %Qv is unknown",
                storageDataCenter);
            DataCenterSetErrors_.push_back(error);
        }
    }

    for (const auto& bannedDataCenter : GetDynamicConfig()->BannedStorageDataCenters) {
        if (auto* dataCenter = nodeTracker->FindDataCenterByName(bannedDataCenter); IsObjectAlive(dataCenter)) {
            if (StorageDataCenters_.contains(dataCenter)) {
                InsertOrCrash(BannedStorageDataCenters_, dataCenter);
            } else {
                auto error = TError("Banned data center %Qv is not a storage data center",
                    bannedDataCenter);
                DataCenterSetErrors_.push_back(error);
            }
        } else {
            auto error = TError("Banned data center %Qv is unknown",
                bannedDataCenter);
            DataCenterSetErrors_.push_back(error);
        }
    }

    for (const auto& error : DataCenterFaultErrors_) {
        DataCenterSetErrors_.push_back(error);
    }

    auto isStorageDataCenterAlive = [this] (const TDataCenter* dataCenter) {
        auto isBanned = BannedStorageDataCenters_.contains(dataCenter);
        auto isFaulty = FaultyStorageDataCenters_.contains(dataCenter);
        return !isBanned && !isFaulty;
    };
    for (auto* aliveDataCenter : StorageDataCenters_ | std::views::filter(isStorageDataCenterAlive)) {
        InsertOrCrash(AliveStorageDataCenters_, aliveDataCenter);
    }

    THashSet<const TDataCenter*> livenessChangedDataCenters;
    for (auto* dataCenter : AliveStorageDataCenters_) {
        if (!oldAliveStorageDataCenters.contains(dataCenter)) {
            livenessChangedDataCenters.insert(dataCenter);
        }
    }
    for (auto* dataCenter : oldAliveStorageDataCenters) {
        if (!AliveStorageDataCenters_.contains(dataCenter)) {
            livenessChangedDataCenters.insert(dataCenter);
        }
    }
    for (auto* dataCenter : livenessChangedDataCenters) {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto* rack : nodeTracker->GetDataCenterRacks(dataCenter)) {
            for (auto* node : nodeTracker->GetRackNodes(rack)) {
                OnNodeUpdated(node);
            }
        }
    }
}

TError TChunkPlacement::ComputeDataCenterFaultiness(
    const TDataCenter* dataCenter,
    const NNodeTrackerClient::TAggregatedNodeStatistics& dataCenterStatistics,
    bool dataCenterIsEnabled) const
{
    const auto& config = GetDynamicConfig()->DataCenterFailureDetector;

    const auto& dataCenterThresholdsMap = config->DataCenterThresholds;
    auto dataCenterThresholds = GetOrDefault(
        dataCenterThresholdsMap,
        dataCenter->GetName(),
        config->DefaultThresholds);

    auto onlineNodeCount = dataCenterStatistics.OnlineNodeCount;
    auto targetOnlineNodeCount = dataCenterIsEnabled
        ? dataCenterThresholds->OnlineNodeCountToDisable
        : dataCenterThresholds->OnlineNodeCountToEnable;
    if (onlineNodeCount < targetOnlineNodeCount) {
        auto error = TError(
            "Storage data center %Qv considered faulty: %v, needed >= %v but got %v",
            dataCenter->GetName(),
            dataCenterIsEnabled ? "enough offline nodes to disable" : "too few online nodes to enable",
            targetOnlineNodeCount,
            onlineNodeCount);
        return error;
    }

    auto totalNodeCount = onlineNodeCount + dataCenterStatistics.OfflineNodeCount;
    auto onlineFraction = static_cast<double>(onlineNodeCount) / totalNodeCount;
    auto targetFraction = dataCenterIsEnabled
        ? dataCenterThresholds->OnlineNodeFractionToDisable
        : dataCenterThresholds->OnlineNodeFractionToEnable;
    if (onlineFraction < targetFraction) {
        auto error = TError(
            "Storage data center %Qv considered faulty: %v, fraction needed >= %v but got %v",
            dataCenter->GetName(),
            dataCenterIsEnabled ? "enough offline nodes to disable" : "too few online nodes to enable",
            targetFraction,
            onlineFraction);
        return error;
    }

    return {};
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
