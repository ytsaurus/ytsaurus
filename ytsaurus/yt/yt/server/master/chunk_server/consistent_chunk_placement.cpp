#include "consistent_chunk_placement.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <util/generic/algorithm.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NHydra;
using namespace NObjectServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

static const int MaxNodeAddressWithSuffixLength = 100;

///////////////////////////////////////////////////////////////////////////////

TConsistentChunkPlacement::TChunkPlacementGroupKey::operator size_t() const
{
    auto result = Hash;
    HashCombine(result, MediumIndex);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TConsistentChunkPlacement::TChunkPlacementGroup::TChunkPlacementGroup(TChunk* chunk)
    : Chunks_(1, chunk)
{ }

void TConsistentChunkPlacement::TChunkPlacementGroup::AddChunk(TChunk *chunk)
{
    auto [it, ite] = std::equal_range(Chunks_.begin(), Chunks_.end(), chunk, TObjectIdComparer());
    YT_VERIFY(it == ite); // Should not be adding same chunk twice.
    Chunks_.insert(it, chunk);
    YT_ASSERT(std::is_sorted(Chunks_.begin(), Chunks_.end(), TObjectIdComparer()));
}

void TConsistentChunkPlacement::TChunkPlacementGroup::RemoveChunk(TChunk *chunk, bool missingOk)
{
    auto [it, ite] = std::equal_range(Chunks_.begin(), Chunks_.end(), chunk, TObjectIdComparer());
    YT_VERIFY(missingOk || it != ite);
    Chunks_.erase(it, ite);
    YT_ASSERT(std::is_sorted(Chunks_.begin(), Chunks_.end(), TObjectIdComparer()));
}

bool TConsistentChunkPlacement::TChunkPlacementGroup::ContainsChunk(TChunk* chunk) const
{
    auto [it, ite] = std::equal_range(Chunks_.begin(), Chunks_.end(), chunk, TObjectIdComparer());
    return it != ite;
}

////////////////////////////////////////////////////////////////////////////////

bool TConsistentChunkPlacement::TComparer::operator()(
    NNodeTrackerServer::TNode* lhs,
    NNodeTrackerServer::TNode* rhs) const
{
    return TObjectIdComparer::Compare(lhs, rhs);
}

bool TConsistentChunkPlacement::TComparer::operator()(
    const TChunkPlacementGroup* lhs,
    const TChunkPlacementGroup* rhs) const
{
    // Empty groups are meaningless and should not exist at all.
    YT_VERIFY(!lhs->Chunks().empty());
    YT_VERIFY(!rhs->Chunks().empty());

    auto* lhsFirstChunk = lhs->Chunks().front();
    auto* rhsFirstChunk = rhs->Chunks().front();
    return TObjectIdComparer::Compare(lhsFirstChunk, rhsFirstChunk);
}

////////////////////////////////////////////////////////////////////////////////

ui64 TConsistentChunkPlacement::THasher::operator()(NNodeTrackerServer::TNode* node, int index) const
{
    // NB: using default address instead of ID here with an eye towards
    // someday supporting masterless chunk locate.
    const auto& address = node->GetDefaultAddress();
    // NB: Attempting to avoid serial collisions. That is, if two nodes
    // collide for a particular index, it's ok. But if they collide for
    // all indexes - that's not great.
    // Hence not using HashCombine here.
    TRawFormatter<MaxNodeAddressWithSuffixLength> addressWithSuffixFormatter;
    addressWithSuffixFormatter.AppendString(address);
    addressWithSuffixFormatter.AppendNumber(index);
    TStringBuf addressWithSuffix(
        addressWithSuffixFormatter.GetData(),
        addressWithSuffixFormatter.GetBytesWritten());
    return ::THash<TStringBuf>()(addressWithSuffix);
}

ui64 TConsistentChunkPlacement::THasher::operator()(const TChunkPlacementGroup* group, int index) const
{
    YT_VERIFY(!group->Chunks().empty());
    // NB: all chunks in a group have the same hash.
    auto result = group->Chunks().front()->GetConsistentReplicaPlacementHash();
    HashCombine(result, index);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TConsistentChunkPlacement::TConsistentChunkPlacement(
    TBootstrap* bootstrap,
    int chunkReplicaCount)
    : Bootstrap_(bootstrap)
    , SufficientlyLargeReplicaCount_(chunkReplicaCount)
{ }

void TConsistentChunkPlacement::AddChunk(TChunk* chunk) noexcept
{
    YT_VERIFY(HasHydraContext());
    YT_VERIFY(chunk->HasConsistentReplicaPlacementHash());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
    const auto& replication = chunk->GetAggregatedReplication(requisitionRegistry);

    for (const auto& entry : replication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto mediumPolicy = entry.Policy();
        YT_VERIFY(mediumPolicy);

        auto [it, emplaced] = PlacementGroups_.emplace(
            TChunkPlacementGroupKey{
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex},
            TChunkPlacementGroup(chunk));
        auto& placementGroup = it->second;
        if (emplaced) {
            YT_ASSERT(placementGroup.Chunks().size() == 1 && placementGroup.Chunks().front() == chunk);

            Rings_[mediumIndex].AddFile(&placementGroup, SufficientlyLargeReplicaCount_);

            YT_LOG_DEBUG("Chunk placement group created (ChunkId: %v, ConsistentReplicaPlacementHash: %x, MediumIndex: %v, ReplicationFactor: %v(%v), TotalPlacementGroupCount: %v)",
                chunk->GetId(),
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex,
                chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry),
                SufficientlyLargeReplicaCount_,
                PlacementGroups_.size());
        } else {
            placementGroup.AddChunk(chunk);

            YT_LOG_DEBUG("Chunk added to placement group (ChunkId: %v, ConsistentReplicaPlacementHash: %x, MediumIndex: %v, ReplicationFactor: %v(%v), TotalPlacementGroupCount: %v)",
                chunk->GetId(),
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex,
                chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry),
                SufficientlyLargeReplicaCount_,
                PlacementGroups_.size());
        }
    }
}

void TConsistentChunkPlacement::RemoveChunk(
    TChunk* chunk,
    const std::optional<TChunkReplication>& replication,
    bool missingOk) noexcept
{
    YT_VERIFY(HasHydraContext());
    YT_VERIFY(chunk->HasConsistentReplicaPlacementHash());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

    auto* effectiveReplication = replication ? &*replication : &chunk->GetAggregatedReplication(requisitionRegistry);

    for (const auto& entry : *effectiveReplication) {
        auto mediumIndex = entry.GetMediumIndex();
        auto mediumPolicy = entry.Policy();
        YT_VERIFY(mediumPolicy);

        auto it = PlacementGroups_.find(
            TChunkPlacementGroupKey{
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex});

        if (it == PlacementGroups_.end()) {
            if (!missingOk) {
                YT_LOG_ALERT("Placement group not found for the chunk (ChunkId: %v, ConsistentChunkPlacementHash: %x, MediumIndex: %v, ReplicationFactor: %v(%v))",
                    chunk->GetId(),
                    chunk->GetConsistentReplicaPlacementHash(),
                    mediumIndex,
                    chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry),
                    SufficientlyLargeReplicaCount_);
            }
            continue;
        }

        auto& placementGroup = it->second;
        YT_ASSERT(placementGroup.ContainsChunk(chunk) || missingOk);
        auto isLastChunk = placementGroup.Chunks().size() == 1 && placementGroup.Chunks().front() == chunk;

        if (isLastChunk) {
            Rings_[mediumIndex].RemoveFile(&placementGroup, SufficientlyLargeReplicaCount_);
        }

        placementGroup.RemoveChunk(chunk, missingOk);

        if (isLastChunk) {
            YT_VERIFY(placementGroup.Chunks().empty());
            PlacementGroups_.erase(it);

            YT_LOG_DEBUG("Chunk placement group destroyed (ChunkId: %v, ConsistentChunkPlacementHash: %x, MediumIndex: %v, ReplicationFactor: %v(%v), TotalPlacementGroupCount: %v)",
                chunk->GetId(),
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex,
                chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry),
                SufficientlyLargeReplicaCount_,
                PlacementGroups_.size());
        } else {
            YT_LOG_DEBUG("Chunk removed from placement group (ChunkId: %v, ConsistentChunkPlacementHash: %x, MediumIndex: %v, ReplicationFactor: %v(%v), TotalPlacementGroupCount: %v)",
                chunk->GetId(),
                chunk->GetConsistentReplicaPlacementHash(),
                mediumIndex,
                chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry),
                SufficientlyLargeReplicaCount_,
                PlacementGroups_.size());
        }
    }
}

std::vector<TChunk*> TConsistentChunkPlacement::AddNode(TNode* node)
{
    YT_VERIFY(HasHydraContext());
    YT_ASSERT(node->IsValidWriteTarget());

    std::vector<TChunk*> result;
    for (auto [mediumIndex, tokenCount] : node->ConsistentReplicaPlacementTokenCount()) {
        if (tokenCount == 0) {
            continue;
        }

        auto oldResultSize = std::ssize(result);
        auto affectedGroupCount = DoAddNode(node, mediumIndex, tokenCount, &result);

        YT_LOG_DEBUG("Node added to consistent placement ring (Node: %v, MediumIndex: %v, TokenCount: %v, AffectedPlacementGroupCount: %v, AffectedChunkCount: %v, TotalPlacementGroupCount: %v)",
            node->GetDefaultAddress(),
            mediumIndex,
            tokenCount,
            affectedGroupCount,
            std::ssize(result) - oldResultSize,
            PlacementGroups_.size());
    }

    SortUnique(result);
    return result;
}

int TConsistentChunkPlacement::DoAddNode(
    TNode* node,
    int mediumIndex,
    int tokenCount,
    std::vector<TChunk*>* affectedChunks)
{
    YT_ASSERT(tokenCount != 0);

    auto& mediumRing = Rings_[mediumIndex];

    mediumRing.AddServer(node, tokenCount);

    auto affectedGroupCount = 0;
    auto affectedGroupRange = mediumRing.GetFileRangeForServer(node, tokenCount);
    for (; affectedGroupRange.first != affectedGroupRange.second; ++affectedGroupRange.first) {
        auto* group = *affectedGroupRange.first;
        affectedChunks->insert(affectedChunks->end(), group->Chunks().begin(), group->Chunks().end());
        ++affectedGroupCount;
    }

    return affectedGroupCount;
}

std::vector<TChunk*> TConsistentChunkPlacement::RemoveNode(TNode* node)
{
    YT_VERIFY(HasHydraContext());

    std::vector<TChunk*> result;
    for (auto [mediumIndex, tokenCount] : node->ConsistentReplicaPlacementTokenCount()) {
        if (tokenCount == 0) {
            continue;
        }

        auto oldResultSize = std::ssize(result);
        auto affectedGroupCount = DoRemoveNode(node, mediumIndex, tokenCount, &result);

        YT_LOG_DEBUG("Node removed from consistent placement ring (Node: %v, MediumIndex: %v, TokenCount: %v, AffectedPlacementGroupCount: %v, AffectedChunkCount: %v, TotalPlacementGroupCount: %v)",
            node->GetDefaultAddress(),
            mediumIndex,
            tokenCount,
            affectedGroupCount,
            std::ssize(result) - oldResultSize,
            PlacementGroups_.size());
    }

    SortUnique(result);
    return result;
}

int TConsistentChunkPlacement::DoRemoveNode(TNode* node, int mediumIndex, int tokenCount, std::vector<TChunk*>* affectedChunks)
{
    YT_ASSERT(tokenCount != 0);

    auto& mediumRing = Rings_[mediumIndex];

    auto affectedGroupCount = 0;
    auto affectedGroupRange = mediumRing.GetFileRangeForServer(node, tokenCount);
    for (; affectedGroupRange.first != affectedGroupRange.second; ++affectedGroupRange.first) {
        auto* group = *affectedGroupRange.first;
        affectedChunks->insert(affectedChunks->end(), group->Chunks().begin(), group->Chunks().end());
        ++affectedGroupCount;
    }

    mediumRing.RemoveServer(node, tokenCount);

    return affectedGroupCount;

}

std::vector<TChunk*> TConsistentChunkPlacement::UpdateNodeTokenCount(
    TNode* node,
    int mediumIndex,
    i64 oldTokenCount,
    i64 newTokenCount)
{
    YT_VERIFY(HasMutationContext());

    std::vector<TChunk*> result;

    if (oldTokenCount != 0) {
        DoRemoveNode(node, mediumIndex, oldTokenCount, &result);
    }

    if (newTokenCount != 0) {
        DoAddNode(node, mediumIndex, newTokenCount, &result);
    }

    SortUnique(result);
    return result;
}

void TConsistentChunkPlacement::SetChunkReplicaCount(int replicaCount)
{
    if (replicaCount == SufficientlyLargeReplicaCount_) {
        return;
    }

    auto oldReplicaCount = SufficientlyLargeReplicaCount_;
    SufficientlyLargeReplicaCount_ = replicaCount;

    for (auto& [groupKey, group] : PlacementGroups_) {
        auto mediumIndex = groupKey.MediumIndex;
        auto& mediumRing = Rings_[mediumIndex];
        mediumRing.RemoveFile(&group, oldReplicaCount);
        mediumRing.AddFile(&group, SufficientlyLargeReplicaCount_);
    }

    YT_LOG_DEBUG("CRP chunk replica count changed (OldReplicaCount: %v, NewReplicaCount: %v, PlacementGroupCount: %v)",
        oldReplicaCount,
        SufficientlyLargeReplicaCount_,
        PlacementGroups_.size());
}

void TConsistentChunkPlacement::Clear()
{
    PlacementGroups_.clear();
    Rings_.clear();
}

TNodeList TConsistentChunkPlacement::GetWriteTargets(const TChunk* chunk, int mediumIndex) const
{
    YT_VERIFY(chunk->HasConsistentReplicaPlacementHash());

    struct TClashAwareness
    {
        bool operator()(NNodeTrackerServer::TNode* node)
        {
            if (IsNodeSeen(node)) {
                return false;
            }

            SeenNodes_.push_back(node);
            return true;
        }

        bool IsNodeSeen(NNodeTrackerServer::TNode* node) const
        {
            return std::find(SeenNodes_.begin(), SeenNodes_.end(), node) != SeenNodes_.end();
        }

    protected:
        TCompactVector<NNodeTrackerServer::TNode*, TypicalReplicaCount> SeenNodes_;
    };

    struct TRackAwareness
        : public TClashAwareness
    {
        bool operator()(NNodeTrackerServer::TNode* node)
        {
            if (IsNodeSeen(node)) {
                return false;
            }

            if (auto* rack = node->GetRack(); IsObjectAlive(rack) && ++PerRackCounters_[rack->GetIndex()] > 1) {
                return false;
            }

            SeenNodes_.push_back(node);
            return true;
        }

    private:
        std::array<i8, NNodeTrackerServer::RackIndexBound> PerRackCounters_{};
    };

    auto placementGroupIt = PlacementGroups_.find(
        TChunkPlacementGroupKey{
            chunk->GetConsistentReplicaPlacementHash(),
            mediumIndex});
    if (placementGroupIt == PlacementGroups_.end()) {
        YT_LOG_ALERT("Consistent chunk placement was requested for unknown group, ignored "
            "(ChunkId: %v, ConsistentReplicaPlacementHash: %x, MediumIndex: %v)",
            chunk->GetId(),
            chunk->GetConsistentReplicaPlacementHash(),
            mediumIndex);
        return {};
    }
    auto& placementGroup = placementGroupIt->second;

    auto ringIt = Rings_.find(mediumIndex);
    YT_VERIFY(ringIt != Rings_.end());
    auto& mediumRing = ringIt->second;
    auto nodeCandidates = mediumRing.GetServersForFile(&placementGroup, SufficientlyLargeReplicaCount_);

    if (nodeCandidates.empty()) {
        YT_VERIFY(mediumRing.GetTokenCount() == 0);
        return nodeCandidates;
    }

    YT_VERIFY(std::ssize(nodeCandidates) == SufficientlyLargeReplicaCount_);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
    auto replicationFactor = chunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry);

    if (replicationFactor > SufficientlyLargeReplicaCount_) {
        YT_LOG_ALERT("CRP's \"replicas_per_chunk\" parameter is less than a chunk's replication factor "
            "(ReplicasPerChunk: %v, ChunkId: %v, MediumIndex: %v, ReplicationFactor: %v)",
            SufficientlyLargeReplicaCount_,
            chunk->GetId(),
            mediumIndex,
            replicationFactor);
    }

    TNodeList result;
    TRackAwareness rackAwareness;
    for (auto* node : nodeCandidates) {
        if (!rackAwareness(node)) {
            continue;
        }

        result.push_back(node);
        if (std::ssize(result) == replicationFactor) {
            break;
        }
    }

    if (std::ssize(result) == replicationFactor) {
        return result;
    }

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto dataNodeCount = std::ssize(nodeTracker->GetNodesWithFlavor(ENodeFlavor::Data));

    if (dataNodeCount >= replicationFactor &&
        dataNodeCount >= ChunkReplicaIndexBound)
    {
        YT_LOG_WARNING("CRP nodes do not satisfy rack awareness constraints (ChunkId: %v, MediumIndex: %v, ReplicationFactor: %v(%v), DataNodeCount: %v)",
            chunk->GetId(),
            mediumIndex,
            replicationFactor,
            SufficientlyLargeReplicaCount_,
            dataNodeCount);
    }

    result.clear();
    TClashAwareness clashAwareness;
    for (auto* node : nodeCandidates) {
        if (!clashAwareness(node)) {
            continue;
        }

        result.push_back(node);
        if (std::ssize(result) == replicationFactor) {
            break;
        }
    }

    if (std::ssize(result) == replicationFactor) {
        return result;
    }

    if (dataNodeCount >= replicationFactor) {
        YT_LOG_WARNING("CRP nodes do not satisfy clash constraints (ChunkId: %v, MediumIndex: %v, ReplicationFactor: %v(%v), DataNodeCount: %v, AllocatedTargetNodeCount: %v)",
            chunk->GetId(),
            mediumIndex,
            replicationFactor,
            SufficientlyLargeReplicaCount_,
            dataNodeCount,
            std::ssize(result));
    }

    result.clear();
    auto it = nodeCandidates.begin();
    while (std::ssize(result) < replicationFactor) {
        result.push_back(*it++);
        if (it == nodeCandidates.end()) {
            // NB: this cyclic repetition is just for the rare case of misconfiguration,
            // when CRP's replicas per chunk is less then RF. It has been log-alerted above.
            it = nodeCandidates.begin();
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
