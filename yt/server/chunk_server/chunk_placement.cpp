#include "chunk_placement.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_tracker.h>
#include <yt/server/node_tracker_server/rack.h>

#include <yt/server/object_server/object.h>

#include <yt/core/misc/small_set.h>

#include <util/random/random.h>

#include <array>

namespace NYT {
namespace NChunkServer {

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
        const TChunk* chunk,
        int maxReplicasPerRack,
        const TNodeList* forbiddenNodes)
        : MaxReplicasPerRack_(maxReplicasPerRack)
    {
        if (forbiddenNodes) {
            ForbiddenNodes_ = *forbiddenNodes;
        }

        for (auto replica : chunk->StoredReplicas()) {
            auto* node = replica.GetPtr();
            IncreaseRackUsage(node);
            ForbiddenNodes_.push_back(node);
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
        ForbiddenNodes_.push_back(node);
    }

    const TNodeList& GetAddedNodes() const
    {
        return AddedNodes_;
    }

private:
    const int MaxReplicasPerRack_;

    std::array<i8, MaxRackCount> PerRackCounters_{};
    TNodeList ForbiddenNodes_;
    TNodeList AddedNodes_;


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
    YCHECK(Config_);
    YCHECK(Bootstrap_);
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    if (node->GetLocalState() != ENodeState::Registered &&
        node->GetLocalState() != ENodeState::Online)
        return;

    InsertToLoadFactorMap(node);

    if (node->GetSessionCount(ESessionType::Replication) < Config_->MaxReplicationWriteSessions) {
        InsertToFillFactorMap(node);
    }
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    RemoveFromLoadFactorMap(node);

    RemoveFromFillFactorMap(node);
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    node->ClearSessionHints();

    // Recompute IO weight.
    // Currently its just the number of non-full locations.
    double ioWeight = 0.0;
    for (const auto& location : node->Statistics().locations()) {
        if (!location.full()) {
            ioWeight += 1.0;
        }
    }
    node->SetIOWeight(ioWeight);

    OnNodeUnregistered(node);
    OnNodeRegistered(node);
}

void TChunkPlacement::OnNodeDisposed(TNode* node)
{
    YCHECK(!node->GetLoadFactorIterator());
    YCHECK(!node->GetFillFactorIterator());
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    TNullable<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        chunk,
        desiredCount,
        minCount,
        replicationFactorOverride,
        forbiddenNodes,
        preferredHostName);

    for (auto* target : targetNodes) {
        AddSessionHint(target, sessionType);
    }

    return targetNodes;
}

void TChunkPlacement::InsertToFillFactorMap(TNode* node)
{
    RemoveFromFillFactorMap(node);

    double fillFactor = node->GetFillFactor();
    auto it = FillFactorToNode_.insert(std::make_pair(fillFactor, node));
    node->SetFillFactorIterator(it);
}

void TChunkPlacement::RemoveFromFillFactorMap(TNode* node)
{
    if (node->GetFillFactorIterator()) {
        FillFactorToNode_.erase(*node->GetFillFactorIterator());
        node->SetFillFactorIterator(Null);
    }
}

void TChunkPlacement::InsertToLoadFactorMap(TNode* node)
{
    RemoveFromLoadFactorMap(node);

    double loadFactor = node->GetLoadFactor();
    auto it = LoadFactorToNode_.insert(std::make_pair(loadFactor, node));
    node->SetLoadFactorIterator(it);
}

void TChunkPlacement::RemoveFromLoadFactorMap(TNode* node)
{
    if (node->GetLoadFactorIterator()) {
        LoadFactorToNode_.erase(*node->GetLoadFactorIterator());
        node->SetLoadFactorIterator(Null);
    }
}

TNodeList TChunkPlacement::GetWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    TNullable<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName)
{
    int maxReplicasPerRack = GetMaxReplicasPerRack(chunk, replicationFactorOverride);
    TTargetCollector collector(chunk, maxReplicasPerRack, forbiddenNodes);

    auto tryAdd = [&] (TNode* node, bool enableRackAwareness) {
        if (IsValidWriteTarget(node, chunk->GetType(), &collector, enableRackAwareness)) {
            collector.AddNode(node);
        }
    };

    auto tryAddAll = [&] (bool enableRackAwareness) {
        for (const auto& pair : LoadFactorToNode_) {
            auto* node = pair.second;
            if (collector.GetAddedNodes().size() == desiredCount)
                break;
            tryAdd(node, enableRackAwareness);
        }
    };

    if (preferredHostName) {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode && preferredNode->GetLocalState() == ENodeState::Online) {
            tryAdd(preferredNode, true);
        }
    }

    tryAddAll(true);
    tryAddAll(false);

    const auto& nodes = collector.GetAddedNodes();
    return nodes.size() < minCount ? TNodeList() : nodes;
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    TNullable<int> replicationFactorOverride,
    ESessionType sessionType)
{
    auto targetNodes = GetWriteTargets(
        chunk,
        desiredCount,
        minCount,
        replicationFactorOverride);

    for (auto* target : targetNodes) {
        AddSessionHint(target, sessionType);
    }

    return targetNodes;
}

TNode* TChunkPlacement::GetRemovalTarget(TChunkPtrWithIndex chunkWithIndex)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int maxReplicasPerRack = GetMaxReplicasPerRack(chunk, Null);

    std::array<i8, MaxRackCount> perRackCounters{};
    for (auto replica : chunk->StoredReplicas()) {
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
        if (chunk->IsRegular() ||
            chunk->IsErasure() && replica.GetIndex() == chunkWithIndex.GetIndex() ||
            chunk->IsJournal()) // allow removing arbitrary journal replicas
        {
            auto* node = replica.GetPtr();
            if (!IsValidRemovalTarget(node))
                continue;

            const auto* rack = node->GetRack();
            if (rack && perRackCounters[rack->GetIndex()] > maxReplicasPerRack) {
                rackWinner = node;
            }

            if (!fillFactorWinner || node->GetFillFactor() > fillFactorWinner->GetFillFactor()) {
                fillFactorWinner = node;
            }
        }
    }

    return rackWinner ? rackWinner : fillFactorWinner;
}

bool TChunkPlacement::HasBalancingTargets(double maxFillFactor)
{
    if (maxFillFactor < 0) {
        return false;
    }

    if (FillFactorToNode_.empty()) {
        return false;
    }

    auto* node = FillFactorToNode_.begin()->second;
    return node->GetFillFactor() < maxFillFactor;
}

TNode* TChunkPlacement::AllocateBalancingTarget(
    TChunk* chunk,
    double maxFillFactor)
{
    auto* target = GetBalancingTarget(chunk, maxFillFactor);

    if (target) {
        AddSessionHint(target, ESessionType::Replication);
    }

    return target;
}

TNode* TChunkPlacement::GetBalancingTarget(
    TChunk* chunk,
    double maxFillFactor)
{
    int maxReplicasPerRack = GetMaxReplicasPerRack(chunk, Null);
    TTargetCollector collector(chunk, maxReplicasPerRack, nullptr);

    for (const auto& pair : FillFactorToNode_) {
        auto* node = pair.second;
        if (node->GetFillFactor() > maxFillFactor) {
            break;
        }
        if (IsValidBalancingTarget(node, chunk->GetType(), &collector, true)) {
            return node;
        }
    }

    return nullptr;
}

bool TChunkPlacement::IsValidWriteTarget(
    TNode* node,
    EObjectType chunkType,
    TTargetCollector* collector,
    bool enableRackAwareness)
{
    if (node->GetLocalState() != ENodeState::Online) {
        // Do not write anything to a node before its first heartbeat or after it is unregistered.
        return false;
    }

    if (node->IsFull()) {
        // Do not write anything to full nodes.
        return false;
    }

    if (!IsAcceptedChunkType(node, chunkType)) {
        // Do not write anything to nodes not accepting this type of chunks.
        return false;
    }

    if (node->GetDecommissioned()) {
        // Do not write anything to decommissioned nodes.
        return false;
    }

    if (!collector->CheckNode(node, enableRackAwareness)) {
        // The collector does not like this node.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(
    TNode* node,
    NObjectClient::EObjectType chunkType,
    TTargetCollector* collector,
    bool enableRackAwareness)
{
    // Balancing implies write, after all.
    if (!IsValidWriteTarget(node, chunkType, collector, enableRackAwareness)) {
        return false;
    }

    if (node->GetSessionCount(ESessionType::Replication) >= Config_->MaxReplicationWriteSessions) {
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

std::vector<TChunkPtrWithIndex> TChunkPlacement::GetBalancingChunks(
    TNode* node,
    int replicaCount)
{
    std::vector<TChunkPtrWithIndex> result;
    result.reserve(replicaCount);

    auto chunkManager = Bootstrap_->GetChunkManager();

    // Let's bound the number of iterations somehow.
    for (int index = 0; index < replicaCount * 2; ++index) {
        auto replica = node->PickRandomReplica();
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
        if (chunk->GetRefreshScheduled()) {
            continue;
        }
        if (chunk->IsJobScheduled()) {
            continue;
        }
        result.push_back(replica);
    }

    return result;
}

bool TChunkPlacement::IsAcceptedChunkType(TNode* node, EObjectType type)
{
    for (auto acceptedType : node->Statistics().accepted_chunk_types()) {
        if (EObjectType(acceptedType) == type) {
            return true;
        }
    }
    return false;
}

void TChunkPlacement::AddSessionHint(TNode* node, ESessionType sessionType)
{
    node->AddSessionHint(sessionType);

    RemoveFromLoadFactorMap(node);
    InsertToLoadFactorMap(node);

    if (node->GetSessionCount(ESessionType::Replication) >= Config_->MaxReplicationWriteSessions) {
        RemoveFromFillFactorMap(node);
    }
}

int TChunkPlacement::GetMaxReplicasPerRack(TChunk* chunk, TNullable<int> replicationFactorOverride)
{
    return std::min(
        Config_->MaxReplicasPerRack,
        chunk->GetMaxReplicasPerRack(replicationFactorOverride));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
