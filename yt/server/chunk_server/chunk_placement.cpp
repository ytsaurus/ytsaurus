#include "stdafx.h"
#include "chunk_placement.h"
#include "chunk.h"
#include "job.h"
#include "chunk_manager.h"
#include "private.h"

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/rack.h>
#include <server/node_tracker_server/node_tracker.h>

#include <server/object_server/object.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>

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

class TChunkPlacement::TTargetChecker
{
public:
    TTargetChecker(
        const TChunk* chunk,
        const TSortedNodeList* forbiddenNodes)
        : ForbiddenNodes_(forbiddenNodes)
        , MaxReplicasPerRack_(chunk->GetMaxReplicasPerRack())
    {
        for (auto replica : chunk->StoredReplicas()) {
            Add(replica.GetPtr());
        }
    }

    bool Check(const TNode* node) const
    {
        if (ForbiddenNodes_ && std::binary_search(ForbiddenNodes_->begin(), ForbiddenNodes_->end(), node)) {
            return false;
        }

        const auto* rack = node->GetRack();
        if (rack && PerRackCounters_[rack->GetIndex()] >= MaxReplicasPerRack_) {
            return false;
        }

        return true;
    }

    void Add(const TNode* node)
    {
        const auto* rack = node->GetRack();
        if (rack) {
            ++PerRackCounters_[rack->GetIndex()];
        }
    }

private:
    const TSortedNodeList* const ForbiddenNodes_;
    const int MaxReplicasPerRack_;
    std::array<i8, MaxRackCount> PerRackCounters_{};

};

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TChunkPlacement::Start()
{
    auto nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& pair : nodeTracker->Nodes()) {
        auto* node = pair.second;
        switch (node->GetState()) {
            case ENodeState::Registered:
            case ENodeState::Online:
                OnNodeRegistered(node);
                break;
            case ENodeState::Unregistered:
                // Some nodes may be in "unregistered" state on leader startup;
                // these will be pushed to removed state soon.
                break;
            default:
                YUNREACHABLE();
        }
    }
}

void TChunkPlacement::Stop()
{
    auto nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& pair : nodeTracker->Nodes()) {
        auto* node = pair.second;
        // NB: Mostly equivalent to OnNodeUnregistered but runs faster.
        node->SetLoadRank(-1);
        node->SetFillFactorIterator(Null);
    }

    // NB: This is redundant since the instance is never reused.
    LoadRankToNode_.clear();
    FillFactorToNode_.clear();
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    InsertToLoadRankList(node);

    if (node->GetSessionCount(EWriteSessionType::Replication) < Config_->MaxReplicationWriteSessions) {
        InsertToFillFactorMap(node);
    }
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    RemoveFromLoadRankList(node);

    RemoveFromFillFactorMap(node);
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    node->ResetSessionHints();

    OnNodeUnregistered(node);
    OnNodeRegistered(node);
}

void TChunkPlacement::OnNodeRemoved(TNode* node)
{
    YCHECK(node->GetLoadRank() < 0);
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    const TSortedNodeList* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    EWriteSessionType sessionType)
{
    auto targets = GetWriteTargets(
        chunk,
        desiredCount,
        minCount,
        forbiddenNodes,
        preferredHostName,
        EWriteSessionType::User);

    for (auto* target : targets) {
        AddSessionHint(target, sessionType);
    }

    return targets;
}

int TChunkPlacement::GetLoadFactor(TNode* node)
{
    return node->GetTotalSessionCount();
}

void TChunkPlacement::InsertToFillFactorMap(TNode* node)
{
    RemoveFromFillFactorMap(node);

    double fillFactor = GetFillFactor(node);
    auto it = FillFactorToNode_.insert(std::make_pair(fillFactor, node));
    node->SetFillFactorIterator(it);
}

void TChunkPlacement::RemoveFromFillFactorMap(TNode* node)
{
    if (!node->GetFillFactorIterator())
        return;
    FillFactorToNode_.erase(*node->GetFillFactorIterator());
    node->SetFillFactorIterator(Null);
}

void TChunkPlacement::InsertToLoadRankList(TNode* node)
{
    RemoveFromLoadRankList(node);

    int loadFactor = GetLoadFactor(node);
    int i = 0;
    while (i < LoadRankToNode_.size() && GetLoadFactor(LoadRankToNode_[i]) < loadFactor) {
        ++i;
    }
    LoadRankToNode_.resize(LoadRankToNode_.size() + 1);
    for (int j = LoadRankToNode_.size() - 1; j > i; --j) {
        LoadRankToNode_[j] = LoadRankToNode_[j - 1];
        LoadRankToNode_[j]->SetLoadRank(j);
    }
    LoadRankToNode_[i] = node;
    node->SetLoadRank(i);
}

void TChunkPlacement::RemoveFromLoadRankList(TNode* node)
{
    int loadRank = node->GetLoadRank();
    if (loadRank < 0)
        return;
    for (int i = loadRank; i < LoadRankToNode_.size() - 1; i++) {
        LoadRankToNode_[i] = LoadRankToNode_[i + 1];
        LoadRankToNode_[i]->SetLoadRank(i);
    }
    LoadRankToNode_.resize(LoadRankToNode_.size() - 1);
    node->SetLoadRank(-1);
}

void TChunkPlacement::AdvanceInLoadRankList(TNode* node)
{
    int loadRank = node->GetLoadRank();
    YCHECK(loadRank >= 0);

    for (int i = loadRank;
         i + 1 < LoadRankToNode_.size() &&
         GetLoadFactor(LoadRankToNode_[i + 1]) < GetLoadFactor(LoadRankToNode_[i]);
         ++i)
    {
        std::swap(LoadRankToNode_[i], LoadRankToNode_[i + 1]);
        LoadRankToNode_[i]->SetLoadRank(i);
        LoadRankToNode_[i + 1]->SetLoadRank(i + 1);
    }
}

TNodeList TChunkPlacement::GetWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    const TSortedNodeList* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    EWriteSessionType sessionType)
{
    TNodeList targets;
    TTargetChecker checker(chunk, forbiddenNodes);

    auto checkTarget = [&] (TNode* node) -> bool {
        if (!IsValidWriteTarget(node, chunk, sessionType)) {
            return false;
        }
        if (!checker.Check(node)) {
            return false;
        }
        return true;
    };

    auto addTarget = [&] (TNode* node) {
        targets.push_back(node);
        checker.Add(node);
    };

    if (preferredHostName) {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode && checkTarget(preferredNode)) {
            addTarget(preferredNode);
        }
    }

    for (auto* node : LoadRankToNode_) {
        if (targets.size() == desiredCount)
            break;
        if (!targets.empty() && targets[0] == node)
            continue; // skip preferred node
        if (!checkTarget(node))
            continue;
        addTarget(node);
    }

    if (targets.size()< minCount) {
        targets.clear();
    }

    return targets;
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    EWriteSessionType sessionType)
{
    auto targets = GetWriteTargets(
        chunk,
        desiredCount,
        minCount,
        sessionType);

    for (auto* target : targets) {
        AddSessionHint(target, sessionType);
    }

    return targets;
}

TNodeList TChunkPlacement::GetWriteTargets(
    TChunk* chunk,
    int desiredCount,
    int minCount,
    EWriteSessionType sessionType)
{
    auto nodeTracker = Bootstrap_->GetNodeTracker();
    auto chunkManager = Bootstrap_->GetChunkManager();

    TSortedNodeList forbiddenNodes;

    for (auto replica : chunk->StoredReplicas()) {
        forbiddenNodes.push_back(replica.GetPtr());
    }

    auto jobList = chunkManager->FindJobList(chunk);
    if (jobList) {
        for (const auto& job : jobList->Jobs()) {
            auto type = job->GetType();
            if (type == EJobType::ReplicateChunk || type == EJobType::RepairChunk) {
                for (const auto& targetAddress : job->TargetAddresses()) {
                    auto* targetNode = nodeTracker->FindNodeByAddress(targetAddress);
                    if (targetNode) {
                        forbiddenNodes.push_back(targetNode);
                    }
                }
            }
        }
    }

    std::sort(forbiddenNodes.begin(), forbiddenNodes.end());

    return GetWriteTargets(
        chunk,
        desiredCount,
        minCount,
        &forbiddenNodes,
        Null,
        sessionType);
}

TNode* TChunkPlacement::GetRemovalTarget(TChunkPtrWithIndex chunkWithIndex)
{
    auto* chunk = chunkWithIndex.GetPtr();
    int maxReplicasPerRack = chunk->GetMaxReplicasPerRack();

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

            if (!fillFactorWinner || GetFillFactor(node) > GetFillFactor(fillFactorWinner)) {
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
    return GetFillFactor(node) < maxFillFactor;
}

TNode* TChunkPlacement::AllocateBalancingTarget(
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor)
{
    auto* target = GetBalancingTarget(chunkWithIndex, maxFillFactor);

    if (target) {
        AddSessionHint(target, EWriteSessionType::Replication);
    }

    return target;
}

TNode* TChunkPlacement::GetBalancingTarget(
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor)
{
    TTargetChecker checker(chunkWithIndex.GetPtr(), nullptr);

    for (const auto& pair : FillFactorToNode_) {
        auto* node = pair.second;
        if (GetFillFactor(node) > maxFillFactor) {
            break;
        }
        if (IsValidBalancingTarget(node, chunkWithIndex) && checker.Check(node)) {
            return node;
        }
    }

    return nullptr;
}

bool TChunkPlacement::IsValidWriteTarget(
    TNode* node,
    TChunk* chunk,
    EWriteSessionType sessionType)
{
    if (node->GetState() != ENodeState::Online) {
        // Do not write anything to a node before its first heartbeat or after the it is unregistered.
        return false;
    }

    if (IsFull(node)) {
        // Do not write anything to full nodes.
        return false;
    }

    if (!IsAcceptedChunkType(node, chunk->GetType())) {
        // Do not write anything to full nodes.
        return false;
    }

    if (node->GetDecommissioned()) {
        // Do not write anything to decommissioned nodes.
        return false;
    }

    // Sanity checks.
    YCHECK(node->GetLoadRank() >= 0);
    YCHECK(node->GetFillFactorIterator());

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(
    TNode* node,
    TChunkPtrWithIndex chunkWithIndex) const
{
    if (!IsValidWriteTarget(node, chunkWithIndex.GetPtr(), EWriteSessionType::Replication)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (node->StoredReplicas().find(chunkWithIndex) != node->StoredReplicas().end())  {
        // Do not balance to a node already having the chunk.
        return false;
    }

    auto chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& job : node->Jobs()) {
        if (job->GetChunkIdWithIndex().Id == chunkWithIndex.GetPtr()->GetId()) {
            // Do not balance to a node already having a job associated with this chunk.
            return false;
        }
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidRemovalTarget(TNode* node)
{
    // Always valid :)
    return true;
}

std::vector<TChunkPtrWithIndex> TChunkPlacement::GetBalancingChunks(
    TNode* node,
    int replicaCount)
{
    std::vector<TChunkPtrWithIndex> result;
    result.reserve(replicaCount);

    auto chunkManager = Bootstrap_->GetChunkManager();

    for (auto replica : node->StoredReplicas()) {
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
        if (chunkManager->FindJobList(chunk)) {
            continue;
        }
        result.push_back(replica);
    }

    return result;
}

double TChunkPlacement::GetFillFactor(TNode* node) const
{
    const auto& statistics = node->Statistics();
    auto freeSpace = statistics.total_available_space() - statistics.total_low_watermark_space();
    return
        statistics.total_used_space() /
        std::max(1.0, static_cast<double>(freeSpace + statistics.total_used_space()));
}

bool TChunkPlacement::IsFull(TNode* node)
{
    return node->Statistics().full();
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

void TChunkPlacement::AddSessionHint(TNode* node, EWriteSessionType sessionType)
{
    node->AddSessionHint(sessionType);

    AdvanceInLoadRankList(node);

    if (node->GetSessionCount(EWriteSessionType::Replication) >= Config_->MaxReplicationWriteSessions) {
        RemoveFromFillFactorMap(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
