#include "stdafx.h"
#include "chunk_placement.h"
#include "chunk.h"
#include "job.h"
#include "chunk_manager.h"
#include "private.h"

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/node_tracker.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);

    auto nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& pair : nodeTracker->Nodes()) {
        auto* node = pair.second;
        OnNodeRegistered(node);
    }
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    // Maintain LoadRankToNode_.
    {
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

    // Maintain FillFactorToNode_.
    if (node->GetSessionCount(EWriteSessionType::Replication) < Config_->MaxReplicationWriteSessions) {
        double fillFactor = GetFillFactor(node);
        auto it = FillFactorToNode_.insert(std::make_pair(fillFactor, node));
        YCHECK(NodeToFillFactorIt_.insert(std::make_pair(node, it)).second);
    }
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    // Maintain LoadRankToNode_.
    {
        for (int i = node->GetLoadRank(); i < LoadRankToNode_.size() - 1; i++) {
            LoadRankToNode_[i] = LoadRankToNode_[i + 1];
            LoadRankToNode_[i]->SetLoadRank(i);
        }
        LoadRankToNode_.resize(LoadRankToNode_.size() - 1);
        node->SetLoadRank(-1);
    }

    // Maintain FillFactorToNode_.
    {
        auto itIt = NodeToFillFactorIt_.find(node);
        if (itIt != NodeToFillFactorIt_.end()) {
            auto it = itIt->second;
            FillFactorToNode_.erase(it);
            NodeToFillFactorIt_.erase(itIt);
        }
    }
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    node->ResetHints();
    OnNodeUnregistered(node);
    OnNodeRegistered(node);
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    int targetCount,
    const TNodeSet* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    EWriteSessionType sessionType,
    EObjectType chunkType)
{
    auto targets = GetWriteTargets(
        targetCount,
        forbiddenNodes,
        preferredHostName,
        EWriteSessionType::User,
        chunkType);

    for (auto* target : targets) {
        AddSessionHint(target, sessionType);
    }

    return targets;
}

int TChunkPlacement::GetLoadFactor(TNode* node)
{
    return node->GetTotalSessionCount();
}

TNodeList TChunkPlacement::GetWriteTargets(
    int targetCount,
    const TNodeSet* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    EWriteSessionType sessionType,
    EObjectType chunkType)
{
    TNodeList targets;

    if (preferredHostName) {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode &&
            IsValidWriteTarget(preferredNode, sessionType, chunkType) &&
            (!forbiddenNodes || forbiddenNodes->count(preferredNode) == 0))
        {
            targets.push_back(preferredNode);
        }
    }

    for (auto* node : LoadRankToNode_) {
        if (targets.size() == targetCount)
            break;
        if (!IsValidWriteTarget(node, sessionType, chunkType))
            continue;
        if (!targets.empty() && targets[0] == node)
            continue; // skip preferred node
        if (forbiddenNodes && forbiddenNodes->count(node))
            continue;
        targets.push_back(node);
    }

    if (targets.size() != targetCount) {
        targets.clear();
    }

    return targets;
}

TNodeList TChunkPlacement::AllocateWriteTargets(
    TChunk* chunk,
    int targetCount,
    EWriteSessionType sessionType,
    EObjectType chunkType)
{
    auto targets = GetWriteTargets(
        chunk,
        targetCount,
        sessionType,
        chunkType);

    for (auto* target : targets) {
        AddSessionHint(target, sessionType);
    }

    return targets;
}

TNodeList TChunkPlacement::GetWriteTargets(
    TChunk* chunk,
    int targetCount,
    EWriteSessionType sessionType,
    EObjectType chunkType)
{
    TNodeSet forbiddenNodes;

    auto nodeTracker = Bootstrap_->GetNodeTracker();
    auto chunkManager = Bootstrap_->GetChunkManager();

    for (auto replica : chunk->StoredReplicas()) {
        forbiddenNodes.insert(replica.GetPtr());
    }

    auto jobList = chunkManager->FindJobList(chunk);
    if (jobList) {
        for (const auto& job : jobList->Jobs()) {
            auto type = job->GetType();
            if (type == EJobType::ReplicateChunk || type == EJobType::RepairChunk) {
                for (const auto& targetAddress : job->TargetAddresses()) {
                    auto* targetNode = nodeTracker->FindNodeByAddress(targetAddress);
                    if (targetNode) {
                        forbiddenNodes.insert(targetNode);
                    }
                }
            }
        }
    }

    return GetWriteTargets(targetCount, &forbiddenNodes, Null, sessionType, chunkType);
}

TNodeList TChunkPlacement::GetRemovalTargets(
    TChunkPtrWithIndex chunkWithIndex,
    int replicaCount)
{
    TNodeList targets;

    // Construct a list of |(node, fillFactor)| pairs.
    typedef std::pair<TNode*, double> TCandidatePair;
    SmallVector<TCandidatePair, TypicalReplicaCount> candidates;
    auto* chunk = chunkWithIndex.GetPtr();
    for (auto replica : chunk->StoredReplicas()) {
        if (chunk->IsRegular() ||
            chunk->IsErasure() && replica.GetIndex() == chunkWithIndex.GetIndex() ||
            chunk->IsJournal()) // allow removing arbitrary journal replicas
        {
            auto* node = replica.GetPtr();
            double fillFactor = GetFillFactor(node);
            candidates.push_back(std::make_pair(node, fillFactor));
        }
    }

    // Sort by fillFactor in descending order.
    std::sort(
        candidates.begin(),
        candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs) {
            return lhs.second > rhs.second;
        });

    // Take top |replicaCount| nodes.
    targets.reserve(replicaCount);
    for (const auto& pair : candidates) {
        if (static_cast<int>(targets.size()) >= replicaCount) {
            break;
        }

        auto* node = pair.first;
        if (IsValidRemovalTarget(node)) {
            targets.push_back(node);
        }
    }

    return targets;
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
    double maxFillFactor,
    EObjectType chunkType)
{
    auto* target = GetBalancingTarget(
        chunkWithIndex,
        maxFillFactor,
        chunkType);

    if (target) {
        AddSessionHint(target, EWriteSessionType::Replication);
    }

    return target;
}

TNode* TChunkPlacement::GetBalancingTarget(
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor,
    EObjectType chunkType)
{
    auto chunkManager = Bootstrap_->GetChunkManager();
    for (const auto& pair : FillFactorToNode_) {
        auto* node = pair.second;
        if (GetFillFactor(node) > maxFillFactor) {
            break;
        }
        if (IsValidBalancingTarget(node, chunkWithIndex, chunkType)) {
            return node;
        }
    }
    return nullptr;
}

bool TChunkPlacement::IsValidWriteTarget(
    TNode* node,
    EWriteSessionType sessionType,
    EObjectType chunkType)
{
    if (node->GetState() != ENodeState::Online) {
        // Do not write anything to nodes before first heartbeat.
        return false;
    }

    if (IsFull(node)) {
        // Do not write anything to full nodes.
        return false;
    }

    if (!AcceptsChunkType(node, chunkType)) {
        // Do not write anything to full nodes.
        return false;
    }

    if (node->GetDecommissioned()) {
        // Do not write anything to decommissioned nodes.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(
    TNode* node,
    TChunkPtrWithIndex chunkWithIndex,
    EObjectType chunkType) const
{
    if (!IsValidWriteTarget(node, EWriteSessionType::Replication, chunkType)) {
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
        if (static_cast<int>(result.size()) >= replicaCount) {
            break;
        }
        if (!chunk->GetMovable()) {
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
    return statistics.total_used_space() /
        (1.0 + statistics.total_used_space() + statistics.total_available_space());
}

bool TChunkPlacement::IsFull(TNode* node)
{
    return node->Statistics().full();
}

bool TChunkPlacement::AcceptsChunkType(TNode* node, EObjectType type)
{
    for (auto acceptedType : node->Statistics().accepted_chunk_types()) {
        if (acceptedType == type) {
            return true;
        }
    }
    return false;
}

void TChunkPlacement::AddSessionHint(TNode* node, EWriteSessionType sessionType)
{
    node->AddSessionHint(sessionType);

    // Maintain LoadRankToNode_.
    for (int i = node->GetLoadRank();
         i + 1 < LoadRankToNode_.size() &&
         GetLoadFactor(LoadRankToNode_[i + 1]) < GetLoadFactor(LoadRankToNode_[i]);
         ++i)
    {
        std::swap(LoadRankToNode_[i], LoadRankToNode_[i + 1]);
        LoadRankToNode_[i]->SetLoadRank(i);
        LoadRankToNode_[i + 1]->SetLoadRank(i + 1);
    }

    // Maintain FillFactorToNode_.
    if (node->GetSessionCount(EWriteSessionType::Replication) >= Config_->MaxReplicationWriteSessions) {
        auto itIt = NodeToFillFactorIt_.find(node);
        if (itIt != NodeToFillFactorIt_.end()) {
            auto it = itIt->second;
            FillFactorToNode_.erase(it);
            NodeToFillFactorIt_.erase(itIt);
        }        
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
