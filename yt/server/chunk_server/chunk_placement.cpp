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

using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(config);
    YCHECK(bootstrap);
}

void TChunkPlacement::Initialize()
{
    auto nodeTracker = Bootstrap->GetNodeTracker();
    FOREACH (auto* node, nodeTracker->GetNodes()) {
        OnNodeRegistered(node);
    }
}

void TChunkPlacement::OnNodeRegistered(TNode* node)
{
    {
        double loadFactor = GetLoadFactor(node);
        auto it = LoadFactorToNode.insert(std::make_pair(loadFactor, node));
        YCHECK(NodeToLoadFactorIt.insert(std::make_pair(node, it)).second);
    }
    {
        double fillFactor = GetFillFactor(node);
        auto it = FillFactorToNode.insert(std::make_pair(fillFactor, node));
        YCHECK(NodeToFilFactorIt.insert(std::make_pair(node, it)).second);
    }
}

void TChunkPlacement::OnNodeUnregistered(TNode* node)
{
    {
        auto itIt = NodeToLoadFactorIt.find(node);
        YCHECK(itIt != NodeToLoadFactorIt.end());
        auto it = itIt->second;
        LoadFactorToNode.erase(it);
        NodeToLoadFactorIt.erase(itIt);
    }
    {
        auto itIt = NodeToFilFactorIt.find(node);
        YCHECK(itIt != NodeToFilFactorIt.end());
        auto it = itIt->second;
        FillFactorToNode.erase(it);
        NodeToFilFactorIt.erase(itIt);
    }
}

void TChunkPlacement::OnNodeUpdated(TNode* node)
{
    OnNodeUnregistered(node);
    OnNodeRegistered(node);
    node->SetHintedSessionCount(0);
}

void TChunkPlacement::OnSessionHinted(TNode* node)
{
    node->SetHintedSessionCount(node->GetHintedSessionCount() + 1);
}

TNodeList TChunkPlacement::AllocateUploadTargets(
    int targetCount,
    const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName)
{
    auto targets = GetUploadTargets(
        targetCount,
        forbiddenNodes,
        preferredHostName);

    FOREACH (auto* target, targets) {
        OnSessionHinted(target);
    }

    return targets;
}

TNodeList TChunkPlacement::GetUploadTargets(
    int targetCount,
    const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName)
{
    TNodeList targets;

    typedef std::pair<TNode*, int> TFeasibleNode;
    std::vector<TFeasibleNode> feasibleNodes;
    feasibleNodes.reserve(LoadFactorToNode.size());

    TNode* preferredNode = nullptr;
    int remainingCount = targetCount;

    auto nodeTracker = Bootstrap->GetNodeTracker();

    // Look for preferred node first.
    if (preferredHostName) {
        preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode && IsValidUploadTarget(preferredNode)) {
            targets.push_back(preferredNode);
            --remainingCount;
        }
    }

    // Put other feasible nodes to feasibleNodes.
    FOREACH (auto& pair, LoadFactorToNode) {
        auto* node = pair.second;
        if (node != preferredNode &&
            IsValidUploadTarget(node) &&
            !(forbiddenNodes && forbiddenNodes->count(node)))
        {
            feasibleNodes.push_back(std::make_pair(node, node->GetTotalSessionCount()));
        }
    }

    // Take a sample from feasibleNodes.
    std::sort(
        feasibleNodes.begin(),
        feasibleNodes.end(),
        [=] (const TFeasibleNode& lhs, const TFeasibleNode& rhs) {
            return lhs.second < rhs.second;
        });

    auto beginGroupIt = feasibleNodes.begin();
    while (beginGroupIt != feasibleNodes.end() && remainingCount > 0) {
        auto endGroupIt = beginGroupIt;
        int groupSize = 0;
        while (endGroupIt != feasibleNodes.end() && beginGroupIt->second == endGroupIt->second) {
            ++endGroupIt;
            ++groupSize;
        }

        int sampleCount = std::min(remainingCount, groupSize);

        std::vector<TFeasibleNode> currentResult;
        RandomSampleN(
            beginGroupIt,
            endGroupIt,
            std::back_inserter(currentResult),
            sampleCount);

        FOREACH (const auto& feasibleNode, currentResult) {
            targets.push_back(feasibleNode.first);
        }

        beginGroupIt = endGroupIt;
        remainingCount -= sampleCount;
    }

    if (targets.size() != targetCount) {
        targets.clear();
    }

    return targets;
}

TNodeList TChunkPlacement::AllocateReplicationTargets(
    TChunk* chunk,
    int targetCount)
{
    auto targets = GetReplicationTargets(
        chunk,
        targetCount);

    FOREACH (auto* target, targets) {
        OnSessionHinted(target);
    }

    return targets;
}

TNodeList TChunkPlacement::GetReplicationTargets(
    TChunk* chunk,
    int targetCount)
{
    TSmallSet<TNode*, TypicalReplicaCount> forbiddenNodes;

    auto nodeTracker = Bootstrap->GetNodeTracker();
    auto chunkManager = Bootstrap->GetChunkManager();

    FOREACH (auto replica, chunk->StoredReplicas()) {
        forbiddenNodes.insert(replica.GetPtr());
    }

    auto jobList = chunkManager->FindJobList(chunk);
    if (jobList) {
        FOREACH (const auto& job, jobList->Jobs()) {
            auto type = job->GetType();
            if (type == EJobType::ReplicateChunk || type == EJobType::RepairChunk) {
                FOREACH (const auto& targetAddress, job->TargetAddresses()) {
                    auto* targetNode = nodeTracker->FindNodeByAddress(targetAddress);
                    if (targetNode) {
                        forbiddenNodes.insert(targetNode);
                    }
                }
            }
        }
    }

    return GetUploadTargets(targetCount, &forbiddenNodes, nullptr);
}

TNode* TChunkPlacement::GetReplicationSource(TChunkPtrWithIndex chunkWithIndex)
{
    TNodePtrWithIndexList storedReplicas;
    auto* chunk = chunkWithIndex.GetPtr();
    FOREACH (auto storedReplica, chunk->StoredReplicas()) {
        if (storedReplica.GetIndex() == chunkWithIndex.GetIndex()) {
            storedReplicas.push_back(storedReplica);
        }
    }

    // Pick a random location containing a matching replica.
    YCHECK(!storedReplicas.empty());
    int index = RandomNumber<size_t>(storedReplicas.size());
    return storedReplicas[index].GetPtr();
}

TNodeList TChunkPlacement::GetRemovalTargets(
    TChunkPtrWithIndex chunkWithIndex,
    int replicaCount)
{
    TNodeList targets;

    // Construct a list of |(nodeId, loadFactor)| pairs.
    typedef std::pair<TNode*, double> TCandidatePair;
    TSmallVector<TCandidatePair, TypicalReplicaCount> candidates;
    auto* chunk = chunkWithIndex.GetPtr();
    candidates.reserve(chunk->StoredReplicas().size());
    FOREACH (auto replica, chunk->StoredReplicas()) {
        if (replica.GetIndex() == chunkWithIndex.GetIndex()) {
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

    // Take first count nodes.
    targets.reserve(replicaCount);
    FOREACH (const auto& pair, candidates) {
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
    if (maxFillFactor < 0)
        return false;

    if (FillFactorToNode.empty())
        return false;

    auto* node = FillFactorToNode.begin()->second;
    return GetFillFactor(node) < maxFillFactor;
}

TNode* TChunkPlacement::AllocateBalancingTarget(
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor)
{
    auto* target = GetBalancingTarget(
        chunkWithIndex,
        maxFillFactor);

    if (target) {
        OnSessionHinted(target);
    }

    return target;
}

TNode* TChunkPlacement::GetBalancingTarget(
    TChunkPtrWithIndex chunkWithIndex,
    double maxFillFactor)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& pair, FillFactorToNode) {
        auto* node = pair.second;
        if (GetFillFactor(node) > maxFillFactor) {
            break;
        }
        if (IsValidBalancingTarget(node, chunkWithIndex)) {
            return node;
        }
    }
    return nullptr;
}

bool TChunkPlacement::IsValidUploadTarget(TNode* node)
{
    if (node->GetState() != ENodeState::Online) {
        // Do not upload anything to nodes before first heartbeat.
        return false;
    }

    if (IsFull(node)) {
        // Do not upload anything to full nodes.
        return false;
    }

    if (node->GetDecommissioned()) {
        // Do not upload anything to decommissioned nodes.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(TNode* node, TChunkPtrWithIndex chunkWithIndex) const
{
    if (!IsValidUploadTarget(node)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (node->StoredReplicas().find(chunkWithIndex) != node->StoredReplicas().end())  {
        // Do not balance to a node already having the chunk.
        return false;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& job, node->Jobs()) {
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

    auto chunkManager = Bootstrap->GetChunkManager();

    FOREACH (auto replica, node->StoredReplicas()) {
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

double TChunkPlacement::GetLoadFactor(TNode* node) const
{
    return
        GetFillFactor(node) +
        Config->ActiveSessionPenality * node->GetTotalSessionCount();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
