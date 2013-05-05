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
        double fillCoeff = GetFillCoeff(node);
        auto it = FillCoeffToNode.insert(std::make_pair(fillCoeff, node));
        YCHECK(NodeToFillCoeffIt.insert(std::make_pair(node, it)).second);
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
        auto itIt = NodeToFillCoeffIt.find(node);
        YCHECK(itIt != NodeToFillCoeffIt.end());
        auto it = itIt->second;
        FillCoeffToNode.erase(it);
        NodeToFillCoeffIt.erase(itIt);
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

TSmallVector<TNode*, TypicalReplicaCount> TChunkPlacement::GetUploadTargets(
    int replicaCount,
    const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName)
{
    TSmallVector<TNode*, TypicalReplicaCount> resultNodes;
    resultNodes.reserve(replicaCount);

    typedef std::pair<TNode*, int> TFeasibleNode;
    std::vector<TFeasibleNode> feasibleNodes;
    feasibleNodes.reserve(LoadFactorToNode.size());

    TNode* preferredNode = nullptr;

    auto nodeTracker = Bootstrap->GetNodeTracker();

    // Look for preferred node first.
    if (preferredHostName) {
        preferredNode = nodeTracker->FindNodeByHostName(*preferredHostName);
        if (preferredNode && IsValidUploadTarget(preferredNode)) {
            resultNodes.push_back(preferredNode);
            --replicaCount;
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
    while (beginGroupIt != feasibleNodes.end() && replicaCount > 0) {
        auto endGroupIt = beginGroupIt;
        int groupSize = 0;
        while (endGroupIt != feasibleNodes.end() && beginGroupIt->second == endGroupIt->second) {
            ++endGroupIt;
            ++groupSize;
        }

        int sampleCount = std::min(replicaCount, groupSize);

        std::vector<TFeasibleNode> currentResult;
        RandomSampleN(
            beginGroupIt,
            endGroupIt,
            std::back_inserter(currentResult),
            sampleCount);

        FOREACH (const auto& feasibleNode, currentResult) {
            resultNodes.push_back(feasibleNode.first);
        }

        beginGroupIt = endGroupIt;
        replicaCount -= sampleCount;
    }

    return resultNodes;
}

TSmallVector<TNode*, TypicalReplicaCount> TChunkPlacement::GetReplicationTargets(
    const TChunk* chunk,
    int count)
{
    TSmallSet<TNode*, TypicalReplicaCount> forbiddenNodes;

    auto nodeTracker = Bootstrap->GetNodeTracker();
    auto chunkManager = Bootstrap->GetChunkManager();

    FOREACH (auto replica, chunk->StoredReplicas()) {
        forbiddenNodes.insert(replica.GetPtr());
    }

    auto jobList = chunkManager->FindJobList(chunk->GetId());
    if (jobList) {
        FOREACH (const auto& job, jobList->Jobs()) {
            if (job->GetType() == EJobType::ReplicateChunk && job->GetChunkId() == chunk->GetId()) {
                FOREACH (const auto& targetAddress, job->TargetAddresses()) {
                    auto* targetNode = nodeTracker->FindNodeByAddress(targetAddress);
                    if (targetNode) {
                        forbiddenNodes.insert(targetNode);
                    }
                }
            }
        }
    }

    return GetUploadTargets(count, &forbiddenNodes, nullptr);
}

TNode* TChunkPlacement::GetReplicationSource(const TChunk* chunk)
{
    // Right now we are just picking a random location (including cached ones).
    YCHECK(!chunk->IsErasure());
    auto replicas = chunk->GetReplicas();
    YCHECK(!replicas.empty());
    int index = RandomNumber<size_t>(replicas.size());
    return replicas[index].GetPtr();
}

TSmallVector<TNode*, TypicalReplicaCount> TChunkPlacement::GetRemovalTargets(
    TChunkPtrWithIndex chunkWithIndex,
    int targetCount)
{
    // Construct a list of |(nodeId, loadFactor)| pairs.
    typedef std::pair<TNode*, double> TCandidatePair;
    TSmallVector<TCandidatePair, TypicalReplicaCount> candidates;
    auto* chunk = chunkWithIndex.GetPtr();
    candidates.reserve(chunk->StoredReplicas().size());
    FOREACH (auto replica, chunk->StoredReplicas()) {
        if (replica.GetIndex() == chunkWithIndex.GetIndex()) {
            auto* node = replica.GetPtr();
            double fillCoeff = GetFillCoeff(node);
            candidates.push_back(std::make_pair(node, fillCoeff));
        }
    }

    // Sort by |fillCoeff| in descending order.
    std::sort(
        candidates.begin(),
        candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs) {
            return lhs.second > rhs.second;
        });

    // Take first |count| nodes.
    TSmallVector<TNode*, TypicalReplicaCount> result;
    result.reserve(targetCount);
    FOREACH (const auto& pair, candidates) {
        if (static_cast<int>(result.size()) >= targetCount) {
            break;
        }
        result.push_back(pair.first);
    }

    return result;
}

bool TChunkPlacement::HasBalancingTargets(double maxFillCoeff)
{
    if (maxFillCoeff < 0)
        return false;

    if (FillCoeffToNode.empty())
        return false;

    auto* node = FillCoeffToNode.begin()->second;
    return GetFillCoeff(node) < maxFillCoeff;
}

TNode* TChunkPlacement::GetBalancingTarget(TChunkPtrWithIndex chunkWithIndex, double maxFillCoeff)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& pair, FillCoeffToNode) {
        auto node = pair.second;
        if (GetFillCoeff(node) > maxFillCoeff) {
            break;
        }
        if (IsValidBalancingTarget(node, chunkWithIndex)) {
            return node;
        }
    }
    return nullptr;
}

bool TChunkPlacement::IsValidUploadTarget(TNode* targetNode)
{
    if (targetNode->GetState() != ENodeState::Online) {
        // Do not upload anything to nodes before first heartbeat.
        return false;
    }

    if (IsFull(targetNode)) {
        // Do not upload anything to full nodes.
        return false;
    }

    const auto& config = targetNode->GetConfig();
    if (config->Decommissioned) {
        // Do not upload anything to decommissioned nodes.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(TNode* targetNode, TChunkPtrWithIndex chunkWithIndex) const
{
    if (!IsValidUploadTarget(targetNode)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (targetNode->StoredReplicas().find(chunkWithIndex) != targetNode->StoredReplicas().end())  {
        // Do not balance to a node already having the chunk.
        return false;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& job, targetNode->Jobs()) {
        if (job->GetChunkId() == chunkWithIndex.GetPtr()->GetId()) {
            // Do not balance to a node already having a job associated with this chunk.
            return false;
        }
    }

    // Seems OK :)
    return true;
}

std::vector<TChunkPtrWithIndex> TChunkPlacement::GetBalancingChunks(TNode* node, int count)
{
    // Do not balance chunks that already have a job.
    yhash_set<TChunkId> forbiddenChunkIds;
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& job, node->Jobs()) {
        forbiddenChunkIds.insert(job->GetChunkId());
    }

    // Right now we just pick some (not even random!) chunks.
    std::vector<TChunkPtrWithIndex> result;
    result.reserve(count);
    FOREACH (auto replica, node->StoredReplicas()) {
        auto* chunk = replica.GetPtr();
        if (static_cast<int>(result.size()) >= count) {
            break;
        }
        if (!chunk->GetMovable()) {
            continue;
        }
        if (forbiddenChunkIds.find(chunk->GetId()) != forbiddenChunkIds.end()) {
            continue;
        }
        result.push_back(replica);
    }

    return result;
}

double TChunkPlacement::GetLoadFactor(TNode* node) const
{
    return
        GetFillCoeff(node) +
        Config->ActiveSessionsPenalityCoeff * node->GetTotalSessionCount();
}

double TChunkPlacement::GetFillCoeff(TNode* node) const
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
