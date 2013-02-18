#include "stdafx.h"
#include "chunk_placement.h"
#include "node.h"
#include "chunk.h"
#include "job.h"
#include "job_list.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>

#include <server/chunk_server/chunk_manager.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkServer");

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

void TChunkPlacement::OnNodeRegistered(TDataNode* node)
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

void TChunkPlacement::OnNodeUnregistered(TDataNode* node)
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

void TChunkPlacement::OnNodeUpdated(TDataNode* node)
{
    OnNodeUnregistered(node);
    OnNodeRegistered(node);
    node->SetHintedSessionCount(0);
}

void TChunkPlacement::OnSessionHinted(TDataNode* node)
{
    node->SetHintedSessionCount(node->GetHintedSessionCount() + 1);
}

TSmallVector<TDataNode*, TypicalReplicationFactor> TChunkPlacement::GetUploadTargets(
    int count,
    const TSmallSet<Stroka, TypicalReplicationFactor>* forbiddenAddresses,
    Stroka* preferredHostName)
{
    // TODO: check replication fan-in for replication jobs

    TSmallVector<TDataNode*, TypicalReplicationFactor> resultNodes;
    resultNodes.reserve(count);

    typedef std::pair<TDataNode*, int> TFeasibleNode;
    std::vector<TFeasibleNode> feasibleNodes;
    feasibleNodes.reserve(LoadFactorToNode.size());

    TDataNode* preferredNode = NULL;

    auto chunkManager = Bootstrap->GetChunkManager();

    // Look for preferred node first.
    if (preferredHostName) {
        preferredNode = chunkManager->FindNodeByHostName(*preferredHostName);
        if (preferredNode && IsValidUploadTarget(preferredNode)) {
            resultNodes.push_back(preferredNode);
            --count;
        }
    }

    // Put other feasible nodes to feasibleNodes.
    FOREACH (auto& pair, LoadFactorToNode) {
        auto* node = pair.second;
        if (node != preferredNode &&
            IsValidUploadTarget(node) &&
            !(forbiddenAddresses && forbiddenAddresses->count(node->GetAddress())))
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
    while (beginGroupIt != feasibleNodes.end() && count > 0) {
        auto endGroupIt = beginGroupIt;
        int groupSize = 0;
        while (endGroupIt != feasibleNodes.end() && beginGroupIt->second == endGroupIt->second) {
            ++endGroupIt;
            ++groupSize;
        }

        int sampleCount = std::min(count, groupSize);

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
        count -= sampleCount;
    }

    return resultNodes;
}

TSmallVector<TDataNode*, TypicalReplicationFactor> TChunkPlacement::GetReplicationTargets(
    const TChunk* chunk,
    int count)
{
    TSmallSet<Stroka, TypicalReplicationFactor> forbiddenAddresses;

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto replica, chunk->StoredReplicas()) {
        forbiddenAddresses.insert(replica.GetNode()->GetAddress());
    }

    const auto* jobList = chunkManager->FindJobList(chunk->GetId());
    if (jobList) {
        FOREACH (auto job, jobList->Jobs()) {
            if (job->GetType() == EJobType::Replicate && job->GetChunkId() == chunk->GetId()) {
                forbiddenAddresses.insert(job->TargetAddresses().begin(), job->TargetAddresses().end());
            }
        }
    }

    return GetUploadTargets(count, &forbiddenAddresses, nullptr);
}

TDataNode* TChunkPlacement::GetReplicationSource(const TChunk* chunk)
{
    // Right now we are just picking a random location (including cached ones).
    auto replicas = chunk->GetReplicas();
    YCHECK(!replicas.empty());
    int index = RandomNumber<size_t>(replicas.size());
    return replicas[index].GetNode();
}

TSmallVector<TDataNode*, TypicalReplicationFactor> TChunkPlacement::GetRemovalTargets(
    const TChunk* chunk,
    int count)
{
    // Construct a list of (nodeId, loadFactor) pairs.
    typedef std::pair<TDataNode*, double> TCandidatePair;
    TSmallVector<TCandidatePair, TypicalReplicationFactor> candidates;
    candidates.reserve(chunk->StoredReplicas().size());
    FOREACH (auto replica, chunk->StoredReplicas()) {
        auto* node = replica.GetNode();
        double fillCoeff = GetFillCoeff(node);
        candidates.push_back(std::make_pair(node, fillCoeff));
    }

    // Sort by fillCoeff in descending order.
    std::sort(
        candidates.begin(),
        candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs) {
            return lhs.second > rhs.second;
        });

    // Take first count nodes.
    TSmallVector<TDataNode*, TypicalReplicationFactor> result;
    result.reserve(count);
    FOREACH (const auto& pair, candidates) {
        if (static_cast<int>(result.size()) >= count) {
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

TDataNode* TChunkPlacement::GetBalancingTarget(TChunk* chunk, double maxFillCoeff)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& pair, FillCoeffToNode) {
        auto node = pair.second;
        if (GetFillCoeff(node) > maxFillCoeff) {
            break;
        }
        if (IsValidBalancingTarget(node, chunk)) {
            return node;
        }
    }
    return NULL;
}

bool TChunkPlacement::IsValidUploadTarget(TDataNode* targetNode)
{
    if (targetNode->GetState() != ENodeState::Online) {
        // Do not upload anything to nodes before first heartbeat.
        return false;
    }

    if (IsFull(targetNode)) {
        // Do not upload anything to full nodes.
        return false;
    }

    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(TDataNode* targetNode, TChunk* chunk) const
{
    if (!IsValidUploadTarget(targetNode)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (targetNode->StoredChunks().find(chunk) != targetNode->StoredChunks().end())  {
        // Do not balance to a node already having the chunk.
        return false;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto* job, targetNode->Jobs()) {
        if (job->GetChunkId() == chunk->GetId()) {
            // Do not balance to a node already having a job associated with this chunk.
            return false;
        }
    }

    auto* sink = chunkManager->FindReplicationSink(targetNode->GetAddress());
    if (sink) {
        if (static_cast<int>(sink->Jobs().size()) >= Config->ChunkReplicator->MaxReplicationFanIn) {
            // Do not balance to a node with too many incoming replication jobs.
            return false;
        }

        FOREACH (const auto* job, sink->Jobs()) {
            if (job->GetChunkId() == chunk->GetId()) {
                // Do not balance to a node that is a replication target for the very same chunk.
                return false;
            }
        }
    }

    // Seems OK :)
    return true;
}

std::vector<TChunk*> TChunkPlacement::GetBalancingChunks(TDataNode* node, int count)
{
    // Do not balance chunks that already have a job.
    yhash_set<TChunkId> forbiddenChunkIds;
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto* job, node->Jobs()) {
        forbiddenChunkIds.insert(job->GetChunkId());
    }

    // Right now we just pick some (not even random!) chunks.
    std::vector<TChunk*> result;
    result.reserve(count);
    FOREACH (auto* chunk, node->StoredChunks()) {
        if (static_cast<int>(result.size()) >= count) {
            break;
        }
        if (!chunk->GetMovable()) {
            continue;
        }
        if (forbiddenChunkIds.find(chunk->GetId()) != forbiddenChunkIds.end()) {
            continue;
        }
        result.push_back(chunk);
    }

    return result;
}

double TChunkPlacement::GetLoadFactor(TDataNode* node) const
{
    return
        GetFillCoeff(node) +
        Config->ActiveSessionsPenalityCoeff * node->GetTotalSessionCount();
}

double TChunkPlacement::GetFillCoeff(TDataNode* node) const
{
    const auto& statistics = node->Statistics();
    return statistics.total_used_space() /
        (1.0 + statistics.total_used_space() + statistics.total_available_space());
}

bool TChunkPlacement::IsFull(TDataNode* node)
{
    return node->Statistics().full();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
