#include "stdafx.h"
#include "chunk_placement.h"
#include "node.h"
#include "chunk.h"
#include "job.h"
#include "job_list.h"

#include <ytlib/misc/foreach.h>
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
    double loadFactor = GetLoadFactor(node);
    auto it = LoadFactorMap.insert(MakePair(loadFactor, node));
    YCHECK(IteratorMap.insert(MakePair(node, it)).second);
    YCHECK(HintedSessionsMap.insert(MakePair(node, 0)).second);
}

void TChunkPlacement::OnNodeUnregistered(TDataNode* node)
{
    auto iteratorIt = IteratorMap.find(node);
    YASSERT(iteratorIt != IteratorMap.end());
    auto preferenceIt = iteratorIt->second;
    LoadFactorMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
    YCHECK(HintedSessionsMap.erase(node) == 1);
}

void TChunkPlacement::OnNodeUpdated(TDataNode* node)
{
    OnNodeUnregistered(node);
    OnNodeRegistered(node);
}

void TChunkPlacement::OnSessionHinted(TDataNode* node)
{
    ++HintedSessionsMap[node];
}

std::vector<TDataNode*> TChunkPlacement::GetUploadTargets(
    int count,
    const yhash_set<Stroka>* forbiddenAddresses,
    Stroka* preferredHostName)
{
    // TODO: check replication fan-in for replication jobs

    std::vector<TDataNode*> resultNodes;
    resultNodes.reserve(count);

    typedef std::pair<TDataNode*, int> TFeasibleNode;
    std::vector<TFeasibleNode> feasibleNodes;
    feasibleNodes.reserve(LoadFactorMap.size());

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
    FOREACH (auto& pair, LoadFactorMap) {
        auto* node = pair.second;
        if (node != preferredNode &&
            IsValidUploadTarget(node) &&
            (!forbiddenAddresses || forbiddenAddresses->find(node->GetAddress()) == forbiddenAddresses->end()))
        {
            feasibleNodes.push_back(std::make_pair(node, GetSessionCount(node)));
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

        int sampleCount = Min(count, groupSize);

        std::vector<TFeasibleNode> currentResult;
        RandomSampleN(
            beginGroupIt,
            endGroupIt,
            std::back_inserter(currentResult),
            sampleCount);

        FOREACH(auto &feasibleNode, currentResult) {
            resultNodes.push_back(feasibleNode.first);
        }

        beginGroupIt = endGroupIt;
        count -= sampleCount;
    }

    return resultNodes;
}

std::vector<TDataNode*> TChunkPlacement::GetReplicationTargets(const TChunk* chunk, int count)
{
    yhash_set<Stroka> forbiddenAddresses;

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto nodeId, chunk->StoredLocations()) {
        const auto* node = chunkManager->GetNode(nodeId);
        forbiddenAddresses.insert(node->GetAddress());
    }

    const auto* jobList = chunkManager->FindJobList(chunk->GetId());
    if (jobList) {
        FOREACH (auto job, jobList->Jobs()) {
            if (job->GetType() == EJobType::Replicate && job->GetChunkId() == chunk->GetId()) {
                forbiddenAddresses.insert(job->TargetAddresses().begin(), job->TargetAddresses().end());
            }
        }
    }

    return GetUploadTargets(count, &forbiddenAddresses, NULL);
}

TDataNode* TChunkPlacement::GetReplicationSource(const TChunk* chunk)
{
    // Right now we are just picking a random location (including cached ones).
    const auto& locations = chunk->GetLocations();
    YCHECK(!locations.empty());
    int index = RandomNumber<size_t>(locations.size());
    return Bootstrap->GetChunkManager()->GetNode(locations[index]);
}

std::vector<TDataNode*> TChunkPlacement::GetRemovalTargets(const TChunk* chunk, int count)
{
    // Construct a list of (nodeId, loadFactor) pairs.
    typedef TPair<TDataNode*, double> TCandidatePair;
    std::vector<TCandidatePair> candidates;
    auto chunkManager = Bootstrap->GetChunkManager();
    candidates.reserve(chunk->StoredLocations().size());
    FOREACH (auto nodeId, chunk->StoredLocations()) {
        auto* node = chunkManager->GetNode(nodeId);
        double loadFactor = GetLoadFactor(node);
        candidates.push_back(MakePair(node, loadFactor));
    }

    // Sort by loadFactor in descending order.
    std::sort(
        candidates.begin(),
        candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs) {
            return lhs.second > rhs.second;
        });

    // Take first count nodes.
    std::vector<TDataNode*> result;
    result.reserve(count);
    FOREACH (const auto& pair, candidates) {
        if (static_cast<int>(result.size()) >= count) {
            break;
        }
        result.push_back(pair.first);
    }
    return result;
}

TDataNode* TChunkPlacement::GetBalancingTarget(TChunk* chunk, double maxFillCoeff)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& pair, LoadFactorMap) {
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

bool TChunkPlacement::IsValidUploadTarget(TDataNode* targetNode) const
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

        FOREACH (auto* job, sink->Jobs()) {
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

int TChunkPlacement::GetSessionCount(TDataNode* node) const
{
    auto hintIt = HintedSessionsMap.find(node);
    return hintIt == HintedSessionsMap.end() ? 0 : hintIt->second;
}

double TChunkPlacement::GetLoadFactor(TDataNode* node) const
{
    const auto& statistics = node->Statistics();
    return
        GetFillCoeff(node) +
        Config->ActiveSessionsPenalityCoeff * (statistics.session_count() + GetSessionCount(node));
}

double TChunkPlacement::GetFillCoeff(TDataNode* node) const
{
    const auto& statistics = node->Statistics();
    return
        (1.0 + statistics.used_space()) /
        (1.0 + statistics.used_space() + statistics.available_space());
}

bool TChunkPlacement::IsFull(TDataNode* node) const
{
    return node->Statistics().full();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
