#include "stdafx.h"
#include "chunk_placement.h"
#include "holder.h"
#include "chunk.h"
#include "job.h"
#include "job_list.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>
#include <ytlib/chunk_server/chunk_manager.h>

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
    YASSERT(config);
    YASSERT(bootstrap);
}

void TChunkPlacement::OnHolderRegistered(THolder* holder)
{
    double loadFactor = GetLoadFactor(holder);
    auto it = LoadFactorMap.insert(MakePair(loadFactor, holder));
    YCHECK(IteratorMap.insert(MakePair(holder, it)).second);
    YCHECK(HintedSessionsMap.insert(MakePair(holder, 0)).second);
}

void TChunkPlacement::OnHolderUnregistered(THolder* holder)
{
    auto iteratorIt = IteratorMap.find(holder);
    YASSERT(iteratorIt != IteratorMap.end());
    auto preferenceIt = iteratorIt->second;
    LoadFactorMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
    YCHECK(HintedSessionsMap.erase(holder) == 1);
}

void TChunkPlacement::OnHolderUpdated(THolder* holder)
{
    OnHolderUnregistered(holder);
    OnHolderRegistered(holder);
}

void TChunkPlacement::OnSessionHinted(THolder* holder)
{
    ++HintedSessionsMap[holder];
}

std::vector<THolder*> TChunkPlacement::GetUploadTargets(
    int count,
    const std::unordered_set<Stroka>* forbiddenAddresses,
    Stroka* preferredHostName)
{
    // TODO: check replication fan-in in case this is a replication job

    std::vector<THolder*> resultHolders;
    resultHolders.reserve(count);

    std::vector<THolder*> feasibleHolders;
    feasibleHolders.reserve(LoadFactorMap.size());

    THolder* preferredHolder = NULL;

    auto chunkManager = Bootstrap->GetChunkManager();

    // Look for preferred holder first.
    if (preferredHostName) {
        preferredHolder = chunkManager->FindHolderByHostName(*preferredHostName);
        if (preferredHolder && IsValidUploadTarget(preferredHolder)) {
            resultHolders.push_back(preferredHolder);
            --count;
        }
    }

    // Put other feasible holders in |feasibleHolders|.
    FOREACH (auto& pair, LoadFactorMap) {
        auto* holder = pair.second;
        if (holder != preferredHolder &&
            IsValidUploadTarget(holder) &&
            (!forbiddenAddresses || forbiddenAddresses->find(holder->GetAddress()) == forbiddenAddresses->end()))
        {
            feasibleHolders.push_back(holder);
        }
    }

    // Take a sample from |feasibleHolders|.
    std::sort(
        feasibleHolders.begin(),
        feasibleHolders.end(),
        [=] (THolder* lhs, THolder* rhs) {
            return GetSessionCount(lhs) < GetSessionCount(rhs);
        });

    auto beginGroupIt = feasibleHolders.begin();
    while (beginGroupIt != feasibleHolders.end() && count > 0) {
        auto endGroupIt = beginGroupIt;
        int groupSize = 0;
        while (endGroupIt != feasibleHolders.end() && GetSessionCount(*beginGroupIt) == GetSessionCount(*endGroupIt)) {
            ++endGroupIt;
            ++groupSize;
        }

        int sampleCount = Min(count, groupSize);
        RandomSampleN(
            beginGroupIt,
            endGroupIt,
            std::back_inserter(resultHolders),
            sampleCount);

        beginGroupIt = endGroupIt;
        count -= sampleCount;
    }

    return resultHolders;
}

std::vector<THolder*> TChunkPlacement::GetReplicationTargets(const TChunk* chunk, int count)
{
    std::unordered_set<Stroka> forbiddenAddresses;

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (auto holderId, chunk->StoredLocations()) {
        const auto* holder = chunkManager->GetHolder(holderId);
        forbiddenAddresses.insert(holder->GetAddress());
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

THolder* TChunkPlacement::GetReplicationSource(const TChunk* chunk)
{
    // Right now we are just picking a random location (including cached ones).
    const auto& locations = chunk->GetLocations();
    YASSERT(!locations.empty());
    int index = RandomNumber<size_t>(locations.size());
    return Bootstrap->GetChunkManager()->GetHolder(locations[index]);
}

std::vector<THolder*> TChunkPlacement::GetRemovalTargets(const TChunk* chunk, int count)
{
    // Construct a list of (holderId, loadFactor) pairs.
    typedef TPair<THolder*, double> TCandidatePair;
    std::vector<TCandidatePair> candidates;
    auto chunkManager = Bootstrap->GetChunkManager();
    candidates.reserve(chunk->StoredLocations().size());
    FOREACH (auto holderId, chunk->StoredLocations()) {
        auto* holder = chunkManager->GetHolder(holderId);
        double loadFactor = GetLoadFactor(holder);
        candidates.push_back(MakePair(holder, loadFactor));
    }

    // Sort by loadFactor in descending order.
    std::sort(
        candidates.begin(),
        candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs) {
            return lhs.second > rhs.second;
        });

    // Take first count holders.
    std::vector<THolder*> result;
    result.reserve(count);
    FOREACH (const auto& pair, candidates) {
        if (static_cast<int>(result.size()) >= count) {
            break;
        }
        result.push_back(pair.first);
    }
    return result;
}

THolder* TChunkPlacement::GetBalancingTarget(TChunk* chunk, double maxFillCoeff)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto& pair, LoadFactorMap) {
        auto holder = pair.second;
        if (GetFillCoeff(holder) > maxFillCoeff) {
            break;
        }
        if (IsValidBalancingTarget(holder, chunk)) {
            return holder;
        }
    }
    return NULL;
}

bool TChunkPlacement::IsValidUploadTarget(THolder* targetHolder) const
{
    if (targetHolder->GetState() != EHolderState::Online) {
        // Do not upload anything to holders before first heartbeat.
        return false;
    }

    if (IsFull(targetHolder)) {
        // Do not upload anything to full holders.
        return false;
    }
            
    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(THolder* targetHolder, TChunk* chunk) const
{
    if (!IsValidUploadTarget(targetHolder)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (targetHolder->StoredChunks().find(chunk) != targetHolder->StoredChunks().end())  {
        // Do not balance to a holder already having the chunk.
        return false;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto* job, targetHolder->Jobs()) {
        if (job->GetChunkId() == chunk->GetId()) {
            // Do not balance to a holder already having a job associated with this chunk.
            return false;
        }
    }

    auto* sink = chunkManager->FindReplicationSink(targetHolder->GetAddress());
    if (sink) {
        if (static_cast<int>(sink->Jobs().size()) >= Config->ChunkReplicator->MaxReplicationFanIn) {
            // Do not balance to a holder with too many incoming replication jobs.
            return false;
        }

        FOREACH (auto* job, sink->Jobs()) {
            if (job->GetChunkId() == chunk->GetId()) {
                // Do not balance to a holder that is a replication target for the very same chunk.
                return false;
            }
        }
    }

    // Seems OK :)
    return true;
}

std::vector<TChunk*> TChunkPlacement::GetBalancingChunks(THolder* holder, int count)
{
    // Do not balance chunks that already have a job.
    std::unordered_set<TChunkId> forbiddenChunkIds;
    auto chunkManager = Bootstrap->GetChunkManager();
    FOREACH (const auto* job, holder->Jobs()) {
        forbiddenChunkIds.insert(job->GetChunkId());
    }

    // Right now we just pick some (not even random!) chunks.
    std::vector<TChunk*> result;
    result.reserve(count);
    FOREACH (auto* chunk, holder->StoredChunks()) {
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

int TChunkPlacement::GetSessionCount(THolder* holder) const
{
    auto hintIt = HintedSessionsMap.find(holder);
    return hintIt == HintedSessionsMap.end() ? 0 : hintIt->second;
}

double TChunkPlacement::GetLoadFactor(THolder* holder) const
{
    const auto& statistics = holder->Statistics();
    return
        GetFillCoeff(holder) +
        Config->ActiveSessionsPenalityCoeff * (statistics.session_count() + GetSessionCount(holder));
}

double TChunkPlacement::GetFillCoeff(THolder* holder) const
{
    const auto& statistics = holder->Statistics();
    return
        (1.0 + statistics.used_space()) /
        (1.0 + statistics.used_space() + statistics.available_space());
}

bool TChunkPlacement::IsFull(THolder* holder) const
{
    return holder->Statistics().full();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
