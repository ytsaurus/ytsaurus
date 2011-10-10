#include "chunk_placement.h"

#include "../misc/foreach.h"

#include <util/random/random.h>

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(TChunkManager::TPtr chunkManager)
    : ChunkManager(chunkManager)
{
    YASSERT(~chunkManager != NULL);
}

void TChunkPlacement::AddHolder(const THolder& holder)
{
    double loadFactor = GetLoadFactor(holder);
    auto it = LoadFactorMap.insert(MakePair(loadFactor, holder.Id));
    YVERIFY(IteratorMap.insert(MakePair(holder.Id, it)).Second());
    YVERIFY(HintedSessionsMap.insert(MakePair(holder.Id, 0)).Second());
}

void TChunkPlacement::RemoveHolder(const THolder& holder)
{
    auto iteratorIt = IteratorMap.find(holder.Id);
    YASSERT(iteratorIt != IteratorMap.end());
    auto preferenceIt = iteratorIt->Second();
    LoadFactorMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
    YVERIFY(HintedSessionsMap.erase(holder.Id) == 1);
}

void TChunkPlacement::UpdateHolder(const THolder& holder)
{
    RemoveHolder(holder);
    AddHolder(holder);
}

void TChunkPlacement::AddHolderSessionHint(const THolder& holder)
{
    ++HintedSessionsMap[holder.Id];
}

void PickRandom(
    yvector<const THolder*>::iterator holderBegin,
    yvector<const THolder*>::iterator holderEnd,
    int replicaCount,
    yvector<THolderId>& result)
{
    unsigned int holdersRemaining = NStl::distance(holderBegin, holderEnd);
    unsigned int replicasNeeded = Min(static_cast<int>(holdersRemaining), replicaCount - result.ysize());
    for (auto holderCurrent = holderBegin; holderCurrent != holderEnd; ++holderCurrent) {
        const auto* holder = *holderCurrent;
        if (RandomNumber(holdersRemaining) < replicasNeeded) {
            result.push_back(holder->Id);
            --replicasNeeded;
        }
        --holdersRemaining;
    }
}

yvector<THolderId> TChunkPlacement::GetUploadTargets(int count)
{
    // TODO: check replication fan-in in case this is a replication job
    yvector<THolderId> result;
    result.reserve(count);

    yvector<const THolder*> holders;
    holders.reserve(LoadFactorMap.size());

    FOREACH(const auto& pair, LoadFactorMap) {
        const auto& holder = ChunkManager->GetHolder(pair.second);
        if (IsValidUploadTarget(holder)) {
            holders.push_back(&holder);
        }
    }

    Sort(holders.begin(), holders.end(),
        [&] (const THolder* lhs, const THolder* rhs) {
            return GetSessionCount(*lhs) < GetSessionCount(*rhs);
        });

    auto it = holders.begin();
    while (it != holders.end()) {
        auto jt = it;
        while (jt != holders.end() && GetSessionCount(*(*it)) == GetSessionCount(*(*jt))) {
            ++jt;
        }

        PickRandom(it, jt, count, result);

        it = jt;
    }

    return result;
}

yvector<THolderId> TChunkPlacement::GetReplicationTargets(const TChunk& chunk, int count)
{
    yhash_set<Stroka> forbiddenAddresses;

    FOREACH(auto holderId, chunk.Locations) {
        const auto& holder = ChunkManager->GetHolder(holderId);
        forbiddenAddresses.insert(holder.Address);
    }

    const auto* jobList = ChunkManager->FindJobList(chunk.Id);
    if (jobList != NULL) {
        FOREACH(const auto& jobId, jobList->Jobs) {
            const auto& job = ChunkManager->GetJob(jobId);
            if (job.Type == EJobType::Replicate && job.ChunkId == chunk.Id) {
                forbiddenAddresses.insert(job.TargetAddresses.begin(), job.TargetAddresses.end());
            }
        }
    }

    // TODO: fixme, pass forbiddenAddresses to GetUploadTargets
    auto candidates = GetUploadTargets(count + forbiddenAddresses.size());

    yvector<THolderId> result;
    FOREACH(auto holderId, candidates) {
        if (result.ysize() >= count)
            break;

        const auto& holder = ChunkManager->GetHolder(holderId);
        if (forbiddenAddresses.find(holder.Address) == forbiddenAddresses.end()) {
            result.push_back(holder.Id);
        }
    }

    return result;
}

THolderId TChunkPlacement::GetReplicationSource(const TChunk& chunk)
{
    // TODO: do something smart
    YASSERT(!chunk.Locations.empty());
    return chunk.Locations[0];
}

yvector<THolderId> TChunkPlacement::GetRemovalTargets(const TChunk& chunk, int count)
{
    // Construct a list of (holderId, loadFactor) pairs.
    typedef TPair<THolderId, double> TCandidatePair;
    yvector<TCandidatePair> candidates;
    candidates.reserve(chunk.Locations.ysize());
    FOREACH(auto holderId, chunk.Locations) {
        const auto& holder = ChunkManager->GetHolder(holderId);
        double loadFactor = GetLoadFactor(holder);
        candidates.push_back(MakePair(holderId, loadFactor));
    }

    // Sort by loadFactor in descending order.
    Sort(candidates.begin(), candidates.end(),
        [] (const TCandidatePair& lhs, const TCandidatePair& rhs)
        {
            return lhs.Second() > rhs.Second();
        });

    // Take first count holders.
    yvector<THolderId> result;
    result.reserve(count);
    FOREACH(auto pair, candidates) {
        if (result.ysize() >= count)
            break;
        result.push_back(pair.First());
    }
    return result;
}

THolderId TChunkPlacement::GetBalancingTarget(const TChunk& chunk, double maxFillCoeff)
{
    FOREACH (const auto& pair, LoadFactorMap) {
        const auto& holder = ChunkManager->GetHolder(pair.second);
        if (GetFillCoeff(holder) > maxFillCoeff) {
            break;
        }
        if (IsValidBalancingTarget(holder, chunk)) {
            return holder.Id;
        }
    }
    return InvalidHolderId;
}

bool TChunkPlacement::IsValidUploadTarget(const THolder& targetHolder) const
{
    if (targetHolder.State != EHolderState::Active) {
        // Do not upload anything to inactive holders.
        return false;
    }

    if (IsFull(targetHolder)) {
        // Do not upload anything to full holders.
        return false;
    }
            
    // Seems OK :)
    return true;
}

bool TChunkPlacement::IsValidBalancingTarget(const THolder& targetHolder, const TChunk& chunk) const
{
    if (!IsValidUploadTarget(targetHolder)) {
        // Balancing implies upload, after all.
        return false;
    }

    if (targetHolder.Chunks.find(chunk.Id) != targetHolder.Chunks.end())  {
        // Do not balance to a holder already having the chunk.
        return false;
    }

    FOREACH (const auto& jobId, targetHolder.Jobs) {
        const auto& job = ChunkManager->GetJob(jobId);
        if (job.ChunkId == chunk.Id) {
            // Do not balance to a holder already having a job associated with this chunk.
            return false;
        }
    }

    auto* sink = ChunkManager->FindReplicationSink(targetHolder.Address);
    if (sink != NULL) {
        if (static_cast<int>(sink->JobIds.size()) >= MaxReplicationFanIn) {
            // Do not balance to a holder with too many incoming replication jobs.
            return false;
        }

        FOREACH (const auto& jobId, sink->JobIds) {
            const auto& job = ChunkManager->GetJob(jobId);
            if (job.ChunkId == chunk.Id) {
                // Do not balance to a holder that is a replication target for the very same chunk.
                return false;
            }
        }
    }

    // Seems OK :)
    return true;
}

yvector<TChunkId> TChunkPlacement::GetBalancingChunks(const THolder& holder, int count)
{
    // Do not balance chunks that already have a job assigned.
    yhash_set<TChunkId> forbiddenChunkIds;
    FOREACH (const auto& jobId, holder.Jobs) {
        const auto& job = ChunkManager->GetJob(jobId);
        forbiddenChunkIds.insert(job.ChunkId);
    }

    // TODO: do something smart
    yvector<TChunkId> result;
    FOREACH (const auto& chunkId, holder.Chunks) {
        if (result.ysize() >= count)
            break;
        result.push_back(chunkId);
    }

    return result;
}

int TChunkPlacement::GetSessionCount(const THolder& holder) const
{
    auto hintIt = HintedSessionsMap.find(holder.Id);
    return hintIt == HintedSessionsMap.end() ? 0 : hintIt->Second();
}

double TChunkPlacement::GetLoadFactor(const THolder& holder) const
{
    const auto& statistics = holder.Statistics;
    return
        GetFillCoeff(holder) +
        ActiveSessionsPenalityCoeff * (statistics.SessionCount + GetSessionCount(holder));
}

double TChunkPlacement::GetFillCoeff(const THolder& holder) const
{
    const auto& statistics = holder.Statistics;
    return
        (1.0 + statistics.UsedSpace) /
        (1.0 + statistics.UsedSpace + statistics.AvailableSpace);
}

bool TChunkPlacement::IsFull(const THolder& holder) const
{
    if (GetFillCoeff(holder) > MaxHolderFillCoeff)
        return true;

    const auto& statistics = holder.Statistics;
    if (statistics.AvailableSpace - statistics.UsedSpace < MinHolderFreeSpace)
        return true;

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
