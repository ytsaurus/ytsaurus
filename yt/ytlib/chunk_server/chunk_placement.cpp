#include "stdafx.h"
#include "chunk_placement.h"

#include "../misc/foreach.h"

#include <util/random/random.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkPlacement::TChunkPlacement(TChunkManager::TPtr chunkManager)
    : ChunkManager(chunkManager)
{
    YASSERT(~chunkManager != NULL);
}

void TChunkPlacement::AddHolder(const THolder& holder)
{
    double loadFactor = GetLoadFactor(holder);
    auto it = LoadFactorMap.insert(MakePair(loadFactor, holder.GetId()));
    YVERIFY(IteratorMap.insert(MakePair(holder.GetId(), it)).Second());
    YVERIFY(HintedSessionsMap.insert(MakePair(holder.GetId(), 0)).Second());
}

void TChunkPlacement::RemoveHolder(const THolder& holder)
{
    auto iteratorIt = IteratorMap.find(holder.GetId());
    YASSERT(iteratorIt != IteratorMap.end());
    auto preferenceIt = iteratorIt->Second();
    LoadFactorMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
    YVERIFY(HintedSessionsMap.erase(holder.GetId()) == 1);
}

void TChunkPlacement::UpdateHolder(const THolder& holder)
{
    RemoveHolder(holder);
    AddHolder(holder);
}

void TChunkPlacement::AddHolderSessionHint(const THolder& holder)
{
    ++HintedSessionsMap[holder.GetId()];
}

yvector<THolderId> TChunkPlacement::GetUploadTargets(int count)
{
    return GetUploadTargets(count, yhash_set<Stroka>());
}

yvector<THolderId> TChunkPlacement::GetUploadTargets(int count, const yhash_set<Stroka>& forbiddenAddresses)
{
    // TODO: check replication fan-in in case this is a replication job
    yvector<const THolder*> holders;
    holders.reserve(LoadFactorMap.size());

    FOREACH(const auto& pair, LoadFactorMap) {
        const auto& holder = ChunkManager->GetHolder(pair.second);
        if (IsValidUploadTarget(holder) &&
            forbiddenAddresses.find(holder.GetAddress()) == forbiddenAddresses.end()) {
            holders.push_back(&holder);
        }
    }

    Sort(holders.begin(), holders.end(),
        [&] (const THolder* lhs, const THolder* rhs) {
            return GetSessionCount(*lhs) < GetSessionCount(*rhs);
        });

    yvector<const THolder*> holdersSample;
    holdersSample.reserve(count);

    auto beginGroupIt = holders.begin();
    while (beginGroupIt != holders.end() && count > 0) {
        auto endGroupIt = beginGroupIt;
        int groupSize = 0;
        while (endGroupIt != holders.end() && GetSessionCount(*(*beginGroupIt)) == GetSessionCount(*(*endGroupIt))) {
            ++endGroupIt;
            ++groupSize;
        }

        int sampleCount = Min(count, groupSize);
        NStl::random_sample_n(
            beginGroupIt,
            endGroupIt,
            NStl::back_inserter(holdersSample),
            sampleCount);

        beginGroupIt = endGroupIt;
        count -= sampleCount;
    }

    yvector<THolderId> holderIdsSample(holdersSample.ysize());
    for (int i = 0; i < holdersSample.ysize(); ++i) {
        holderIdsSample[i] = holdersSample[i]->GetId();
    }

    return holderIdsSample;
}

yvector<THolderId> TChunkPlacement::GetReplicationTargets(const TChunk& chunk, int count)
{
    yhash_set<Stroka> forbiddenAddresses;

    FOREACH(auto holderId, chunk.Locations()) {
        const auto& holder = ChunkManager->GetHolder(holderId);
        forbiddenAddresses.insert(holder.GetAddress());
    }

    const auto* jobList = ChunkManager->FindJobList(chunk.GetId());
    if (jobList != NULL) {
        FOREACH(const auto& jobId, jobList->Jobs) {
            const auto& job = ChunkManager->GetJob(jobId);
            if (job.Type == EJobType::Replicate && job.ChunkId == chunk.GetId()) {
                forbiddenAddresses.insert(job.TargetAddresses.begin(), job.TargetAddresses.end());
            }
        }
    }

    return GetUploadTargets(count, forbiddenAddresses);
}

THolderId TChunkPlacement::GetReplicationSource(const TChunk& chunk)
{
    // TODO: do something smart
    YASSERT(!chunk.Locations().empty());
    return chunk.Locations()[0];
}

yvector<THolderId> TChunkPlacement::GetRemovalTargets(const TChunk& chunk, int count)
{
    // Construct a list of (holderId, loadFactor) pairs.
    typedef TPair<THolderId, double> TCandidatePair;
    yvector<TCandidatePair> candidates;
    candidates.reserve(chunk.Locations().ysize());
    FOREACH(auto holderId, chunk.Locations()) {
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
            return holder.GetId();
        }
    }
    return InvalidHolderId;
}

bool TChunkPlacement::IsValidUploadTarget(const THolder& targetHolder) const
{
    if (targetHolder.GetState() != EHolderState::Active) {
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

    if (targetHolder.Chunks().find(chunk.GetId()) != targetHolder.Chunks().end())  {
        // Do not balance to a holder already having the chunk.
        return false;
    }

    FOREACH (const auto& jobId, targetHolder.Jobs()) {
        const auto& job = ChunkManager->GetJob(jobId);
        if (job.ChunkId == chunk.GetId()) {
            // Do not balance to a holder already having a job associated with this chunk.
            return false;
        }
    }

    auto* sink = ChunkManager->FindReplicationSink(targetHolder.GetAddress());
    if (sink != NULL) {
        if (static_cast<int>(sink->JobIds.size()) >= MaxReplicationFanIn) {
            // Do not balance to a holder with too many incoming replication jobs.
            return false;
        }

        FOREACH (const auto& jobId, sink->JobIds) {
            const auto& job = ChunkManager->GetJob(jobId);
            if (job.ChunkId == chunk.GetId()) {
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
    FOREACH (const auto& jobId, holder.Jobs()) {
        const auto& job = ChunkManager->GetJob(jobId);
        forbiddenChunkIds.insert(job.ChunkId);
    }

    // TODO: do something smart
    yvector<TChunkId> result;
    FOREACH (const auto& chunkId, holder.Chunks()) {
        if (result.ysize() >= count)
            break;
        result.push_back(chunkId);
    }

    return result;
}

int TChunkPlacement::GetSessionCount(const THolder& holder) const
{
    auto hintIt = HintedSessionsMap.find(holder.GetId());
    return hintIt == HintedSessionsMap.end() ? 0 : hintIt->Second();
}

double TChunkPlacement::GetLoadFactor(const THolder& holder) const
{
    const auto& statistics = holder.Statistics();
    return
        GetFillCoeff(holder) +
        ActiveSessionsPenalityCoeff * (statistics.SessionCount + GetSessionCount(holder));
}

double TChunkPlacement::GetFillCoeff(const THolder& holder) const
{
    const auto& statistics = holder.Statistics();
    return
        (1.0 + statistics.UsedSpace) /
        (1.0 + statistics.UsedSpace + statistics.AvailableSpace);
}

bool TChunkPlacement::IsFull(const THolder& holder) const
{
    if (GetFillCoeff(holder) > MaxHolderFillCoeff)
        return true;

    const auto& statistics = holder.Statistics();
    if (statistics.AvailableSpace - statistics.UsedSpace < MinHolderFreeSpace)
        return true;

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
