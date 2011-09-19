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
{ }

void TChunkPlacement::AddHolder(const THolder& holder)
{
    double loadFactor = GetLoadFactor(holder);
    auto it = PreferenceMap.insert(MakePair(loadFactor, holder.Id));
    YVERIFY(IteratorMap.insert(MakePair(holder.Id, it)).Second());
}

void TChunkPlacement::RemoveHolder(const THolder& holder)
{
    auto iteratorIt = IteratorMap.find(holder.Id);
    YASSERT(iteratorIt != IteratorMap.end());
    auto preferenceIt = iteratorIt->Second();
    PreferenceMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
}

void TChunkPlacement::UpdateHolder(const THolder& holder)
{
    RemoveHolder(holder);
    AddHolder(holder);
}

yvector<THolderId> TChunkPlacement::GetUploadTargets(int replicaCount)
{
    // TODO: do not list holders that are nearly full
    yvector<THolderId> result;
    result.reserve(replicaCount);
    unsigned int replicasNeeded = replicaCount;
    unsigned int holdersRemaining = IteratorMap.size();
    FOREACH(auto pair, IteratorMap) {
        if (RandomNumber(holdersRemaining) < replicasNeeded) {
            result.push_back(pair.First());
            --replicasNeeded;
        }
        --holdersRemaining;
    }
    return result;
}

double TChunkPlacement::GetLoadFactor(const THolder& holder)
{
    const auto& statistics = holder.Statistics;
    return
        (1.0 + statistics.UsedSpace) /
        (1.0 + statistics.UsedSpace + statistics.AvailableSpace);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
