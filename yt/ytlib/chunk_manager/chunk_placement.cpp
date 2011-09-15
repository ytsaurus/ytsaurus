#include "chunk_placement.h"

#include "../misc/foreach.h"

#include <util/random/random.h>

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

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

yvector<THolderId> TChunkPlacement::GetTargetHolders(int replicaCount)
{
    // TODO: do not list holders that are nearly full
    yvector<THolderId> result;
    result.reserve(replicaCount);
    unsigned int replicasNeeded = replicaCount;
    unsigned int holdersRemaining = IteratorMap.size();
    FOREACH(const auto& pair, IteratorMap) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
