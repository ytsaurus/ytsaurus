#include "chunk_placement.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

void TChunkPlacement::RegisterHolder(const THolder& holder)
{
    double preference = GetPreference(holder);
    TPreferenceMap::iterator it = PreferenceMap.insert(MakePair(preference, holder.Id));
    YVERIFY(IteratorMap.insert(MakePair(holder.Id, it)).Second());
}

void TChunkPlacement::UnregisterHolder(const THolder& holder)
{
    TIteratorMap::iterator iteratorIt = IteratorMap.find(holder.Id);
    YASSERT(iteratorIt != IteratorMap.end());
    TPreferenceMap::iterator preferenceIt = iteratorIt->Second();
    PreferenceMap.erase(preferenceIt);
    IteratorMap.erase(iteratorIt);
}

void TChunkPlacement::UpdateHolder(const THolder& holder)
{
    UnregisterHolder(holder);
    RegisterHolder(holder);
}

yvector<THolderId> TChunkPlacement::GetTargetHolders(int replicaCount)
{
    yvector<THolderId> result;
    TPreferenceMap::reverse_iterator it = PreferenceMap.rbegin();
    while (it != PreferenceMap.rend() && result.ysize() < replicaCount) {
        result.push_back((*it++).second);
    }
    return result;
}

double TChunkPlacement::GetPreference(const THolder& holder)
{
    const THolderStatistics& statistics = holder.Statistics;
    return
        (1.0 + statistics.UsedSpace) /
        (1.0 + statistics.UsedSpace + statistics.AvailableSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
