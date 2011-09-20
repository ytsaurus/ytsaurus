#pragma once

#include "chunk_manager.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkPlacement> TPtr;

    TChunkPlacement(TChunkManager::TPtr chunkManager);

    void AddHolder(const THolder& holder);
    void RemoveHolder(const THolder& holder);
    void UpdateHolder(const THolder& holder);

    yvector<THolderId> GetUploadTargets(int count);
    yvector<THolderId> GetRemovalTargets(const TChunk& chunk, int count);
    THolderId GetReplicationSource(const TChunk& chunk);
   
private:
    typedef ymultimap<double, THolderId> TPreferenceMap;
    typedef yhash_map<THolderId, TPreferenceMap::iterator> TIteratorMap;

    TChunkManager::TPtr ChunkManager;
    TPreferenceMap PreferenceMap;
    TIteratorMap IteratorMap;

    static double GetLoadFactor(const THolder& holder);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
