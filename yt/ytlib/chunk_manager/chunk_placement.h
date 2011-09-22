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


    static double GetLoadFactor(const THolder& holder);

    yvector<THolderId> GetUploadTargets(int count);
    yvector<THolderId> GetRemovalTargets(const TChunk& chunk, int count);
    THolderId GetReplicationSource(const TChunk& chunk);
    yvector<TChunkId> GetBalancingChunks(const THolder& holder, int count);
    THolderId GetBalancingTarget(const TChunk& chunk, double maxLoadFactor);
   
private:
    typedef ymultimap<double, THolderId> TLoadFactorMap;
    typedef yhash_map<THolderId, TLoadFactorMap::iterator> TIteratorMap;

    TChunkManager::TPtr ChunkManager;
    TLoadFactorMap LoadFactorMap;
    TIteratorMap IteratorMap;

    bool IsValidBalancingTarget(const THolder& targetHolder, const TChunk& chunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
