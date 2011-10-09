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
    void AddHolderSessionHint(const THolder& holder);

    double GetLoadFactor(const THolder& holder) const;
    double GetFillCoeff(const THolder& holder) const;

    yvector<THolderId> GetUploadTargets(int count);
    yvector<THolderId> GetReplicationTargets(const TChunk& chunk, int count);
    yvector<THolderId> GetRemovalTargets(const TChunk& chunk, int count);
    THolderId GetReplicationSource(const TChunk& chunk);
    yvector<TChunkId> GetBalancingChunks(const THolder& holder, int count);
    THolderId GetBalancingTarget(const TChunk& chunk, double maxFillCoeff);
   
private:
    typedef ymultimap<double, THolderId> TLoadFactorMap;
    typedef yhash_map<THolderId, TLoadFactorMap::iterator> TIteratorMap;

    TChunkManager::TPtr ChunkManager;
    TLoadFactorMap LoadFactorMap;
    TIteratorMap IteratorMap;
    yhash_map<THolderId, int> HintedSessionsMap;

    bool IsFull(const THolder& holder) const;
    int GetSessionCount(const THolder& holder) const;
    bool IsValidUploadTarget(const THolder& targetHolder) const;
    bool IsValidBalancingTarget(const THolder& targetHolder, const TChunk& chunk) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
