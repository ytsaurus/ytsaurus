#pragma once

#include "public.h"

#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement
    : public TRefCounted
{
public:
    TChunkPlacement(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void OnHolderRegistered(const THolder& holder);
    void OnHolderUnregistered(const THolder& holder);
    void OnHolderUpdated(const THolder& holder);

    void OnSessionHinted(const THolder& holder);

    double GetLoadFactor(const THolder& holder) const;
    double GetFillCoeff(const THolder& holder) const;

    yvector<THolderId> GetUploadTargets(int count);
    yvector<THolderId> GetUploadTargets(int count, const yhash_set<Stroka>& forbiddenAddresses);
    yvector<THolderId> GetReplicationTargets(TChunk *chunk, int count);
    yvector<THolderId> GetRemovalTargets(const TChunk& chunk, int count);
    THolderId GetReplicationSource(const TChunk& chunk);
    yvector<TChunkId> GetBalancingChunks(const THolder& holder, int count);
    THolderId GetBalancingTarget(TChunk *chunk, double maxFillCoeff);
   
private:
    typedef ymultimap<double, THolderId> TLoadFactorMap;
    typedef yhash_map<THolderId, TLoadFactorMap::iterator> TIteratorMap;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TLoadFactorMap LoadFactorMap;
    TIteratorMap IteratorMap;
    yhash_map<THolderId, int> HintedSessionsMap;

    bool IsFull(const THolder& holder) const;
    int GetSessionCount(const THolder& holder) const;
    bool IsValidUploadTarget(const THolder& targetHolder) const;
    bool IsValidBalancingTarget(const THolder& targetHolder, TChunk *chunk) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
