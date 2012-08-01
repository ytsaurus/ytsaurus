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

    void OnNodeRegistered(THolder* holder);
    void OnNodeUnregistered(THolder* holder);
    void OnNodeUpdated(THolder* holder);

    void OnSessionHinted(THolder* holder);

    double GetLoadFactor(THolder* holder) const;
    double GetFillCoeff(THolder* holder) const;

    // TODO(babenko): consider using small vectors here
    std::vector<THolder*> GetUploadTargets(
        int count,
        const yhash_set<Stroka>* forbiddenAddresses,
        Stroka* preferredHostName);
    std::vector<THolder*> GetReplicationTargets(const TChunk* chunk, int count);
    std::vector<THolder*> GetRemovalTargets(const TChunk* chunk, int count);
    THolder* GetReplicationSource(const TChunk* chunk);
    std::vector<TChunk*> GetBalancingChunks(THolder* holder, int count);
    THolder* GetBalancingTarget(TChunk *chunk, double maxFillCoeff);
   
private:
    typedef ymultimap<double, THolder*> TLoadFactorMap;
    typedef yhash_map<THolder*, TLoadFactorMap::iterator> TIteratorMap;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TLoadFactorMap LoadFactorMap;
    TIteratorMap IteratorMap;
    yhash_map<THolder*, int> HintedSessionsMap;

    bool IsFull(THolder* holder) const;
    int GetSessionCount(THolder* holder) const;
    bool IsValidUploadTarget(THolder* tarGetNode) const;
    bool IsValidBalancingTarget(THolder* tarGetNode, TChunk *chunk) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
