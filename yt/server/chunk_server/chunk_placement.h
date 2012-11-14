#pragma once

#include "public.h"

#include <server/cell_master/public.h>

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

    void OnNodeRegistered(TDataNode* node);
    void OnNodeUnregistered(TDataNode* node);
    void OnNodeUpdated(TDataNode* node);

    void OnSessionHinted(TDataNode* node);

    double GetLoadFactor(TDataNode* node) const;
    double GetFillCoeff(TDataNode* node) const;

    // TODO(babenko): consider using small vectors here
    std::vector<TDataNode*> GetUploadTargets(
        int count,
        const yhash_set<Stroka>* forbiddenAddresses,
        Stroka* preferredHostName);
    std::vector<TDataNode*> GetReplicationTargets(const TChunk* chunk, int count);
    std::vector<TDataNode*> GetRemovalTargets(const TChunk* chunk, int count);
    TDataNode* GetReplicationSource(const TChunk* chunk);
    std::vector<TChunk*> GetBalancingChunks(TDataNode* node, int count);
    TDataNode* GetBalancingTarget(TChunk *chunk, double maxFillCoeff);
   
private:
    typedef ymultimap<double, TDataNode*> TLoadFactorMap;
    typedef yhash_map<TDataNode*, TLoadFactorMap::iterator> TIteratorMap;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TLoadFactorMap LoadFactorMap;
    TIteratorMap IteratorMap;

    static bool IsFull(TDataNode* node);
    static bool IsValidUploadTarget(TDataNode* targetNode);
    bool IsValidBalancingTarget(TDataNode* targetNode, TChunk *chunk) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
