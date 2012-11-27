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
    
    std::vector<TDataNode*> GetRemovalTargets(const TChunk* chunk, int count);

    std::vector<TDataNode*> GetReplicationTargets(const TChunk* chunk, int count);
    TDataNode* GetReplicationSource(const TChunk* chunk);

    std::vector<TChunk*> GetBalancingChunks(TDataNode* node, int count);
    bool HasBalancingTargets(double maxFillCoeff);
    TDataNode* GetBalancingTarget(TChunk *chunk, double maxFillCoeff);
   
private:
    typedef ymultimap<double, TDataNode*> TCoeffToNode;
    typedef yhash_map<TDataNode*, TCoeffToNode::iterator> TNodeToCoeffIt;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TCoeffToNode LoadFactorToNode;
    TNodeToCoeffIt NodeToLoadFactorIt;

    TCoeffToNode FillCoeffToNode;
    TNodeToCoeffIt NodeToFillCoeffIt;

    static bool IsFull(TDataNode* node);
    static bool IsValidUploadTarget(TDataNode* targetNode);
    bool IsValidBalancingTarget(TDataNode* targetNode, TChunk *chunk) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
