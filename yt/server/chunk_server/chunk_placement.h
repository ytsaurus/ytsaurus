#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/misc/small_set.h>
#include <ytlib/misc/nullable.h>

#include <server/node_tracker_server/node_tracker.h>

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

    void Initialize();

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);

    double GetLoadFactor(TNode* node) const;
    double GetFillCoeff(TNode* node) const;

    TNodeList AllocateUploadTargets(
        int replicaCount,
        const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName);

    TNodeList AllocateReplicationTargets(
        const TChunk* chunk,
        int targetCount);

    TNodeList GetRemovalTargets(
        TChunkPtrWithIndex chunkWithIndex,
        int targetCount);

    TNode* GetReplicationSource(TChunkPtrWithIndex chunkWithIndex);

    bool HasBalancingTargets(double maxFillCoeff);

    std::vector<TChunkPtrWithIndex> GetBalancingChunks(TNode* node, int count);

    TNode* AllocateBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillCoeff);

private:
    typedef ymultimap<double, TNode*> TCoeffToNode;
    typedef yhash_map<TNode*, TCoeffToNode::iterator> TNodeToCoeffIt;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TCoeffToNode LoadFactorToNode;
    TNodeToCoeffIt NodeToLoadFactorIt;

    TCoeffToNode FillCoeffToNode;
    TNodeToCoeffIt NodeToFillCoeffIt;

    TNodeList GetUploadTargets(
        int targetCount,
        const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName);

    TNodeList GetReplicationTargets(
        const TChunk* chunk,
        int targetCount);

    TNode* GetBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillCoeff);

    void OnSessionHinted(TNode* node);

    static bool IsFull(TNode* node);

    static bool IsValidUploadTarget(TNode* node);
    bool IsValidBalancingTarget(TNode* node, TChunkPtrWithIndex chunkWithIndex) const;
    bool IsValidRemovalTarget(TNode* node);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
