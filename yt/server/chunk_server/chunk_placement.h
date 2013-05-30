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
    double GetFillFactor(TNode* node) const;

    TNodeList AllocateWriteTargets(
        int replicaCount,
        const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int targetCount,
        NChunkClient::EWriteSessionType sessionType);

    TNodeList GetRemovalTargets(
        TChunkPtrWithIndex chunkWithIndex,
        int replicaCount);

    bool HasBalancingTargets(double maxFillFactor);

    std::vector<TChunkPtrWithIndex> GetBalancingChunks(
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillFactor);

private:
    typedef ymultimap<double, TNode*> TFactorToNode;
    typedef yhash_map<TNode*, TFactorToNode::iterator> TNodeToFactorIt;

    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TFactorToNode LoadFactorToNode;
    TNodeToFactorIt NodeToLoadFactorIt;

    TFactorToNode FillFactorToNode;
    TNodeToFactorIt NodeToFilFactorIt;

    TNodeList GetWriteTargets(
        int targetCount,
        const TSmallSet<TNode*, TypicalReplicaCount>* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int targetCount,
        NChunkClient::EWriteSessionType sessionType);

    TNode* GetBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillFactor);

    static bool IsFull(TNode* node);

    static bool IsValidWriteTarget(
        TNode* node,
        NChunkClient::EWriteSessionType sessionType);
    
    bool IsValidBalancingTarget(
        TNode* node,
        TChunkPtrWithIndex chunkWithIndex) const;
    
    bool IsValidRemovalTarget(TNode* node);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
