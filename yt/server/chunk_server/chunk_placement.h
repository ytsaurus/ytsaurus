#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <core/misc/nullable.h>

#include <server/node_tracker_server/node.h>

#include <server/cell_master/public.h>

#include <util/generic/map.h>

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

    void Start();
    void Stop();

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);

    double GetFillFactor(TNode* node) const;

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int replicaCount,
        const TSortedNodeList* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int targetCount,
        NChunkClient::EWriteSessionType sessionType);

    TNode* GetRemovalTarget(TChunkPtrWithIndex chunkWithIndex);

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

    TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;

    std::vector<TNode*> LoadRankToNode_;
    TFillFactorToNodeMap FillFactorToNode_;


    static int GetLoadFactor(TNode* node);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int targetCount,
        const TSortedNodeList* forbiddenNodes,
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

    static bool IsAcceptedChunkType(
        TNode* node,
        NObjectClient::EObjectType type);

    static bool IsValidWriteTarget(
        TNode* node,
        TChunk* chunk,
        NChunkClient::EWriteSessionType sessionType);
    
    bool IsValidBalancingTarget(
        TNode* node,
        TChunkPtrWithIndex chunkWithIndex) const;
    
    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        NChunkClient::EWriteSessionType sessionType);

};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
