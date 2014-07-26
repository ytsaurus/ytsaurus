#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <core/misc/small_vector.h>
#include <core/misc/small_set.h>
#include <core/misc/nullable.h>

#include <server/node_tracker_server/node_tracker.h>

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

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);

    double GetFillFactor(TNode* node) const;

    TNodeList AllocateWriteTargets(
        int replicaCount,
        const TNodeSet* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType,
        NObjectClient::EObjectType chunkType);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int targetCount,
        NChunkClient::EWriteSessionType sessionType,
        NObjectClient::EObjectType chunkType);

    TNodeList GetRemovalTargets(
        TChunkPtrWithIndex chunkWithIndex,
        int replicaCount);

    bool HasBalancingTargets(double maxFillFactor);

    std::vector<TChunkPtrWithIndex> GetBalancingChunks(
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillFactor,
        NObjectClient::EObjectType chunkType);

private:
    typedef ymultimap<double, TNode*> TFactorToNode;
    typedef yhash_map<TNode*, TFactorToNode::iterator> TNodeToFactorIt;

    TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* Bootstrap_;

    std::vector<TNode*> LoadRankToNode_;

    //! Enables traversing nodes by increasing fill factor, which is useful for finding balancing targets.
    //! Nodes with the number of replication write sessions exceeding the limits are omitted.
    TFactorToNode FillFactorToNode_;

    //! Provides backpointers from nodes to positions in #FillFactorToNode_.
    TNodeToFactorIt NodeToFillFactorIt_;


    static int GetLoadFactor(TNode* node);

    TNodeList GetWriteTargets(
        int targetCount,
        const TNodeSet* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType,
        NObjectClient::EObjectType chunkType);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int targetCount,
        NChunkClient::EWriteSessionType sessionType,
        NObjectClient::EObjectType chunkType);

    TNode* GetBalancingTarget(
        TChunkPtrWithIndex chunkWithIndex,
        double maxFillFactor,
        NObjectClient::EObjectType chunkType);

    static bool IsFull(TNode* node);

    static bool AcceptsChunkType(TNode* node, NObjectClient::EObjectType type);

    static bool IsValidWriteTarget(
        TNode* node,
        NChunkClient::EWriteSessionType sessionType,
        NObjectClient::EObjectType chunkType);
    
    bool IsValidBalancingTarget(
        TNode* node,
        TChunkPtrWithIndex chunkWithIndex,
        NObjectClient::EObjectType chunkType) const;
    
    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        NChunkClient::EWriteSessionType sessionType);

};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
