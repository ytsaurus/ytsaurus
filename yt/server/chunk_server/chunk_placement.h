#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/node.h>

#include <yt/core/misc/nullable.h>

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
    void OnNodeDisposed(TNode* node);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::ESessionType sessionType);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        NChunkClient::ESessionType sessionType);

    TNode* GetRemovalTarget(TChunkPtrWithIndex chunkWithIndex);

    bool HasBalancingTargets(double maxFillFactor);

    std::vector<TChunkPtrWithIndex> GetBalancingChunks(
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TChunk* chunk,
        double maxFillFactor);

    int GetMaxReplicasPerRack(
        TChunk* chunk,
        TNullable<int> replicationFactorOverride = Null);

private:
    class TTargetCollector;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    //! Nodes listed here must pass #IsValidBalancingTarget test.
    TFillFactorToNodeMap FillFactorToNode_;
    //! Nodes listed here must pass #IsValidWriteTarget test.
    TFillFactorToNodeMap LoadFactorToNode_;


    void InsertToFillFactorMap(TNode* node);
    void RemoveFromFillFactorMap(TNode* node);

    void InsertToLoadFactorMap(TNode* node);
    void RemoveFromLoadFactorMap(TNode* node);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        bool forceRackAwareness,
        TNullable<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes = nullptr,
        const TNullable<Stroka>& preferredHostName = Null);

    TNode* GetBalancingTarget(
        TChunk* chunk,
        double maxFillFactor);

    static bool IsAcceptedChunkType(
        TNode* node,
        NObjectClient::EObjectType type);

    bool IsValidWriteTarget(TNode* node);
    
    bool IsValidWriteTarget(
        TNode* node,
        NObjectClient::EObjectType chunkType,
        TTargetCollector* collector,
        bool enableRackAwareness);

    bool IsValidBalancingTarget(TNode* node);

    bool IsValidBalancingTarget(
        TNode* node,
        NObjectClient::EObjectType chunkType,
        TTargetCollector* collector,
        bool enableRackAwareness);

    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        NChunkClient::ESessionType sessionType);

};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
