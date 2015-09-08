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

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);
    void OnNodeRemoved(TNode* node);

    double GetFillFactor(TNode* node) const;

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        NChunkClient::EWriteSessionType sessionType);

    TNodeList AllocateWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        NChunkClient::EWriteSessionType sessionType);

    TNode* GetRemovalTarget(TChunkPtrWithIndex chunkWithIndex);

    bool HasBalancingTargets(double maxFillFactor);

    std::vector<TChunkPtrWithIndex> GetBalancingChunks(
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TChunk* chunk,
        double maxFillFactor);

private:
    class TTargetCollector;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    std::vector<TNode*> LoadRankToNode_;
    TFillFactorToNodeMap FillFactorToNode_;


    static int GetLoadFactor(TNode* node);

    void InsertToFillFactorMap(TNode* node);
    void RemoveFromFillFactorMap(TNode* node);

    void InsertToLoadRankList(TNode* node);
    void RemoveFromLoadRankList(TNode* node);
    void AdvanceInLoadRankList(TNode* node);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName);

    TNodeList GetWriteTargets(
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride);

    TNode* GetBalancingTarget(
        TChunk* chunk,
        double maxFillFactor);

    static bool IsFull(TNode* node);

    static bool IsAcceptedChunkType(
        TNode* node,
        NObjectClient::EObjectType type);

    bool IsValidWriteTarget(
        TNode* node,
        NObjectClient::EObjectType chunkType,
        TTargetCollector* collector,
        bool enableRackAwareness);
    
    bool IsValidBalancingTarget(
        TNode* node,
        NObjectClient::EObjectType chunkType,
        TTargetCollector* collector,
        bool enableRackAwareness);

    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        NChunkClient::EWriteSessionType sessionType);

    int GetMaxReplicasPerRack(
        TChunk* chunk,
        TNullable<int> replicationFactorOverride);

};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
