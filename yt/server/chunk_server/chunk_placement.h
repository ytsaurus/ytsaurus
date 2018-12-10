#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/node.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/small_set.h>

#include <util/generic/map.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement
    : public TRefCounted
{
public:
    using TDataCenterSet = SmallSet<const NNodeTrackerServer::TDataCenter*, NNodeTrackerServer::TypicalInterDCEdgeCount>;

    TChunkPlacement(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void OnNodeRegistered(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeUpdated(TNode* node);
    void OnNodeDisposed(TNode* node);

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const std::optional<TString>& preferredHostName,
        NChunkClient::ESessionType sessionType);

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TDataCenterSet& dataCenters,
        NChunkClient::ESessionType sessionType);

    TNode* GetRemovalTarget(TChunkPtrWithIndexes chunkWithIndexes);

    bool HasBalancingTargets(TMedium* medium, double maxFillFactor);

    std::vector<TChunkPtrWithIndexes> GetBalancingChunks(
        TMedium* medium,
        TNode* node,
        int replicaCount);

    TNode* AllocateBalancingTarget(
        TMedium* medium,
        TChunk* chunk,
        double maxFillFactor,
        const TDataCenterSet& dataCenters);

    int GetMaxReplicasPerRack(
        const TMedium* medium,
        TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt);
    int GetMaxReplicasPerRack(
        int mediumIndex,
        TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt);

private:
    class TTargetCollector;

    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    using TFillFactorToNodeMaps = TPerMediumArray<TFillFactorToNodeMap>;
    using TLoadFactorToNodeMaps = TPerMediumArray<TLoadFactorToNodeMap>;

    //! Nodes listed here must pass #IsValidBalancingTarget test.
    TFillFactorToNodeMaps MediumToFillFactorToNode_;
    //! Nodes listed here must pass #IsValidWriteTarget test.
    TLoadFactorToNodeMaps MediumToLoadFactorToNode_;

    void InsertToFillFactorMaps(TNode* node);
    void RemoveFromFillFactorMaps(TNode* node);

    void InsertToLoadFactorMaps(TNode* node);
    void RemoveFromLoadFactorMaps(TNode* node);

    TNodeList GetWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        bool forceRackAwareness,
        std::optional<int> replicationFactorOverride,
        const TDataCenterSet* dataCenters,
        const TNodeList* forbiddenNodes = nullptr,
        const std::optional<TString>& preferredHostName = std::nullopt);

    TNode* GetBalancingTarget(
        TMedium* medium,
        const TDataCenterSet* dataCenters,
        TChunk* chunk,
        double maxFillFactor);

    bool IsValidWriteTarget(
        TMedium* medium,
        TNode* node);

    bool IsValidWriteTarget(
        TMedium* medium,
        const TDataCenterSet* dataCenters,
        TNode* node,
        TTargetCollector* collector,
        bool enableRackAwareness);

    bool IsValidBalancingTarget(
        TMedium* medium,
        TNode* node);

    bool IsValidBalancingTarget(
        TMedium* medium,
        const TDataCenterSet* dataCenters,
        TNode* node,
        TTargetCollector* collector,
        bool enableRackAwareness);

    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        int mediumIndex,
        NChunkClient::ESessionType sessionType);

};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
