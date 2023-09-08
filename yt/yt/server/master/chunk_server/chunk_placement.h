#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "domestic_medium.h"
#include "consistent_chunk_placement.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/client/object_client/helpers.h>

#include <util/generic/map.h>

#include <optional>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TLoadFactorToNodeMapItemComparator
{
    TLoadFactorToNodeMapItemComparator() = default;

    bool operator()(TLoadFactorToNodeMap::const_reference lhs, TLoadFactorToNodeMap::const_reference rhs)
    {
        return lhs.first < rhs.first;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPlacement
    : public TRefCounted
{
public:
    TChunkPlacement(
        NCellMaster::TBootstrap* bootstrap,
        TConsistentChunkPlacementPtr consistentPlacement);

    void Clear();
    void Initialize();

    void OnNodeRegistered(TNode* node);
    void OnNodeUpdated(TNode* node);
    void OnNodeUnregistered(TNode* node);
    void OnNodeDisposed(TNode* node);

    void OnDataCenterChanged(NNodeTrackerServer::TDataCenter* dataCenter);

    bool IsDataCenterFeasible(const NNodeTrackerServer::TDataCenter* dataCenter) const;

    TNodeList AllocateWriteTargets(
        TDomesticMedium* medium,
        TChunk* chunk,
        const TChunkLocationPtrWithReplicaInfoList& replicas,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        const std::optional<TString>& preferredHostName,
        NChunkClient::ESessionType sessionType);

    TNodeList AllocateWriteTargets(
        TDomesticMedium* medium,
        TChunk* chunk,
        const TChunkLocationPtrWithReplicaInfoList& replicas,
        const TChunkReplicaIndexList& replicaIndexes,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        NChunkClient::ESessionType sessionType,
        TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica = {});

    TNodeList GetConsistentPlacementWriteTargets(const TChunk* chunk, int mediumIndex);

    // NB: Removal queue is stored in chunk location but actual deletion may happen
    // on different location of the same node.
    TChunkLocation* GetRemovalTarget(
        TChunkPtrWithReplicaAndMediumIndex replica,
        const TChunkLocationPtrWithReplicaInfoList& replicas);

    int GetMaxReplicasPerRack(
        const TMedium* medium,
        const TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt) const;
    int GetMaxReplicasPerRack(
        int mediumIndex,
        const TChunk* chunk,
        std::optional<int> replicationFactorOverride = std::nullopt) const;

    int GetMaxReplicasPerDataCenter(
        const TDomesticMedium* medium,
        const TChunk* chunk,
        const NNodeTrackerServer::TDataCenter* dataCenter,
        std::optional<int> replicationFactorOverride = std::nullopt) const;
    int GetMaxReplicasPerDataCenter(
        int mediumIndex,
        const TChunk* chunk,
        const NNodeTrackerServer::TDataCenter* dataCenter,
        std::optional<int> replicationFactorOverride = std::nullopt) const;

    const std::vector<TError>& GetAlerts() const;

private:
    class TTargetCollector;

    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkManagerConfigPtr Config_;
    const TConsistentChunkPlacementPtr ConsistentPlacement_;

    using TLoadFactorToNodeMaps = THashMap<const TDomesticMedium*, TLoadFactorToNodeMap>;

    //! Nodes listed here must pass #IsValidWriteTargetToInsert test.
    TLoadFactorToNodeMaps MediumToLoadFactorToNode_;

    bool IsDataCenterAware_ = false;

    THashSet<const NNodeTrackerServer::TDataCenter*> StorageDataCenters_;
    THashSet<const NNodeTrackerServer::TDataCenter*> BannedStorageDataCenters_;
    THashSet<const NNodeTrackerServer::TDataCenter*> AliveStorageDataCenters_;
    std::vector<TError> DataCenterSetErrors_;

    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/);

    void RegisterNode(TNode* node);
    void UnregisterNode(TNode* node);

    void InsertToLoadFactorMaps(TNode* node);
    void RemoveFromLoadFactorMaps(TNode* node);

    TNodeList GetWriteTargets(
        TDomesticMedium* medium,
        TChunk* chunk,
        const TChunkLocationPtrWithReplicaInfoList& replicas,
        const TChunkReplicaIndexList& replicaIndexes,
        int desiredCount,
        int minCount,
        bool forceRackAwareness,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes = nullptr,
        const TNodeList* allocatedNodes = nullptr,
        const std::optional<TString>& preferredHostName = std::nullopt,
        TChunkLocationPtrWithReplicaInfo unsafelyPlacedReplica = {});

    std::optional<TNodeList> FindConsistentPlacementWriteTargets(
        TDomesticMedium* medium,
        TChunk* chunk,
        const TChunkLocationPtrWithReplicaInfoList& replicas,
        const TChunkReplicaIndexList& replicaIndexes,
        int desiredCount,
        int minCount,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        TNode* preferredNode);

    TNode* FindPreferredNode(
        const std::optional<TString>& preferredHostName,
        TDomesticMedium* medium);

    bool IsValidWriteTargetToInsert(TDomesticMedium* medium, TNode* node);
    bool IsValidWriteTargetToAllocate(
        TNode* node,
        TTargetCollector* collector,
        bool enableRackAwareness,
        bool enableDataCenterAwareness);
    bool IsValidWriteTargetCore(TNode* node);
    // Preferred nodes are special: they don't come from load-factor maps and
    // thus may not have been vetted by #IsValidWriteTargetToInsert. Thus,
    // additional checking of their media is required.
    bool IsValidPreferredWriteTargetToAllocate(TNode* node, TDomesticMedium* medium);

    bool IsValidRemovalTarget(TNode* node);

    void AddSessionHint(
        TNode* node,
        int mediumIndex,
        NChunkClient::ESessionType sessionType);

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig() const;
    bool IsConsistentChunkPlacementEnabled() const;

    void RecomputeDataCenterSets();
};

DEFINE_REFCOUNTED_TYPE(TChunkPlacement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
