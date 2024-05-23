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
#include <util/random/fast.h>

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
    class TAllocationSession;

    class TNodeToLoadFactorMap
    {
    public:
        friend class TAllocationSession;

        TNodeToLoadFactorMap();

        void InsertNodeOrCrash(TNodeId nodeId, double loadFactor);
        void RemoveNode(TNodeId nodeId);
        bool Contains(TNodeId nodeId) const;
        [[nodiscard]] bool Empty() const;
        ui64 Size() const;

        //NB: Here we can rely on the fact that allocation sessions cannot overlap and not use any synchronization primitives.
        TAllocationSession StartAllocationSession(ui32 nodesToCheckBeforeGivingUpOnWriteTargetAllocation);

    private:
        //NB: This may change the order of the elements in Values_!
        TNodeId PickRandomNode(ui32 nodesChecked);
        void SwapNodes(ui32 firstIndex, ui32 secondIndex);

        std::vector<std::pair<TNodeId, double>> Values_;
        THashMap<TNodeId, i64> NodeToIndex_;
        TReallyFastRng32 Rng_;
    };

    class TAllocationSession
    {
    public:
        friend class TNodeToLoadFactorMap;

        //! Picks a random node using the power of two choices.
        // Does not return the node more than once within the current session.
        [[nodiscard]] TNodeId PickRandomNode();

        //! Allocation session is considered unsuccessful iff the amount of attempts to pick a node
        // exceedes the corresponding limit.
        bool HasFailed() const;

    private:
        TAllocationSession(
            TNodeToLoadFactorMap* associatedMap,
            ui32 nodesToCheckBeforeGivingUpOnWriteTargetAllocation);

        TNodeToLoadFactorMap* AssociatedMap_;
        ui32 NodesToCheckBeforeFailing_;
        ui32 NodesChecked_ = 0;
    };


    NCellMaster::TBootstrap* const Bootstrap_;
    const TChunkManagerConfigPtr Config_;
    const TConsistentChunkPlacementPtr ConsistentPlacement_;

    using TNodeToLoadFactorMaps = THashMap<const TDomesticMedium*, TNodeToLoadFactorMap>;
    //! Nodes listed here must pass #IsValidWriteTargetToInsert test.
    TNodeToLoadFactorMaps MediumToNodeToLoadFactor_;

    // COMPAT(h0pless): Remove this when power of two choices will prove to be working.
    using TLoadFactorToNodeMaps = THashMap<const TDomesticMedium*, TLoadFactorToNodeMap>;
    TLoadFactorToNodeMaps MediumToLoadFactorToNode_;

    bool EnableTwoRandomChoicesWriteTargetAllocation_ = false;
    int NodesToCheckBeforeGivingUpOnWriteTargetAllocation_ = 0;
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
        NChunkClient::ESessionType sessionType,
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
