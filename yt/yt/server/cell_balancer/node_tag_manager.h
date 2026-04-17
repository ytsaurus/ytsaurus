#include "cypress_bindings.h"
#include "public.h"
#include "spare_instances.h"

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTagManager
{
public:
    TNodeTagManager(
        std::string bundleName,
        const TSchedulerInputState& input,
        TSpareInstanceAllocator<TSpareNodesInfo>* spareNodeAllocator,
        TSchedulerMutations* mutations,
        INodeTrackerPtr nodeTracker);

    // * Collects alive bundle nodes (with immediate grace period).
    // * Picks data centers to populate.
    // * Creates assignment/releasement operations for bundle nodes
    //   for own nodes in active/inactive data centers.
    // * Processes assignments/releasements of bundle nodes.
    // * Creates spare node asignments/releasements if needed.
    // * Processes assignments/releasements of spare nodes.
    void SetNodeTags();

private:
    const std::string BundleName_;
    const TSchedulerInputState& Input_;
    TSpareInstanceAllocator<TSpareNodesInfo>* const SpareNodeAllocator_;
    TSchedulerMutations* const Mutations_;
    const INodeTrackerPtr NodeTracker_;

    NLogging::TLogger Logger;

    // Handles node assignment pipeline. Performs the following actions
    // with the node in order:
    // * set node tag and decommissioned flag
    // * wait for the dynamic config
    // * remove the decommissioned flag
    // Returns true if the pipeline is completed.
    bool ProcessNodeAssignment(const std::string& nodeAddress);

    // Handles node releasement pipeline. Performs the following actions
    // with the node in order:
    // * set decommissioned flag
    // * wait for the cells to go away from the node
    // * remove bundle tag
    // * if necessary, decommission released node
    // Returns true if the pipeline is completed.
    bool ProcessNodeReleasement(
        const std::string& nodeAddress,
        bool leaveDecommissioned);

    void RemoveTagsFromNodes(const THashSet<std::string>& offlineNodes);

    // For each node from nodeAssignments:
    //  * creates an alert if assignment is stuck
    //  * runs ProcessNodeAssignment and removes node from |nodeAssignments|
    //    on success.
    void ProcessNodeAssignments(
        TIndexedEntries<TNodeTagFilterOperationState>* nodeAssignments);

    // For each node from nodeAssignments:
    //  * creates an alert if assignment is stuck
    //  * runs ProcessNodeReleasement and removes node from |nodeReleasements|
    //    on success.
    void ProcessNodeReleasements(
        bool leaveDecommissioned,
        TIndexedEntries<TNodeTagFilterOperationState>* nodeReleasements);

    // Creates operations of kind |bundleState->SpareNodeReleasements|.
    // Takes nodes from spareNodesInfo->UsedByBundle.
    // Does not modify nodes.
    // Does not do anything if spare node assignment is in progress.
    void TryCreateSpareNodeReleasements(
        int slotsToRelease,
        const TSpareNodesInfo* spareNodesInfo);

    // Creates operations of kind |bundleState->SpareNodeAssignments|.
    // Takes spare nodes from allocator.
    // Does not modify nodes.
    void TryCreateSpareNodeAssignments(
        const std::string& dataCenterName,
        int slotsToAdd,
        TSpareInstanceAllocator<TSpareNodesInfo>* spareNodeAllocator);

    // Creates operations of kind |bundleState->BundleNodeAssignments|
    // for all nodes in |aliveNodes|.
    // May cancel node releasement and unset decommission flag.
    void TryCreateBundleNodesAssignment(
        const THashSet<std::string>& aliveNodes);

    // Creates operations of kind |bundleState->BundleNodeReleasements|
    // for nodes in |nodesToRelease|.
    // Does not do anything if bundle cells are being removed or if node
    // is already being assigned/released.
    void TryCreateBundleNodesReleasement(
        const THashSet<std::string>& nodesToRelease);

    int GetAvailableAliveTabletNodeCount(
        const std::string& dataCenterName,
        const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
        const TPerDataCenterSpareNodesInfo& spareNodesInfo);

    int GetAssignedTabletNodeCount(
        const std::string& nodeTagFilter,
        const std::string& dataCenterName,
        const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
        const TPerDataCenterSpareNodesInfo& spareNodesInfo,
        const TIndexedEntries<TTabletNodeInfo>& tabletNodes);

    int GetAssignedTabletCellCount(
        const std::string& dataCenterName,
        const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
        const TIndexedEntries<TTabletNodeInfo>& tabletNodes);

    THashSet<std::string> GetDataCentersToPopulate(
        const std::string& nodeTagFilter,
        const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveNodes,
        const TPerDataCenterSpareNodesInfo& spareNodesInfo);
};

////////////////////////////////////////////////////////////////////////////////

void InitializeZoneToSpareNodes(TSchedulerInputState& input, TSchedulerMutations* mutations);

void ManageNodeTags(
    TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator,
    TSchedulerMutations* mutations,
    const INodeTrackerPtr& nodeTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
