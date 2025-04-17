#include "bundle_scheduler.h"
#include "config.h"
#include "cypress_bindings.h"

#include <compare>
#include <algorithm>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BundleControllerLogger;
static constexpr bool LeaveNodesDecommissioned = true;
static constexpr bool DoNotLeaveNodesDecommissioned = false;

////////////////////////////////////////////////////////////////////////////////

static int GetCeiledShare(int totalAmount, int partCount)
{
    YT_VERIFY(partCount > 0);

    return (totalAmount + partCount - 1) / partCount;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> GetBundlesByTag(
    const TTabletNodeInfoPtr& nodeInfo,
    const THashMap<std::string, std::string>& filterTagToBundleName)
{
    std::vector<std::string> result;

    for (const auto& tag : nodeInfo->UserTags) {
        if (auto it = filterTagToBundleName.find(tag); it != filterTagToBundleName.end()) {
            result.push_back(GetOrCrash(filterTagToBundleName, tag));
        }
    }

    return result;
}

int GetReadyNodeCount(
    const TBundleInfoPtr& bundleInfo,
    const THashSet<std::string>& aliveBundleNodes,
    const TSchedulerInputState& input)
{
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;

    int readyNodeCount = 0;
    for (const auto& nodeName : aliveBundleNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->UserTags.count(nodeTagFilter) != 0 && !nodeInfo->Decommissioned) {
            ++readyNodeCount;
        }
    }

    return readyNodeCount;
}

////////////////////////////////////////////////////////////////////////////////

TPerDataCenterSpareNodesInfo GetSpareNodesInfo(
    const std::string& zoneName,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto zoneIt = input.Zones.find(zoneName);
    if (zoneIt == input.Zones.end()) {
        return {};
    }

    auto spareBundle = GetSpareBundleName(zoneIt->second);
    auto spareNodesIt = input.BundleNodes.find(spareBundle);
    if (spareNodesIt == input.BundleNodes.end()) {
        return {};
    }

    // NodeTagFilter To Bundle Name.
    THashMap<std::string, std::string> filterTagToBundleName;
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        filterTagToBundleName[bundleInfo->NodeTagFilter] = bundleName;
    }

    THashMap<std::string, std::string> operationsToBundle;
    for (const auto& [bundleName, bundleState] : input.BundleStates) {
        for (const auto& [spareNode, _] : bundleState->SpareNodeAssignments) {
            operationsToBundle[spareNode] = bundleName;
        }

        for (const auto& [spareNode, _] : bundleState->SpareNodeReleasements) {
            operationsToBundle[spareNode] = bundleName;
        }
    }

    const auto& spareBundleState = GetOrInsert(mutations->ChangedStates, spareBundle, New<TBundleControllerState>);

    auto zoneAliveNodes = GetAliveNodes(
        spareBundle,
        spareNodesIt->second,
        input,
        spareBundleState,
        EGracePeriodBehaviour::Immediately);

    TPerDataCenterSpareNodesInfo result;

    for (const auto& [dataCenterName, aliveNodes] : zoneAliveNodes) {
        auto& spareNodes = result[dataCenterName];

        for (const auto& spareNodeName : aliveNodes) {
            auto nodeInfo = GetOrCrash(input.TabletNodes, spareNodeName);

            bool hasMaintenanceRequests = !nodeInfo->CmsMaintenanceRequests.empty();

            auto assignedBundlesNames = GetBundlesByTag(nodeInfo, filterTagToBundleName);
            if (assignedBundlesNames.empty() && operationsToBundle.count(spareNodeName) == 0) {
                if (hasMaintenanceRequests) {
                    spareNodes.ScheduledForMaintenance.push_back(spareNodeName);
                } else {
                    spareNodes.FreeNodes.push_back(spareNodeName);
                }
                continue;
            }

            if (std::ssize(assignedBundlesNames) > 1) {
                YT_LOG_WARNING("Spare node is assigned to the multiple bundles (Node: %v, Bundles: %v)",
                    spareNodeName,
                    assignedBundlesNames);

                mutations->AlertsToFire.push_back({
                    .Id = "node_with_multiple_node_tag_filters",
                    .Description = Format("Spare node: %v is assigned to the multiple bundles: %v.",
                        spareNodeName,
                        assignedBundlesNames),
                });
                continue;
            }

            const auto bundleName = !assignedBundlesNames.empty() ? assignedBundlesNames.back() : GetOrCrash(operationsToBundle, spareNodeName);

            if (nodeInfo->Decommissioned && operationsToBundle.count(spareNodeName) == 0) {
                YT_LOG_WARNING("Spare node is externally decommissioned (Node: %v, AssignedToBundle: %v)",
                    spareNodeName,
                    bundleName);

                spareNodes.ExternallyDecommissioned.push_back(spareNodeName);
                continue;
            }

            const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
            if (!bundleInfo->EnableBundleController) {
                YT_LOG_WARNING("Spare node is occupied by unmanaged bundle (Node: %v, Bundles: %v)",
                    spareNodeName,
                    bundleName);

                spareNodes.UsedByBundle[bundleName].push_back(spareNodeName);
                continue;
            }

            TBundleControllerStatePtr bundleState;
            if (const auto it = mutations->ChangedStates.find(bundleName); it != mutations->ChangedStates.end()) {
                bundleState = it->second;
            } else if (const auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
                bundleState = it->second;
            }

            if (bundleState && bundleState->SpareNodeReleasements.count(spareNodeName) != 0) {
                spareNodes.ReleasingByBundle[bundleName].push_back(spareNodeName);
            } else {
                spareNodes.UsedByBundle[bundleName].push_back(spareNodeName);
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Returns true if the node finally/already assigned to the bundle.
bool ProcessNodeAssignment(
    const std::string& nodeName,
    const std::string& bundleName,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

    if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
        auto tags = nodeInfo->UserTags;
        tags.insert(nodeTagFilter);
        mutations->ChangedNodeUserTags[nodeName] = mutations->WrapMutation(std::move(tags));
        if (input.Config->DecommissionReleasedNodes) {
            mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(true);
        }

        YT_LOG_INFO("Setting node tag filter and decommission "
            "(Bundle: %v, TabletNode: %v, NodeTagFilter: %v, Decommissioned: %v)",
            bundleName,
            nodeName,
            nodeTagFilter,
            input.Config->DecommissionReleasedNodes);

        return false;
    }

    bool isNodeOnline = InstanceStateOnline == nodeInfo->State;

    if (!isNodeOnline) {
        YT_LOG_WARNING("Node went offline during assigning to bundle "
            "(Bundle: %v, TabletNode: %v, NodeState: %v, Banned: %v, LastSeenTime: %v)",
            bundleName,
            nodeName,
            nodeInfo->State,
            nodeInfo->Banned,
            nodeInfo->LastSeenTime);
    }

    const auto& targetConfig = bundleInfo->TargetConfig;

    if (isNodeOnline && targetConfig->CpuLimits->WriteThreadPoolSize != std::ssize(nodeInfo->TabletSlots)) {
        YT_LOG_DEBUG("Node has not applied dynamic bundle config yet "
            "(Bundle: %v, TabletNode: %v, ExpectedSlotCount: %v, ActualSlotCount: %v)",
            bundleName,
            nodeName,
            targetConfig->CpuLimits->WriteThreadPoolSize,
            std::ssize(nodeInfo->TabletSlots));

        return false;
    }

    auto tabletStatic = targetConfig->MemoryLimits->TabletStatic;
    if (isNodeOnline && tabletStatic && *tabletStatic != nodeInfo->Statistics->Memory->TabletStatic->Limit) {
        YT_LOG_DEBUG("Node has not applied dynamic bundle config yet "
            "(Bundle: %v, TabletNode: %v, ExpectedTabletStatic: %v, ActualTabletStatic: %v)",
            bundleName,
            nodeName,
            tabletStatic,
            nodeInfo->Statistics->Memory->TabletStatic->Limit);

        return false;
    }

    if (nodeInfo->Decommissioned) {
        YT_LOG_DEBUG("Removing decommissioned flag after applying bundle dynamic config "
            "(Bundle: %v, TabletNode: %v)",
            bundleName,
            nodeName);
        mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(false);
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateSpareNodesReleasements(
    const std::string& bundleName,
    const TSchedulerInputState& input,
    int slotsToRelease,
    TSpareNodesInfo* spareNodesInfo,
    const TBundleControllerStatePtr& bundleState)
{
    if (!bundleState->SpareNodeAssignments.empty()) {
        // Let's not mix spare node assigning with releasing.
        return;
    }

    if (!bundleState->RemovingCells.empty()) {
        return;
    }

    const auto& usingSpareNodes = spareNodesInfo->UsedByBundle[bundleName];
    auto it = usingSpareNodes.begin();

    auto now = TInstant::Now();

    while (slotsToRelease > 0 && it != usingSpareNodes.end()) {
        const auto& nodeName = *it;

        auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (std::ssize(nodeInfo->TabletSlots) <= slotsToRelease) {
            YT_LOG_INFO("Releasing spare node (Bundle: %v, NodeName: %v, NodeSlotCount: %v, SlotsToRelease: %v)",
                bundleName,
                nodeName,
                std::ssize(nodeInfo->TabletSlots),
                slotsToRelease);

            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->SpareNodeReleasements[nodeName] = operation;

            slotsToRelease -= std::ssize(nodeInfo->TabletSlots);
        }
        ++it;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateSpareNodesAssignment(
    const std::string& bundleName,
    const std::string& zoneName,
    const std::string& dataCenterName,
    const TSchedulerInputState& input,
    int slotsToAdd,
    TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator,
    const TBundleControllerStatePtr& bundleState)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    int perNodeSlotCount = bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);
    auto now = TInstant::Now();

    while (slotsToAdd > 0 && spareNodesAllocator.HasInstances(zoneName, dataCenterName)) {
        auto nodeName = spareNodesAllocator.Allocate(zoneName, dataCenterName, bundleName);
        auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        YT_LOG_INFO("Assigning spare node (Bundle: %v, NodeName: %v)",
            bundleName,
            nodeName);

        auto operation = New<TNodeTagFilterOperationState>();
        operation->CreationTime = now;
        bundleState->SpareNodeAssignments[nodeName] = operation;

        slotsToAdd -= perNodeSlotCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateBundleNodesAssignment(
    const std::string& bundleName,
    const TSchedulerInputState& input,
    const THashSet<std::string>& aliveNodes,
    const TBundleControllerStatePtr& bundleState,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto now = TInstant::Now();

    for (const auto& nodeName : aliveNodes) {
        if (bundleState->BundleNodeAssignments.count(nodeName) != 0) {
            continue;
        }

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (bundleState->BundleNodeReleasements.count(nodeName) != 0) {
            YT_LOG_WARNING("Trying to cancel bundle node release "
            "(Bundle: %v, NodeName: %v, Decommissioned: %v, UserTags: %v)",
                bundleName,
                nodeName,
                nodeInfo->Decommissioned,
                nodeInfo->UserTags);

            bundleState->BundleNodeReleasements.erase(nodeName);
            mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(false);
            continue;
        }

        if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->BundleNodeAssignments[nodeName] = operation;

            YT_LOG_INFO("Creating node tag filter assignment for bundle node (BundleName: %v, TabletNode: %v, NodeUserTags: %v)",
                bundleName,
                nodeName,
                nodeInfo->UserTags);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateBundleNodesReleasement(
    const std::string& bundleName,
    const TSchedulerInputState& input,
    const THashSet<std::string>& nodesToRelease,
    const TBundleControllerStatePtr& bundleState)
{
    if (!bundleState->RemovingCells.empty()) {
        return;
    }

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto now = TInstant::Now();

    for (const auto& nodeName : nodesToRelease) {
        if (bundleState->BundleNodeAssignments.count(nodeName) != 0 ||
            bundleState->BundleNodeReleasements.count(nodeName) != 0) {
            continue;
        }

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->BundleNodeReleasements[nodeName] = operation;

            YT_LOG_INFO("Creating node tag filter releasement for bundle node "
                "(Bundle: %v, TabletNode: %v, NodeUserTags: %v)",
                bundleName,
                nodeName,
                nodeInfo->UserTags);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

bool AllTabletSlotsAreEmpty(const TTabletNodeInfoPtr& nodeInfo)
{
    const auto& slots = nodeInfo->TabletSlots;

    return std::all_of(slots.begin(), slots.end(), [] (const TTabletSlotPtr& slot) {
        return slot->State == TabletSlotStateEmpty;
    });
}

////////////////////////////////////////////////////////////////////////////////

void ProcessNodesAssignments(
    const std::string& bundleName,
    const TSchedulerInputState& input,
    TIndexedEntries<TNodeTagFilterOperationState>* nodeAssignments,
    TSchedulerMutations* mutations)
{
    std::vector<std::string> finished;
    auto now = TInstant::Now();

    for (const auto& [nodeName,  operation] : *nodeAssignments) {
        if (now - operation->CreationTime > input.Config->NodeAssignmentTimeout) {
            YT_LOG_WARNING("Assigning node is stuck (Bundle: %v, TabletNode: %v)",
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "node_assignment_is_stuck",
                .BundleName = bundleName,
                .Description = Format("Assigning node %v for bundle %v is taking more time than expected",
                    bundleName,
                    nodeName),
            });
        }

        if (ProcessNodeAssignment(nodeName, bundleName, input, mutations)) {
            YT_LOG_INFO("Assigning tablet node is finished (Bundle: %v, TabletNode: %v)",
                bundleName,
                nodeName);

            finished.push_back(nodeName);
        }
    }

    for (const auto& node : finished) {
        nodeAssignments->erase(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProcessNodesReleasements(
    const std::string& bundleName,
    const TSchedulerInputState& input,
    bool leaveDecommissioned,
    TIndexedEntries<TNodeTagFilterOperationState>* nodeAssignments,
    TSchedulerMutations* mutations)
{
    const auto& nodeTagFilter = GetOrCrash(input.Bundles, bundleName)->NodeTagFilter;
    std::vector<std::string> finished;
    auto now = TInstant::Now();

    for (const auto& [nodeName,  operation] : *nodeAssignments) {
        if (now - operation->CreationTime > input.Config->NodeAssignmentTimeout) {
            YT_LOG_WARNING("Releasing node is stuck (Bundle: %v, Node: %v)",
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "node_releasment_is_stuck",
                .BundleName = bundleName,
                .Description = Format("Releasing node %v for bundle %v is taking more time than expected",
                    bundleName,
                    nodeName),
            });
        }

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
            if (input.Config->DecommissionReleasedNodes) {
                if (!nodeInfo->Decommissioned) {
                    mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(true);
                    YT_LOG_DEBUG("Releasing node: setting decommissioned flag (Bundle: %v, NodeName: %v)",
                        bundleName,
                        nodeName);
                    continue;
                }

                if (!AllTabletSlotsAreEmpty(nodeInfo)) {
                    YT_LOG_DEBUG("Releasing node: not all tablet cells are empty (Bundle: %v, NodeName: %v)",
                        bundleName,
                        nodeName);
                    continue;
                }
            } else {
                YT_LOG_DEBUG("Releasing node: will not decommission (Bundle: %v, NodeName: %v)",
                    bundleName,
                    nodeName);
            }

            YT_LOG_INFO("Releasing node: Removing node tag filter (Bundle: %v, NodeName: %v)",
                bundleName,
                nodeName);

            auto userTags = nodeInfo->UserTags;
            userTags.erase(nodeTagFilter);
            mutations->ChangedNodeUserTags[nodeName] = mutations->WrapMutation(userTags);
            continue;
        }

        if (nodeInfo->Decommissioned != leaveDecommissioned) {
            YT_LOG_DEBUG("Releasing node: setting target decommissioned state (Bundle: %v, NodeName: %v, Decommissioned: %v)",
                bundleName,
                nodeName,
                leaveDecommissioned);
            mutations->ChangedDecommissionedFlag[nodeName] = mutations->WrapMutation(leaveDecommissioned);
            continue;
        }

        YT_LOG_INFO("Cleaned up released node (Bundle: %v, NodeName: %v)",
            bundleName,
            nodeName);
        finished.push_back(nodeName);
    }

    for (const auto& node : finished) {
        nodeAssignments->erase(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterOrder
{
    // Data center does not have enough alive bundle nodes (even with spare ones).
    bool Unfeasible = false;

    // Data center is forbidden by admin.
    bool Forbidden = false;

    // TODO(capone212): User preferences goes here.

    int AssignedTabletCellCount = 0;

    // How many nodes we have to assign to bundle, i.e. how many nodes do not have needed node tag filter.
    int RequiredNodeAssignmentCount = 0;

    // Just dc name alphabetical order for predictability.
    std::string DataCenter;

    auto MakeTuple() const
    {
        return std::tie(Unfeasible, Forbidden, AssignedTabletCellCount, RequiredNodeAssignmentCount, DataCenter);
    }

    bool operator<(const TDataCenterOrder& other) const
    {
        return MakeTuple() < other.MakeTuple();
    }
};

int GetAvailableLiveTabletNodeCount(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        result += std::ssize(it->second);
    }

    if (auto it = spareNodesInfo.find(dataCenterName); it != spareNodesInfo.end()) {
        const auto& dataCenterSpare = it->second;
        result += std::ssize(dataCenterSpare.FreeNodes);

        const auto& usedByBundle = dataCenterSpare.UsedByBundle;
        auto bundleIt = usedByBundle.find(bundleName);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

int GetAssignedTabletNodeCount(
    const std::string& bundleName,
    const std::string& nodeTagFilter,
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo,
    const TSchedulerInputState& input)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        for (const auto& nodeName : it->second) {
            auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
                ++result;
            }
        }
    }

    if (auto it = spareNodesInfo.find(dataCenterName); it != spareNodesInfo.end()) {
        const auto& usedByBundle = it->second.UsedByBundle;

        auto bundleIt = usedByBundle.find(bundleName);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

int GetAssignedTabletCellCount(
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes,
    const TSchedulerInputState& input)
{
    int result = 0;

    if (auto it = aliveBundleNodes.find(dataCenterName); it != aliveBundleNodes.end()) {
        for (const auto& nodeName : it->second) {
            auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            for (const auto& slot : nodeInfo->TabletSlots) {
                if (slot->State == PeerStateLeading) {
                    ++result;
                }
            }
        }
    }

    return result;
}

THashSet<std::string> GetDataCentersToPopulate(
    const std::string& bundleName,
    const std::string& nodeTagFilter,
    const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveNodes,
    const TPerDataCenterSpareNodesInfo& spareNodesInfo,
    const TSchedulerInputState& input)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);
    const auto perNodeSlotCount = targetConfig->CpuLimits->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);

    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - zoneInfo->RedundantDataCenterCount;
    YT_VERIFY(activeDataCenterCount > 0);

    int perDataCenterSlotCount = GetCeiledShare(std::ssize(bundleInfo->TabletCellIds) *  bundleInfo->Options->PeerCount, activeDataCenterCount);
    int requiredPerDataCenterNodeCount = GetCeiledShare(perDataCenterSlotCount, perNodeSlotCount);

    std::vector<TDataCenterOrder> dataCentersOrder;
    dataCentersOrder.reserve(std::ssize(zoneInfo->DataCenters));

    for (const auto& [dataCenter, dataCenterInfo] : zoneInfo->DataCenters) {
        int availableNodeCount = GetAvailableLiveTabletNodeCount(
            bundleName,
            dataCenter,
            perDataCenterAliveNodes,
            spareNodesInfo);

        int assignedNodeCount = GetAssignedTabletNodeCount(
            bundleName,
            nodeTagFilter,
            dataCenter,
            perDataCenterAliveNodes,
            spareNodesInfo,
            input);

        int assignedTabletCellCount = GetAssignedTabletCellCount(dataCenter, perDataCenterAliveNodes, input);
        bool perBundleForbidden = targetConfig->ForbiddenDataCenters.count(dataCenter) != 0;

        dataCentersOrder.push_back({
            .Unfeasible = availableNodeCount < requiredPerDataCenterNodeCount,
            .Forbidden = dataCenterInfo->Forbidden || perBundleForbidden,
            .AssignedTabletCellCount = -1 * assignedTabletCellCount,
            .RequiredNodeAssignmentCount = requiredPerDataCenterNodeCount - assignedNodeCount,
            .DataCenter = dataCenter,
        });

        const auto& status = dataCentersOrder.back();

        YT_LOG_DEBUG(
            "Bundle data center status "
            "(Bundle: %v, DataCenter: %v, Unfeasible: %v, Forbidden: %v, AssignedTabletCellCount: %v,"
            " PerDataCenterSlotCount: %v, RequiredPerDataCenterNodeCount: %v"
            " RequiredNodeAssignmentCount: %v, AvailableNodeCount: %v, RequiredNodeCount: %v)",
            bundleName,
            dataCenter,
            status.Unfeasible,
            status.Forbidden,
            status.AssignedTabletCellCount,
            perDataCenterSlotCount,
            requiredPerDataCenterNodeCount,
            status.RequiredNodeAssignmentCount,
            availableNodeCount,
            requiredPerDataCenterNodeCount);
    }

    std::sort(dataCentersOrder.begin(), dataCentersOrder.end());
    dataCentersOrder.resize(activeDataCenterCount);

    THashSet<std::string> result;
    for (const auto& item : dataCentersOrder) {
        result.insert(item.DataCenter);
    }

    YT_LOG_DEBUG("Bundle data center preference (Bundle: %v, DataCenters: %v)",
        bundleName,
        result);

    return result;
}

int GetRequiredSlotCount(const TBundleInfoPtr& bundleInfo, const TSchedulerInputState& input)
{
    int requiredSlotCount = 0;
    for (const auto& cellId : bundleInfo->TabletCellIds) {
        auto it = input.TabletCells.find(cellId);
        if (it != input.TabletCells.end()) {
            for (const auto& peer : it->second->Peers) {
                auto nodeIt = input.TabletNodes.find(peer->Address);
                if (nodeIt == input.TabletNodes.end() || !nodeIt->second->Decommissioned) {
                    ++requiredSlotCount;
                }
            }
        }
    }
    return requiredSlotCount;
}

////////////////////////////////////////////////////////////////////////////////

void SetNodeTagFilter(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneName = bundleInfo->Zone;
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& bundleState = mutations->ChangedStates[bundleName];

    auto& perDataCenterSpareNodes = spareNodesAllocator.SpareInstances[zoneName];

    auto perDataCenterAliveNodes = GetAliveNodes(
        bundleName,
        bundleNodes,
        input,
        bundleState,
        EGracePeriodBehaviour::Immediately);

    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

    if (targetConfig->EnableDrillsMode || GetDrillsNodeTagFilter(bundleInfo, bundleName) == nodeTagFilter) {
        YT_LOG_WARNING(
            "Bundle has drills mode enabled. To disable drills mode set bundle attribute @bundle_controller_target_config/enable_drills_mode=%%false (Bundle: %v)",
            bundleName);

        mutations->AlertsToFire.push_back({
            .Id = "bundle_has_drills_mode_enabled",
            .BundleName = bundleName,
            .Description = Format("Bundle %Qv has drills mode enabled",
                bundleName),
        });
        return;
    }

    if (nodeTagFilter.empty()) {
        YT_LOG_WARNING("Bundle does not have node_tag_filter attribute (Bundle: %v)",
            bundleName);

        mutations->AlertsToFire.push_back({
            .Id = "bundle_with_no_tag_filter",
            .BundleName = bundleName,
            .Description = Format("Bundle %Qv does not have node_tag_filter attribute set",
                bundleName),
        });
        return;
    }

    auto dataCentersToPopulate = GetDataCentersToPopulate(
        bundleName,
        nodeTagFilter,
        perDataCenterAliveNodes,
        perDataCenterSpareNodes,
        input);

    if (!targetConfig->ForbiddenDataCenters.empty()) {
        mutations->AlertsToFire.push_back({
            .Id = "bundle_has_forbidden_dc",
            .BundleName = bundleName,
            .Description = Format("Data centers %v are forbidden for bundle %Qv",
                targetConfig->ForbiddenDataCenters,
                bundleName)});
    }

    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveNodes = perDataCenterAliveNodes[dataCenterName];

        if (dataCentersToPopulate.count(dataCenterName) != 0) {
            TryCreateBundleNodesAssignment(bundleName, input, aliveNodes, bundleState, mutations);
        } else {
            TryCreateBundleNodesReleasement(bundleName, input, aliveNodes, bundleState);
        }
    }

    ProcessNodesAssignments(bundleName, input, &bundleState->BundleNodeAssignments, mutations);
    ProcessNodesReleasements(
        bundleName,
        input,
        DoNotLeaveNodesDecommissioned,
        &bundleState->BundleNodeReleasements,
        mutations);

    int requiredSlotCount = GetRequiredSlotCount(bundleInfo, input);

    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveNodes = perDataCenterAliveNodes[dataCenterName];
        int perNodeSlotCount = targetConfig->CpuLimits->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize);
        auto& spareNodes = perDataCenterSpareNodes[dataCenterName];

        auto getSpareSlotCount = [perNodeSlotCount, bundleName] (const auto& sparesByBundle) ->int {
            auto it = sparesByBundle.find(bundleName);
            if (it != sparesByBundle.end()) {
                return perNodeSlotCount * std::ssize(it->second);
            }
            return 0;
        };

        int requiredDataCenterSlotCount = GetCeiledShare(requiredSlotCount, std::ssize(dataCentersToPopulate));
        if (dataCentersToPopulate.count(dataCenterName) == 0) {
            requiredDataCenterSlotCount = 0;
        }

        int readyBundleNodeCount = GetReadyNodeCount(bundleInfo, aliveNodes, input);
        int actualSlotCount = perNodeSlotCount * readyBundleNodeCount;
        int usedSpareSlotCount = getSpareSlotCount(spareNodes.UsedByBundle);

        int releasingSlotCount = usedSpareSlotCount + actualSlotCount - requiredDataCenterSlotCount;
        int assigningSlotCount = requiredDataCenterSlotCount - usedSpareSlotCount - std::ssize(aliveNodes) * perNodeSlotCount;

        YT_LOG_DEBUG("Checking tablet cell slots for bundle "
            "(Bundle: %v, DataCenter: %v, ReleasingSlotCount: %v, AssigningSlotCount: %v, "
            "SpareSlotCount: %v, BundleSlotCount: %v, RequiredDataCenterSlotCount: %v)",
            bundleName,
            dataCenterName,
            releasingSlotCount,
            assigningSlotCount,
            usedSpareSlotCount,
            actualSlotCount,
            requiredDataCenterSlotCount);

        if (releasingSlotCount > 0) {
            TryCreateSpareNodesReleasements(bundleName, input, releasingSlotCount, &spareNodes, bundleState);
        } else if (assigningSlotCount > 0) {
            TryCreateSpareNodesAssignment(bundleName, zoneName, dataCenterName, input, assigningSlotCount, spareNodesAllocator, bundleState);
        }
    }

    ProcessNodesAssignments(bundleName, input, &bundleState->SpareNodeAssignments, mutations);
    ProcessNodesReleasements(
        bundleName,
        input,
        LeaveNodesDecommissioned && input.Config->DecommissionReleasedNodes,
        &bundleState->SpareNodeReleasements,
        mutations);
}

////////////////////////////////////////////////////////////////////////////////

void InitializeZoneToSpareNodes(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareNodes[zoneName] = GetSpareNodesInfo(zoneName, input, mutations);

        for (const auto& [dataCenterName, spareInfo] : input.ZoneToSpareNodes[zoneName]) {
            auto inUseSpareNodes = std::ssize(spareInfo.UsedByBundle) + std::ssize(spareInfo.ReleasingByBundle);
            if (std::ssize(spareInfo.FreeNodes) == 0 && inUseSpareNodes > 0) {
                YT_LOG_WARNING("No free spare nodes available (Zone: %v, DataCenter: %v)",
                    zoneName,
                    dataCenterName);

                mutations->AlertsToFire.push_back({
                    .Id = "no_free_spare_nodes",
                    .Description = Format("No free spare node available in zone: %v in data center: %v.",
                        zoneName,
                        dataCenterName),
                });
            }

            if (std::ssize(spareInfo.ExternallyDecommissioned) > 0) {
                mutations->AlertsToFire.push_back({
                    .Id = "externally_decommissioned_spare_nodes",
                    .Description = Format("Externally decommissioned spare nodes in zone: %v in data center: %v.",
                        zoneName,
                        dataCenterName),
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageNodeTagFilters(
    TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator,
    TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        if (auto zoneIt = input.Zones.find(bundleInfo->Zone); zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        SetNodeTagFilter(bundleName, bundleNodes, input, spareNodesAllocator, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
