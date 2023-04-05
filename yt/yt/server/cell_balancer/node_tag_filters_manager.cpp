#include "bundle_scheduler.h"
#include "config.h"

namespace NYT::NCellBalancer {

///////////////////////////////////////////////////////////////

static const auto& Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetBundlesByTag(
    const TTabletNodeInfoPtr& nodeInfo,
    const THashMap<TString, TString>& filterTagToBundleName)
{
    std::vector<TString> result;

    for (const auto& tag : nodeInfo->UserTags) {
        if (auto it = filterTagToBundleName.find(tag); it != filterTagToBundleName.end()) {
            result.push_back(GetOrCrash(filterTagToBundleName, tag));
        }
    }

    return result;
}

int GetReadyNodesCount(
    const TBundleInfoPtr& bundleInfo,
    const THashSet<TString>& aliveBundleNodes,
    const TSchedulerInputState& input)
{
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;

    int readyNodesCount = 0;

    for (const auto& nodeName : aliveBundleNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
            ++readyNodesCount;
        }
    }

    return readyNodesCount;
}

////////////////////////////////////////////////////////////////////////////////

TSpareNodesInfo GetSpareNodesInfo(
    const TString& zoneName,
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

    auto aliveNodes = GetAliveNodes(spareBundle, spareNodesIt->second, input, EGracePeriodBehaviour::Immediately);

    // NodeTagFilter To Bundle Name.
    THashMap<TString, TString> filterTagToBundleName;
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        filterTagToBundleName[bundleInfo->NodeTagFilter] = bundleName;
    }

    THashMap<TString, TString> operationsToBundle;
    for (const auto& [bundleName, bundleState] : input.BundleStates) {
        for (const auto& [spareNode, _] : bundleState->SpareNodeAssignments) {
            operationsToBundle[spareNode] = bundleName;
        }

        for (const auto& [spareNode, _] : bundleState->SpareNodeReleasements) {
            operationsToBundle[spareNode] = bundleName;
        }
    }

    TSpareNodesInfo result;

    for (const auto& spareNodeName : aliveNodes) {
        auto nodeInfo = GetOrCrash(input.TabletNodes, spareNodeName);

        auto assignedBundlesNames = GetBundlesByTag(nodeInfo, filterTagToBundleName);
        if (assignedBundlesNames.empty() && operationsToBundle.count(spareNodeName) == 0) {
            result.FreeNodes.push_back(spareNodeName);
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

        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        if (!bundleInfo->EnableBundleController) {
            YT_LOG_WARNING("Spare node is occupied by unmanaged bundle (Node: %v, Bundles: %v)",
                spareNodeName,
                bundleName);

            result.UsedByBundle[bundleName].push_back(spareNodeName);
            continue;
        }

        const auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);

        if (bundleState->SpareNodeReleasements.count(spareNodeName)) {
            result.ReleasingByBundle[bundleName].push_back(spareNodeName);
        } else {
            result.UsedByBundle[bundleName].push_back(spareNodeName);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Returns true if the node finally/already assigned to the bundle.
bool ProcessNodeAssignment(
    const TString& nodeName,
    const TString& bundleName,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

    if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
        auto tags = nodeInfo->UserTags;
        tags.insert(nodeTagFilter);
        mutations->ChangedNodeUserTags[nodeName] = std::move(tags);
        mutations->ChangedDecommissionedFlag[nodeName] = true;

        YT_LOG_INFO("Setting node tag filter "
            "(Bundle: %v, TabletNode: %v, NodeTagFilter: %v)",
            bundleName,
            nodeName,
            nodeTagFilter);

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
        mutations->ChangedDecommissionedFlag[nodeName] = false;
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateSpareNodesReleasements(
    const TString& bundleName,
    const TSchedulerInputState& input,
    int slotsToRelease,
    TSpareNodesInfo* spareNodesInfo,
    const TBundleControllerStatePtr& bundleState)
{
    if (!bundleState->SpareNodeAssignments.empty()) {
        // Let's not mix spare node assigning with releasing.
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
    const TString& bundleName,
    const TSchedulerInputState& input,
    int slotsToAdd,
    TSpareNodesInfo* spareNodesInfo,
    const TBundleControllerStatePtr& bundleState)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto& spareNodes = spareNodesInfo->FreeNodes;
    int perNodeSlotCount = bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize;
    auto now = TInstant::Now();

    while (slotsToAdd > 0 && !spareNodes.empty()) {
        const auto& nodeName = spareNodes.back();
        auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        YT_LOG_INFO("Assigning spare node (Bundle: %v, NodeName: %v)",
            bundleName,
            nodeName);

        auto operation = New<TNodeTagFilterOperationState>();
        operation->CreationTime = now;
        bundleState->SpareNodeAssignments[nodeName] = operation;

        slotsToAdd -= perNodeSlotCount;
        spareNodes.pop_back();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryCreateBundleNodesAssignment(
    const TString& bundleName,
    const TSchedulerInputState& input,
    THashSet<TString>& aliveNodes,
    const TBundleControllerStatePtr& bundleState)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto now = TInstant::Now();

    for (const auto& nodeName : aliveNodes) {
        if (bundleState->BundleNodeAssignments.count(nodeName) != 0) {
            continue;
        }

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
            auto operation = New<TNodeTagFilterOperationState>();
            operation->CreationTime = now;
            bundleState->BundleNodeAssignments[nodeName] = operation;

            YT_LOG_INFO("Creating node tag filter assignment for bundle node (Bundle: %v, TabletNode: %v, NodeUserTags: %v)",
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
    const TString& bundleName,
    const TSchedulerInputState& input,
    TIndexedEntries<TNodeTagFilterOperationState>* nodeAssignments,
    TSchedulerMutations* mutations)
{
    std::vector<TString> finished;
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

    for (auto node : finished) {
        nodeAssignments->erase(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProcessSpareNodesReleasements(
    const TString& bundleName,
    const TSchedulerInputState& input,
    const TBundleControllerStatePtr& bundleState,
    TSchedulerMutations* mutations)
{
    const auto& nodeTagFilter = GetOrCrash(input.Bundles, bundleName)->NodeTagFilter;
    std::vector<TString> finished;
    auto now = TInstant::Now();

    for (const auto& [nodeName,  operation] : bundleState->SpareNodeReleasements) {
        if (now - operation->CreationTime > input.Config->NodeAssignmentTimeout) {
            YT_LOG_WARNING("Releasing spare node is stuck (Bundle: %v, SpareNode: %v)",
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "spare_node_releasment_is_stuck",
                .BundleName = bundleName,
                .Description = Format("Releasing spare node %v for bundle %v is taking more time than expected",
                    bundleName,
                    nodeName),
            });
        }

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (!nodeInfo->Decommissioned) {
            mutations->ChangedDecommissionedFlag[nodeName] = true;
            continue;
        }

        if (!AllTabletSlotsAreEmpty(nodeInfo)) {
            continue;
        }

        if (nodeInfo->UserTags.count(nodeTagFilter) != 0) {
            YT_LOG_INFO("Removing node tag filter for released spare node (Bundle: %v, NodeName: %v)",
                bundleName,
                nodeName);

            auto userTags = nodeInfo->UserTags;
            userTags.erase(nodeTagFilter);
            mutations->ChangedNodeUserTags[nodeName] = userTags;
            continue;
        }

        YT_LOG_INFO("Cleaned up released spare node (Bundle: %v, NodeName: %v)",
            bundleName,
            nodeName);
        finished.push_back(nodeName);
    }

    for (auto node : finished) {
        bundleState->SpareNodeReleasements.erase(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

void SetNodeTagFilter(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input,
    TSpareNodesInfo& spareNodesInfo,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto aliveNodes = GetAliveNodes(bundleName, bundleNodes, input, EGracePeriodBehaviour::Immediately);
    const TString& nodeTagFilter = bundleInfo->NodeTagFilter;

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

    const auto& bundleState = mutations->ChangedStates[bundleName];
    TryCreateBundleNodesAssignment(bundleName, input, aliveNodes, bundleState);
    ProcessNodesAssignments(bundleName, input, &bundleState->BundleNodeAssignments, mutations);

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

    int perNodeSlotCount = bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize;

    auto getSpareSlotCount = [perNodeSlotCount, bundleName] (auto& sparesByBundle) ->int {
        auto it = sparesByBundle.find(bundleName);
        if (it != sparesByBundle.end()) {
            return perNodeSlotCount * std::ssize(it->second);
        }
        return 0;
    };

    int readyBundleNodes = GetReadyNodesCount(bundleInfo, aliveNodes, input);
    int actualSlotCount = perNodeSlotCount * readyBundleNodes;
    int usedSpareSlotCount = getSpareSlotCount(spareNodesInfo.UsedByBundle);

    int slotsBallance = usedSpareSlotCount  + actualSlotCount - requiredSlotCount;

    YT_LOG_DEBUG("Checking tablet cell slots for bundle "
        "(Bundle: %v, SlotsBallance: %v, SpareSlotCount: %v, BundleSlotCount: %v, RequiredSlotCount: %v)",
        bundleName,
        slotsBallance,
        usedSpareSlotCount,
        actualSlotCount,
        requiredSlotCount);

    if (slotsBallance > 0) {
        TryCreateSpareNodesReleasements(bundleName, input, slotsBallance, &spareNodesInfo, bundleState);
    } else {
        TryCreateSpareNodesAssignment(bundleName, input, std::abs(slotsBallance), &spareNodesInfo, bundleState);
    }

    ProcessNodesAssignments(bundleName, input, &bundleState->SpareNodeAssignments, mutations);
    ProcessSpareNodesReleasements(bundleName, input, bundleState, mutations);
}

////////////////////////////////////////////////////////////////////////////////

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareNodes[zoneName] = GetSpareNodesInfo(zoneName, input, mutations);

        const auto& spareInfo = input.ZoneToSpareNodes[zoneName];

        auto inUseSpareNodes = std::ssize(spareInfo.UsedByBundle) + std::ssize(spareInfo.ReleasingByBundle);
        if (std::ssize(spareInfo.FreeNodes) == 0 && inUseSpareNodes > 0) {
            YT_LOG_WARNING("No free spare nodes available (Zone: %v)",
                zoneName);

            mutations->AlertsToFire.push_back({
                .Id = "no_free_spare_nodes",
                .Description = Format("No free spare node available in zone: %v.",
                    zoneName),
            });
        }
    }

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        auto& spareNodes = input.ZoneToSpareNodes[bundleInfo->Zone];
        const auto& bundleNodes = input.BundleNodes[bundleName];
        SetNodeTagFilter(bundleName, bundleNodes, input, spareNodes, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
