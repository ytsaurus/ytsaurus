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

////////////////////////////////////////////////////////////////////////////////

TSpareNodesInfo GetSpareNodesInfo(
    const TString& zoneName,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto spareBundle = GetSpareBundleName(zoneName);
    auto spareNodesIt = input.BundleNodes.find(spareBundle);
    if (spareNodesIt == input.BundleNodes.end()) {
        return {};
    }

    const auto& spareNodes = spareNodesIt->second;
    auto aliveNodes = GetAliveNodes(spareBundle, spareNodes, input, EGracePeriodBehaviour::Immediately);

    // NodeTagFilter To Bundle Name.
    THashMap<TString, TString> filterTagToBundleName;
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        filterTagToBundleName[bundleInfo->NodeTagFilter] = bundleName;
    }

    TSpareNodesInfo result;

    for (const auto& spareNodeName : spareNodes) {
        auto nodeInfo = GetOrCrash(input.TabletNodes, spareNodeName);

        auto assignedBundlesNames = GetBundlesByTag(nodeInfo, filterTagToBundleName);
        if (assignedBundlesNames.empty()) {
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

        const auto& bundleName = assignedBundlesNames.front();

        if (nodeInfo->Decommissioned) {
            result.DecommissionedByBundle[bundleName].push_back(spareNodeName);
        } else {
            result.UsedByBundle[bundleName].push_back(spareNodeName);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TryReleaseSpareNodes(
    const TString& bundleName,
    const TSchedulerInputState& input,
    int slotsToRelease,
    TSpareNodesInfo& spareNodesInfo,
    TSchedulerMutations* mutations)
{
    auto usingSpareNodes = spareNodesInfo.UsedByBundle[bundleName];
    auto it = usingSpareNodes.begin();

    while (slotsToRelease > 0 && it != usingSpareNodes.end()) {
        const auto& nodeName = *it;
        auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        if (std::ssize(nodeInfo->TabletSlots) <= slotsToRelease) {
            YT_LOG_INFO("Releasing spare node (Bundle: %v, NodeName: %v)",
                bundleName,
                nodeName);

            mutations->ChangedDecommissionedFlag[nodeName] = true;
            slotsToRelease -= std::ssize(nodeInfo->TabletSlots);
        }
        ++it;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryAssignSpareNodes(
    const TString& bundleName,
    const TSchedulerInputState& input,
    int slotsToAdd,
    TSpareNodesInfo& spareNodesInfo,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& nodeTagFilter = bundleInfo->NodeTagFilter;
    auto spareNodes = spareNodesInfo.FreeNodes;

    while (slotsToAdd > 0 && !spareNodes.empty()) {
        const auto& nodeName = spareNodes.back();
        auto nodeInfo = GetOrCrash(input.TabletNodes, nodeName);

        YT_LOG_INFO("Assigning spare node (Bundle: %v, NodeName: %v)",
            bundleName,
            nodeName);

        mutations->ChangedDecommissionedFlag[nodeName] = false;
        auto userTags = nodeInfo->UserTags;
        userTags.insert(nodeTagFilter);
        mutations->ChangedNodeUserTags[nodeName] = userTags;

        slotsToAdd -= std::ssize(nodeInfo->TabletSlots);
        spareNodes.pop_back();
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

    int actualSlotCount = 0;
    for (const auto& nodeName : aliveNodes) {
        // Ensure bundle nodes has node tag filter set.
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->UserTags.count(nodeTagFilter) == 0) {
            auto tags = nodeInfo->UserTags;
            tags.insert(nodeTagFilter);
            mutations->ChangedNodeUserTags[nodeName] = std::move(tags);
        }

        actualSlotCount += std::ssize(nodeInfo->TabletSlots);
    }

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

    auto getSpareSlotCount = [&input, bundleName] (auto& sparesByBundle) {
        int usedSpareSlotCount = 0;
        auto it = sparesByBundle.find(bundleName);
        if (it != sparesByBundle.end()) {
            for (const auto& nodeName : it->second) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
                usedSpareSlotCount += std::ssize(nodeInfo->TabletSlots);
            }
        }
        return usedSpareSlotCount;
    };

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
        TryReleaseSpareNodes(bundleName, input, slotsBallance, spareNodesInfo, mutations);
    } else {
        TryAssignSpareNodes(bundleName, input, std::abs(slotsBallance), spareNodesInfo, mutations);
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

void CleanupSpareNodes(
    const TSchedulerInputState& input,
    const TSpareNodesInfo& spareNodes,
    TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, nodes] : spareNodes.DecommissionedByBundle) {
        const auto& nodeTagFilter = GetOrCrash(input.Bundles, bundleName)->NodeTagFilter;

        for (const auto& nodeName : nodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            if (!AllTabletSlotsAreEmpty(nodeInfo)) {
                continue;
            }

            YT_LOG_INFO("Cleaning up released spare node (Bundle: %v, NodeName: %v)",
                bundleName,
                nodeName);

            auto userTags = nodeInfo->UserTags;
            userTags.erase(nodeTagFilter);
            mutations->ChangedNodeUserTags[nodeName] = userTags;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareNodes[zoneName] = GetSpareNodesInfo(zoneName, input, mutations);

        const auto& spareInfo = input.ZoneToSpareNodes[zoneName];

        auto inUseSpareNodes = std::ssize(spareInfo.UsedByBundle) + std::ssize(spareInfo.DecommissionedByBundle);
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

    for (const auto& [_, spareNodes] : input.ZoneToSpareNodes) {
        CleanupSpareNodes(input, spareNodes, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
