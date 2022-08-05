#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

using TBundlesDynamicConfig = THashMap<TString, TBundleDynamicConfigPtr>;

struct TSchedulerInputState
{
    TBundleControllerConfigPtr Config;

    TIndexedEntries<TZoneInfo> Zones;
    TIndexedEntries<TBundleInfo> Bundles;
    TIndexedEntries<TBundleControllerState> BundleStates;
    TIndexedEntries<TTabletNodeInfo> TabletNodes;
    TIndexedEntries<TTabletCellInfo> TabletCells;

    TIndexedEntries<TAllocationRequest> AllocationRequests;
    TIndexedEntries<TDeallocationRequest> DeallocationRequests;

    using TBundleToNodeMapping = THashMap<TString, std::vector<TString>>;
    TBundleToNodeMapping BundleNodes;

    THashMap<TString, TString> PodIdToNodeName;

    using TZoneToNodesMap = THashMap<TString, std::vector<TString>>;
    TZoneToNodesMap ZoneNodes;

    TBundlesDynamicConfig DynamicConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    TString Id;
    TString Description;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerMutations
{
    TIndexedEntries<TAllocationRequest> NewAllocations;
    TIndexedEntries<TDeallocationRequest> NewDeallocations;
    TIndexedEntries<TBundleControllerState> ChangedStates;
    TIndexedEntries<TTabletNodeAnnotationsInfo> ChangeNodeAnnotations;

    using TUserTags = THashSet<TString>;
    THashMap<TString, TUserTags> ChangedNodeUserTags;

    THashMap<TString, bool> ChangedDecommissionedFlag;

    std::vector<TString> CellsToRemove;

    // Maps bundle name to new tablet cells count to create.
    THashMap<TString, int> CellsToCreate;

    std::vector<TAlert> AlertsToFire;

    std::optional<TBundlesDynamicConfig> DynamicConfig;
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

TString GetSpareBundleName(const TString& zoneName);

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations);

THashSet<TString> GetAliveNodes(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
