#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerInputState
{
    TBundleControllerConfigPtr Config;

    TIndexedEntries<TZoneInfo> Zones;
    TIndexedEntries<TBundleInfo> Bundles;
    TIndexedEntries<TBundleControllerState> States;
    TIndexedEntries<TTabletNodeInfo> TabletNodes;
    TIndexedEntries<TTabletCellInfo> TabletCells;

    TIndexedEntries<TAllocationRequest> AllocationRequests;
    TIndexedEntries<TDeallocationRequest> DeallocationRequests;

    using TBundleToNodeMapping = THashMap<TString, std::vector<TString>>;
    TBundleToNodeMapping BundleNodes;

    THashMap<TString, TString> PodIdToNodeName;

    using TZoneToNodesMap = THashMap<TString, std::vector<TString>>;
    TZoneToNodesMap ZoneNodes;
};

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    TString Id;
    TString Description;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerMutatingContext
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
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutatingContext* mutations);

////////////////////////////////////////////////////////////////////////////////

TString GetSpareBundleName(const TString& zoneName);

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutatingContext* mutations);

THashSet<TString> GetAliveNodes(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
