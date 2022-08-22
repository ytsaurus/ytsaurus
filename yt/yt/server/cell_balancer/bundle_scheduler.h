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
    TIndexedEntries<TRpcProxyInfo> RpcProxies;

    TIndexedEntries<TAllocationRequest> AllocationRequests;
    TIndexedEntries<TDeallocationRequest> DeallocationRequests;

    using TBundleToInstanceMapping = THashMap<TString, std::vector<TString>>;
    TBundleToInstanceMapping BundleNodes;
    TBundleToInstanceMapping BundleProxies;

    THashMap<TString, TString> PodIdToInstanceName;

    using TZoneToInstanceMap = THashMap<TString, std::vector<TString>>;
    TZoneToInstanceMap ZoneNodes;
    TZoneToInstanceMap ZoneProxies;

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
    TIndexedEntries<TInstanceAnnotations> ChangeNodeAnnotations;
    TIndexedEntries<TInstanceAnnotations> ChangedProxyAnnotations;

    using TUserTags = THashSet<TString>;
    THashMap<TString, TUserTags> ChangedNodeUserTags;

    THashMap<TString, bool> ChangedDecommissionedFlag;

    THashMap<TString, TString> ChangedProxyRole;

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

void ManageRpcProxyRoles(TSchedulerInputState& input, TSchedulerMutations* mutations);

THashSet<TString> GetAliveNodes(
    const TString& bundleName,
    const std::vector<TString>& bundleNodes,
    const TSchedulerInputState& input);

THashSet<TString> GetAliveProxies(
    const std::vector<TString>& bundleProxies,
    const TSchedulerInputState& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
