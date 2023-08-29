#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

using TBundlesDynamicConfig = THashMap<TString, TBundleDynamicConfigPtr>;

////////////////////////////////////////////////////////////////////////////////

struct TSpareNodesInfo
{
    std::vector<TString> FreeNodes;
    std::vector<TString> ExternallyDecommissioned;
    THashMap<TString, std::vector<TString>> UsedByBundle;
    THashMap<TString, std::vector<TString>> ReleasingByBundle;
};

using TPerDataCenterSpareNodesInfo = THashMap<TString, TSpareNodesInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TSpareProxiesInfo
{
    std::vector<TString> FreeProxies;
    THashMap<TString, std::vector<TString>> UsedByBundle;
};

////////////////////////////////////////////////////////////////////////////////

struct TInstanceRackInfo
{
    THashMap<TString, int> RackToBundleInstances;
    THashMap<TString, int> RackToSpareInstances;

    // Spare instances needed-for-minus one rack guarantee.
    int RequiredSpareNodeCount = 0;
};

using TDataCenterRackInfo = THashMap<TString, TInstanceRackInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterDisruptedState
{
    int OfflineNodeCount = 0;
    int OfflineNodeThreshold = 0;

    int OfflineProxyCount = 0;
    int OfflineProxyThreshold = 0;

    bool IsNodesDisrupted() const
    {
        return OfflineNodeThreshold > 0 && OfflineNodeCount > OfflineNodeThreshold;
    }

    bool IsProxiesDisrupted() const
    {
        return OfflineProxyThreshold > 0 && OfflineProxyCount > OfflineProxyThreshold;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TDataCenterToInstanceMap = THashMap<TString, std::vector<TString>>;

struct TZoneToInstanceInfo
{
    TDataCenterToInstanceMap PerDataCenter;
};

////////////////////////////////////////////////////////////////////////////////

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

    TIndexedEntries<TSystemAccount> SystemAccounts;
    TSystemAccountPtr RootSystemAccount;

    using TBundleToInstanceMapping = THashMap<TString, TDataCenterToInstanceMap>;
    TBundleToInstanceMapping BundleNodes;
    TBundleToInstanceMapping BundleProxies;

    THashMap<TString, TString> PodIdToInstanceName;

    using TZoneToInstanceMap = THashMap<TString, TZoneToInstanceInfo>;
    TZoneToInstanceMap ZoneNodes;
    TZoneToInstanceMap ZoneProxies;

    THashMap<TString, TDataCenterRackInfo> ZoneToRacks;

    TBundlesDynamicConfig DynamicConfig;

    THashMap<TString, TPerDataCenterSpareNodesInfo> ZoneToSpareNodes;

    THashMap<TString, TSpareProxiesInfo> ZoneToSpareProxies;

    THashMap<TString, TInstanceResourcesPtr> BundleResourceAlive;
    THashMap<TString, TInstanceResourcesPtr> BundleResourceAllocated;
    THashMap<TString, TInstanceResourcesPtr> BundleResourceTarget;

    using TInstanceCountBySize = THashMap<TString, int>;
    THashMap<TString, TInstanceCountBySize> AllocatedNodesBySize;
    THashMap<TString, TInstanceCountBySize> AliveNodesBySize;
    THashMap<TString, TInstanceCountBySize> AllocatedProxiesBySize;
    THashMap<TString, TInstanceCountBySize> AliveProxiesBySize;

    using TQualifiedDCName = std::pair<TString, TString>;
    THashMap<TQualifiedDCName, TDataCenterDisruptedState> DatacenterDisrupted;

    THashMap<TString, TString> BundleToShortName;
};

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    TString Id;
    std::optional<TString> BundleName;
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
    THashMap<TString, bool> ChangedEnableBundleBalancerFlag;

    THashMap<TString, TString> ChangedProxyRole;

    std::vector<TString> CellsToRemove;

    // Maps bundle name to new tablet cells count to create.
    THashMap<TString, int> CellsToCreate;

    std::vector<TAlert> AlertsToFire;

    THashMap<TString, TAccountResourcesPtr> LiftedSystemAccountLimit;
    THashMap<TString, TAccountResourcesPtr> LoweredSystemAccountLimit;
    TAccountResourcesPtr ChangedRootSystemAccountLimit;

    std::optional<TBundlesDynamicConfig> DynamicConfig;

    THashSet<TString> NodesToCleanup;
    THashSet<TString> ProxiesToCleanup;

    THashMap<TString, i64> ChangedTabletStaticMemory;
    THashMap<TString, TString> ChangedBundleShortName;

    THashMap<TString, TString> InitializedNodeTagFilters;
    THashMap<TString, TBundleConfigPtr> InitializedBundleTargetConfig;
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

TString GetSpareBundleName(const TZoneInfoPtr& zoneInfo);

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations);

void ManageRpcProxyRoles(TSchedulerInputState& input, TSchedulerMutations* mutations);

DEFINE_ENUM(EGracePeriodBehaviour,
    ((Wait)         (0))
    ((Immediately)  (1))
);

THashMap<TString, THashSet<TString>> GetAliveNodes(
    const TString& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    const TBundleControllerStatePtr& bundleState,
    EGracePeriodBehaviour gracePeriodBehaviour);

THashMap<TString, THashSet<TString>> GetAliveProxies(
    const TDataCenterToInstanceMap& bundleProxies,
    const TSchedulerInputState& input,
    EGracePeriodBehaviour gracePeriodBehaviour);

TString GetInstancePodIdTemplate(
    const TString& cluster,
    const TString& bundleName,
    const TString& instanceType,
    int index);

int FindNextInstanceId(
    const std::vector<TString>& instanceNames,
    const TString& cluster,
    const TString& instanceType);

TIndexedEntries<TBundleControllerState> MergeBundleStates(
    const TSchedulerInputState& schedulerState,
    const TSchedulerMutations& mutations);

TString GetPodIdForInstance(const TString& name);

TString GetInstanceSize(const TInstanceResourcesPtr& resource);

// TODO(capone212): remove after
THashSet<TString> FlattenAliveInstancies(const THashMap<TString, THashSet<TString>>& instancies);
std::vector<TString> FlattenBundleInstancies(const THashMap<TString,std::vector<TString>>& instancies);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
