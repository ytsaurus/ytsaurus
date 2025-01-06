#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultWriteThreadPoolSize = 5;

////////////////////////////////////////////////////////////////////////////////

using TBundlesDynamicConfig = THashMap<std::string, TBundleDynamicConfigPtr>;

////////////////////////////////////////////////////////////////////////////////

struct TSpareNodesInfo
{
    std::vector<std::string> FreeNodes;
    std::vector<std::string> ExternallyDecommissioned;
    THashMap<std::string, std::vector<std::string>> UsedByBundle;
    THashMap<std::string, std::vector<std::string>> ReleasingByBundle;
};

using TPerDataCenterSpareNodesInfo = THashMap<std::string, TSpareNodesInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TSpareProxiesInfo
{
    std::vector<std::string> FreeProxies;
    THashMap<std::string, std::vector<std::string>> UsedByBundle;
};

using TPerDataCenterSpareProxiesInfo = THashMap<std::string, TSpareProxiesInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TInstanceRackInfo
{
    THashMap<std::string, int> RackToBundleInstances;
    THashMap<std::string, int> RackToSpareInstances;

    // Spare instances needed-for-minus one rack guarantee.
    int RequiredSpareNodeCount = 0;
};

using TDataCenterRackInfo = THashMap<std::string, TInstanceRackInfo>;

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

using TDataCenterToInstanceMap = THashMap<std::string, std::vector<std::string>>;

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

    using TBundleToInstanceMapping = THashMap<std::string, TDataCenterToInstanceMap>;
    TBundleToInstanceMapping BundleNodes;
    TBundleToInstanceMapping BundleProxies;

    THashMap<std::string, std::string> PodIdToInstanceName;

    using TZoneToInstanceMap = THashMap<std::string, TZoneToInstanceInfo>;
    TZoneToInstanceMap ZoneNodes;
    TZoneToInstanceMap ZoneProxies;

    THashMap<std::string, TDataCenterRackInfo> ZoneToRacks;

    TBundlesDynamicConfig DynamicConfig;

    THashMap<std::string, TPerDataCenterSpareNodesInfo> ZoneToSpareNodes;

    THashMap<std::string, TPerDataCenterSpareProxiesInfo> ZoneToSpareProxies;

    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> BundleResourceAlive;
    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> BundleResourceAllocated;
    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> BundleResourceTarget;

    using TInstanceCountBySize = THashMap<std::string, int>;
    THashMap<std::string, TInstanceCountBySize> AllocatedNodesBySize;
    THashMap<std::string, TInstanceCountBySize> AliveNodesBySize;
    THashMap<std::string, TInstanceCountBySize> AllocatedProxiesBySize;
    THashMap<std::string, TInstanceCountBySize> AliveProxiesBySize;

    using TQualifiedDCName = std::pair<std::string, std::string>;
    THashMap<TQualifiedDCName, TDataCenterDisruptedState> DatacenterDisrupted;

    THashMap<std::string, std::string> BundleToShortName;

    TSysConfigPtr SysConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerMutations
{
    TIndexedEntries<TAllocationRequest> NewAllocations;
    TIndexedEntries<TDeallocationRequest> NewDeallocations;
    TIndexedEntries<TBundleControllerState> ChangedStates;
    TIndexedEntries<TInstanceAnnotations> ChangeNodeAnnotations;
    TIndexedEntries<TInstanceAnnotations> ChangedProxyAnnotations;

    using TUserTags = THashSet<std::string>;
    THashMap<std::string, TUserTags> ChangedNodeUserTags;

    THashMap<std::string, bool> ChangedDecommissionedFlag;
    THashMap<std::string, bool> ChangedEnableBundleBalancerFlag;
    THashMap<std::string, bool> ChangedMuteTabletCellsCheck;
    THashMap<std::string, bool> ChangedMuteTabletCellSnapshotsCheck;

    THashMap<std::string, std::string> ChangedProxyRole;
    THashSet<std::string> RemovedProxyRole;

    std::vector<std::string> CellsToRemove;

    // Maps bundle name to new tablet cells count to create.
    THashMap<std::string, int> CellsToCreate;

    std::vector<TAlert> AlertsToFire;

    THashMap<std::string, TAccountResourcesPtr> LiftedSystemAccountLimit;
    THashMap<std::string, TAccountResourcesPtr> LoweredSystemAccountLimit;
    TAccountResourcesPtr ChangedRootSystemAccountLimit;

    std::optional<TBundlesDynamicConfig> DynamicConfig;

    THashSet<std::string> NodesToCleanup;
    THashSet<std::string> ProxiesToCleanup;

    THashMap<std::string, i64> ChangedTabletStaticMemory;
    THashMap<std::string, std::string> ChangedBundleShortName;

    THashMap<std::string, std::string> ChangedNodeTagFilters;
    THashMap<std::string, TBundleConfigPtr> InitializedBundleTargetConfig;
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

std::string GetSpareBundleName(const TZoneInfoPtr& zoneInfo);

void ManageNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations);

void ManageRpcProxyRoles(TSchedulerInputState& input, TSchedulerMutations* mutations);

DEFINE_ENUM(EGracePeriodBehaviour,
    ((Wait)         (0))
    ((Immediately)  (1))
);

THashMap<std::string, THashSet<std::string>> GetAliveNodes(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    const TBundleControllerStatePtr& bundleState,
    EGracePeriodBehaviour gracePeriodBehaviour);

THashMap<std::string, THashSet<std::string>> GetAliveProxies(
    const TDataCenterToInstanceMap& bundleProxies,
    const TSchedulerInputState& input,
    EGracePeriodBehaviour gracePeriodBehaviour);

std::string GetInstancePodIdTemplate(
    const std::string& cluster,
    const std::string& bundleName,
    const std::string& instanceType,
    int index);

int FindNextInstanceId(
    const std::vector<std::string>& instanceNames,
    const std::string& cluster,
    const std::string& instanceType);

TIndexedEntries<TBundleControllerState> MergeBundleStates(
    const TSchedulerInputState& schedulerState,
    const TSchedulerMutations& mutations);

std::string GetPodIdForInstance(const std::string& name);

std::string GetInstanceSize(const NBundleControllerClient::TInstanceResourcesPtr& resource);

// TODO(capone212): remove after
THashSet<std::string> FlattenAliveInstancies(const THashMap<std::string, THashSet<std::string>>& instancies);
std::vector<std::string> FlattenBundleInstancies(const THashMap<std::string,std::vector<std::string>>& instancies);

std::string GetDrillsNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName);
std::string GetReleasedProxyRole(const std::string& rpcProxyRole);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
