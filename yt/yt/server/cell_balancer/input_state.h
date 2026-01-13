#pragma once

#include "public.h"
#include "private.h"
#include "spare_instances.h"

#include <yt/yt/client/bundle_controller_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TZoneToInstanceInfo
{
    TDataCenterToInstanceMap PerDataCenter;
};

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

void InitializeRelations(TSchedulerInputState* input, TOnAlertCallback onAlert);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
