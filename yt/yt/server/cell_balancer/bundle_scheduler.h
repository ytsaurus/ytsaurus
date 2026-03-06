#pragma once

#include "public.h"
#include "cypress_bindings.h"
#include "spare_instances.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

NBundleControllerClient::TCpuLimitsPtr GetBundleEffectiveCpuLimits(
    const std::string& bundleName,
    const TBundleInfoPtr& bundleInfo,
    const TSchedulerInputState& input);

std::string GetSpareBundleName(const TZoneInfoPtr& zoneInfo);

void InitializeZoneToSpareNodes(TSchedulerInputState& input, TSchedulerMutations* mutations);
void ManageNodeTagFilters(TSchedulerInputState& input, TSpareInstanceAllocator<TSpareNodesInfo>& spareNodesAllocator, TSchedulerMutations* mutations);

void InitializeZoneToSpareProxies(TSchedulerInputState& input, TSchedulerMutations* mutations);
void ManageRpcProxyRoles(TSchedulerInputState& input, TSpareInstanceAllocator<TSpareProxiesInfo>& spareProxiesAllocator, TSchedulerMutations* mutations);

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

std::string GetPodIdForInstance(const TCypressAnnotationsPtr& cypressAnnotations, const std::string& name);

std::string GetInstanceSize(const NBundleControllerClient::TInstanceResourcesPtr& resource);

// TODO(capone212): remove after
THashSet<std::string> FlattenAliveInstances(const THashMap<std::string, THashSet<std::string>>& instancies);
std::vector<std::string> FlattenBundleInstances(const THashMap<std::string, std::vector<std::string>>& instancies);

std::string GetDrillsNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName);
std::string GetReleasedProxyRole(const std::string& rpcProxyRole);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
