#include "input_state.h"

#include "config.h"
#include "cypress_bindings.h"
#include "mutations.h"
#include "pod_id_helpers.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

void InitDefaultDataCenter(TSchedulerInputState* input)
{
    for (const auto& [zoneName, zoneInfo] : input->Zones) {
        if (!zoneInfo->DataCenters.empty()) {
            continue;
        }
        auto dataCenter = New<TDataCenterInfo>();
        dataCenter->YPCluster = zoneInfo->DefaultYPCluster;
        dataCenter->TabletNodeNannyService = zoneInfo->DefaultTabletNodeNannyService;
        dataCenter->RpcProxyNannyService = zoneInfo->DefaultRpcProxyNannyService;
        zoneInfo->DataCenters[DefaultDataCenterName] = std::move(dataCenter);
    }
}

void InitializeVirtualSpareBundle(TSchedulerInputState* input)
{
    for (const auto& [zoneName, zoneInfo] : input->Zones) {
        auto spareVirtualBundle = zoneInfo->SpareBundleName;
        auto bundleInfo = New<TBundleInfo>();
        bundleInfo->EnableInstanceAllocation = input->Config->HasInstanceAllocatorService;
        bundleInfo->TargetConfig = zoneInfo->SpareTargetConfig;
        bundleInfo->EnableBundleController = true;
        bundleInfo->EnableTabletCellManagement = false;
        bundleInfo->EnableNodeTagFilterManagement = false;
        bundleInfo->EnableTabletNodeDynamicConfig = false;
        bundleInfo->EnableRpcProxyManagement = false;
        bundleInfo->EnableSystemAccountManagement = false;
        bundleInfo->EnableResourceLimitsManagement = false;
        bundleInfo->Zone = zoneName;
        input->Bundles[spareVirtualBundle] = bundleInfo;
    }
}

template <class TCollection>
TSchedulerInputState::TZoneToInstanceMap MapZonesToInstances(
    const TSchedulerInputState& input,
    const TCollection& collection)
{
    THashMap<std::string, std::string> nannyServiceToZone;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        for (const auto& [dataCenterName, dataCenterInfo] : zoneInfo->DataCenters) {
            if (!dataCenterInfo->TabletNodeNannyService.empty()) {
                nannyServiceToZone[dataCenterInfo->TabletNodeNannyService] = zoneName;
            }

            if (!dataCenterInfo->RpcProxyNannyService.empty()) {
                nannyServiceToZone[dataCenterInfo->RpcProxyNannyService] = zoneName;
            }
        }
    }

    TSchedulerInputState::TZoneToInstanceMap result;
    for (const auto& [instanceName, instanceInfo] : collection) {
        if (!instanceInfo->BundleControllerAnnotations->Allocated) {
            continue;
        }
        auto it = nannyServiceToZone.find(instanceInfo->BundleControllerAnnotations->NannyService);
        if (it == nannyServiceToZone.end()) {
            continue;
        }
        const auto& zoneName = it->second;
        const auto& dataCenterName = instanceInfo->BundleControllerAnnotations->DataCenter.value_or(DefaultDataCenterName);
        result[zoneName].PerDataCenter[dataCenterName].push_back(instanceName);
    }

    return result;
}


template <class TCollection>
TSchedulerInputState::TBundleToInstanceMapping MapBundlesToInstances(const TCollection& collection)
{
    TSchedulerInputState::TBundleToInstanceMapping result;

    for (const auto& [instanceName, instanceInfo] : collection) {
        auto dataCenter = instanceInfo->BundleControllerAnnotations->DataCenter.value_or(DefaultDataCenterName);
        auto bundleName = instanceInfo->BundleControllerAnnotations->AllocatedForBundle;

        if (!bundleName.empty()) {
            result[bundleName][dataCenter].push_back(instanceName);
        }
    }

    return result;
}

THashMap<std::string, std::string> MapPodIdToInstanceName(const TSchedulerInputState& input)
{
    THashMap<std::string, std::string> result;

    for (const auto& [nodeName, instanceInfo] : input.TabletNodes) {
        auto podId = GetPodIdForInstance(instanceInfo->CypressAnnotations, nodeName);
        result[podId] = nodeName;
    }

    for (const auto& [proxyName, instanceInfo] : input.RpcProxies) {
        auto podId = GetPodIdForInstance(instanceInfo->CypressAnnotations, proxyName);
        result[podId] = proxyName;
    }

    return result;
}

THashMap<std::string, TDataCenterRackInfo> MapZonesToRacks(
    const TSchedulerInputState& input,
    TOnAlertCallback onAlert)
{
    THashMap<std::string, TDataCenterRackInfo> zoneToRacks;

    for (const auto& [zoneName, zoneNodes] : input.ZoneNodes) {
        auto zoneInfo = GetOrCrash(input.Zones, zoneName);
        auto spareBundleName = zoneInfo->SpareBundleName;

        for (const auto& [dataCenterName, dataCenterNodes] : zoneNodes.PerDataCenter) {
            auto& dataCenterRacks = zoneToRacks[zoneName][dataCenterName];

            for (const auto& tabletNode : dataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, tabletNode);
                if (nodeInfo->State != InstanceStateOnline) {
                    continue;
                }

                if (nodeInfo->BundleControllerAnnotations->AllocatedForBundle == spareBundleName) {
                    ++dataCenterRacks.RackToSpareInstances[nodeInfo->Rack];
                } else {
                    ++dataCenterRacks.RackToBundleInstances[nodeInfo->Rack];
                }
            }
        }
    }

    for (auto& [_, zoneRacks] : zoneToRacks) {
        for (auto& [_, dataCenterRacks] : zoneRacks) {
            for (const auto& [rackName, bundleNodes] : dataCenterRacks.RackToBundleInstances) {
                int spareNodeCount = 0;

                const auto& spareRacks = dataCenterRacks.RackToSpareInstances;
                if (auto it = spareRacks.find(rackName); it != spareRacks.end()) {
                    spareNodeCount = it->second;
                }

                dataCenterRacks.RequiredSpareNodeCount = std::max(
                    dataCenterRacks.RequiredSpareNodeCount,
                    bundleNodes + spareNodeCount);
            }
        }
    }

    for (auto& [zone, zoneInfo] : input.Zones) {
        for (auto& [dataCenter, _] : zoneInfo->DataCenters) {
            auto zoneIt = zoneToRacks.find(zone);
            if (zoneIt == zoneToRacks.end()) {
                continue;
            }

            auto dataCenterIt = zoneIt->second.find(dataCenter);
            if (dataCenterIt == zoneIt->second.end()) {
                continue;
            }

            if (zoneInfo->RequiresMinusOneRackGuarantee && zoneInfo->SpareTargetConfig->TabletNodeCount < dataCenterIt->second.RequiredSpareNodeCount) {
                onAlert({
                    .Id = "minus_one_rack_guarantee_violation",
                    .DataCenter = dataCenter,
                    .Description = Format("Zone %v in data center %v has target spare nodes: %v "
                        ", where required count is at least %v.",
                        zone,
                        dataCenter,
                        zoneInfo->SpareTargetConfig->TabletNodeCount,
                        dataCenterIt->second.RequiredSpareNodeCount),
                });

                YT_LOG_WARNING("Zone spare nodes violate minus one rack guarantee (Zone: %v, DataCenter: %v, ZoneSpareNodes: %v, RequiredSpareNodes: %v)",
                    zone,
                    dataCenter,
                    zoneInfo->SpareTargetConfig->TabletNodeCount,
                    dataCenterIt->second.RequiredSpareNodeCount);
            }
        }
    }

    return zoneToRacks;
}

////////////////////////////////////////////////////////////////////////////////

void InitializeRelations(TSchedulerInputState* input, TOnAlertCallback onAlert)
{
    InitDefaultDataCenter(input);
    InitializeVirtualSpareBundle(input);

    input->ZoneNodes = MapZonesToInstances(*input, input->TabletNodes);
    input->ZoneProxies = MapZonesToInstances(*input, input->RpcProxies);
    input->BundleNodes = MapBundlesToInstances(input->TabletNodes);
    input->BundleProxies = MapBundlesToInstances(input->RpcProxies);
    input->PodIdToInstanceName = MapPodIdToInstanceName(*input);
    input->ZoneToRacks = MapZonesToRacks(*input, onAlert);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
