#include "orchid_bindings.h"

#include "config.h"

#include <yt/yt/core/ytree/yson_struct.h>


namespace NYT::NCellBalancer::NOrchid {

////////////////////////////////////////////////////////////////////////////////

void TInstanceInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("resource", &TThis::Resource)
        .DefaultNew();

    registrar.Parameter("pod_id", &TThis::PodId)
        .Default();
    registrar.Parameter("yp_cluster", &TThis::YPCluster)
        .Default();
    registrar.Parameter("removing", &TThis::Removing)
        .Optional();
    registrar.Parameter("data_center", &TThis::DataCenter)
        .Optional();
}

void TAlert::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("description", &TThis::Description)
        .Default();
}

void TAllocatingInstanceInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("hulk_request_state", &TThis::HulkRequestState)
        .Default();
    registrar.Parameter("hulk_request_link", &TThis::HulkRequestLink)
        .Default();
    registrar.Parameter("instance_info", &TThis::InstanceInfo)
        .Optional();
}

void TBundleInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_quota", &TThis::ResourceQuota)
        .DefaultNew();
    registrar.Parameter("resource_allocated", &TThis::ResourceAllocated)
        .DefaultNew();
    registrar.Parameter("resource_alive", &TThis::ResourceAlive)
        .DefaultNew();
    registrar.Parameter("resource_target", &TThis::ResourceTarget)
        .DefaultNew();

    registrar.Parameter("allocated_tablet_nodes", &TThis::AllocatedTabletNodes)
        .Default();
    registrar.Parameter("allocated_rpc_proxies", &TThis::AllocatedRpcProxies)
        .Default();

    registrar.Parameter("allocating_tablet_nodes", &TThis::AllocatingTabletNodes)
        .Default();
    registrar.Parameter("allocating_rpc_proxies", &TThis::AllocatingRpcProxies)
        .Default();

    registrar.Parameter("assigned_spare_tablet_nodes", &TThis::AssignedSpareTabletNodes)
        .Default();
    registrar.Parameter("assigned_spare_rpc_proxies", &TThis::AssignedSpareRpcProxies)
        .Default();

    registrar.Parameter("removing_cell_count", &TThis::RemovingCellCount)
        .Default();
    registrar.Parameter("allocating_tablet_node_count", &TThis::AllocatingTabletNodeCount)
        .Default();
    registrar.Parameter("deallocating_tablet_node_count", &TThis::DeallocatingTabletNodeCount)
        .Default();
    registrar.Parameter("allocating_rpc_proxy_count", &TThis::AllocatingRpcProxyCount)
        .Default();
    registrar.Parameter("deallocating_rpc_proxy_count", &TThis::DeallocatingRpcProxyCount)
        .Default();

    registrar.Parameter("alerts", &TThis::Alerts)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDataCenterRacksInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("rack_to_bundle_nodes", &TThis::RackToBundleNodes)
        .Default();
    registrar.Parameter("rack_to_spare_nodes", &TThis::RackToSpareNodes)
        .Default();
    registrar.Parameter("required_spare_node_count", &TThis::RequiredSpareNodeCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TScanBundleCounter::Register(TRegistrar registrar)
{
    registrar.Parameter("successful", &TThis::Successful)
        .Default(0);
    registrar.Parameter("failed", &TThis::Failed)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TBundleInstances, typename TCollection>
void PopulateInstances(
    const TBundleInstances& bundleInstances,
    const TCollection& instanciesInfo,
    THashMap<std::string, TInstanceInfoPtr>& instancies)
{
    for (const auto& name : bundleInstances) {
        auto instance = New<TInstanceInfo>();
        const auto& instanceInfo = GetOrCrash(instanciesInfo, name);
        const auto& annotations = instanceInfo->Annotations;

        instance->Resource = annotations->Resource;
        instance->PodId = GetPodIdForInstance(name);
        instance->YPCluster = annotations->YPCluster;

        instance->DataCenter = annotations->DataCenter;

        instancies[name] = instance;
    }
}

template <typename TBundleToInstances, typename TCollection>
void PopulateInstancesPerDC(
    const std::string& bundleName,
    const TBundleToInstances& bundleToInstances,
    const TCollection& instanciesInfo,
    THashMap<std::string, TInstanceInfoPtr>& instancies)
{
    auto it = bundleToInstances.find(bundleName);
    if (it == bundleToInstances.end()) {
        return;
    }

    for (const auto& [_, dataCenterNodes] : it->second) {
        PopulateInstances(dataCenterNodes, instanciesInfo, instancies);
    }
}

template <typename TBundleToInstances, typename TCollection>
void PopulateInstancesPerBundle(
    const std::string& bundleName,
    const TBundleToInstances& bundleToInstances,
    const TCollection& instanciesInfo,
    THashMap<std::string, TInstanceInfoPtr>& instancies)
{
    auto it = bundleToInstances.find(bundleName);
    if (it == bundleToInstances.end()) {
        return;
    }

    PopulateInstances(it->second, instanciesInfo, instancies);
}

static const std::string INITIAL_REQUEST_STATE = "REQUEST_CREATED";

void PopulateAllocatingInstances(
    const TIndexedEntries<TAllocationRequestState>& allocationStates,
    const TSchedulerInputState& input,
    TIndexedEntries<TAllocatingInstanceInfo>& destination)
{
    for (const auto& [allocationId, _] : allocationStates) {
        auto& orchidInfo = destination[allocationId];
        orchidInfo = New<TAllocatingInstanceInfo>();
        orchidInfo->HulkRequestLink = Format("%v/%v", input.Config->HulkAllocationsPath, allocationId);
        orchidInfo->HulkRequestState = INITIAL_REQUEST_STATE;
        auto it = input.AllocationRequests.find(allocationId);
        if (it == input.AllocationRequests.end()) {
            continue;
        }
        const auto& request = it->second;
        auto& instanceInfo = orchidInfo->InstanceInfo;
        instanceInfo = New<TInstanceInfo>();
        orchidInfo->HulkRequestState = request->Status->State;
        instanceInfo->PodId = request->Status->PodId;
        instanceInfo->YPCluster = request->Spec->YPCluster;
        ConvertToInstanceResources(*instanceInfo->Resource, *request->Spec->ResourceRequest);
    }
}

void MarkDeallocatingInstances(
    const TIndexedEntries<TDeallocationRequestState>& deallocations,
    THashMap<std::string, TInstanceInfoPtr>& allocatedInstances)
{
    for (const auto& [_, deallocationState] : deallocations) {
        auto it = allocatedInstances.find(deallocationState->InstanceName);
        if (it == allocatedInstances.end()) {
            continue;
        }
        it->second->Removing = true;
    }
}

TBundlesInfo GetBundlesInfo(const TSchedulerInputState& state, const TSchedulerMutations& mutations)
{
    auto mergedBundlesState = MergeBundleStates(state, mutations);

    TBundlesInfo result;
    for (const auto& [bundleName, bundleInfo] : state.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }
        auto bundleOrchidInfo = New<TBundleInfo>();
        bundleOrchidInfo->ResourceAllocated->Clear();
        bundleOrchidInfo->ResourceAlive->Clear();
        bundleOrchidInfo->ResourceTarget->Clear();

        PopulateInstancesPerDC(bundleName, state.BundleNodes, state.TabletNodes, bundleOrchidInfo->AllocatedTabletNodes);
        PopulateInstancesPerDC(bundleName, state.BundleProxies, state.RpcProxies, bundleOrchidInfo->AllocatedRpcProxies);

        if (auto it = state.ZoneToSpareNodes.find(bundleInfo->Zone); it != state.ZoneToSpareNodes.end()) {
            for (const auto& [_, spareInfo] : it->second) {
                PopulateInstancesPerBundle(
                    bundleName,
                    spareInfo.UsedByBundle,
                    state.TabletNodes,
                    bundleOrchidInfo->AssignedSpareTabletNodes);
            }
        }

        if (auto it = state.ZoneToSpareProxies.find(bundleInfo->Zone); it != state.ZoneToSpareProxies.end()) {
            for (const auto& [_, spareInfo] : it->second) {
                PopulateInstancesPerBundle(
                    bundleName,
                    spareInfo.UsedByBundle,
                    state.RpcProxies,
                    bundleOrchidInfo->AssignedSpareRpcProxies);
            }
        }

        if (bundleInfo->ResourceQuota) {
            bundleOrchidInfo->ResourceQuota->Vcpu = bundleInfo->ResourceQuota->Vcpu();
            bundleOrchidInfo->ResourceQuota->Memory = bundleInfo->ResourceQuota->Memory;
        } else {
            bundleOrchidInfo->ResourceQuota = CloneYsonStruct(bundleOrchidInfo->ResourceAllocated);
        }

        if (auto it = state.BundleResourceAlive.find(bundleName); it != state.BundleResourceAlive.end()) {
            bundleOrchidInfo->ResourceAlive = CloneYsonStruct(it->second);
        }

        if (auto it = state.BundleResourceAllocated.find(bundleName); it != state.BundleResourceAllocated.end()) {
            bundleOrchidInfo->ResourceAllocated = CloneYsonStruct(it->second);
        }

        if (auto it = state.BundleResourceTarget.find(bundleName); it != state.BundleResourceTarget.end()) {
            bundleOrchidInfo->ResourceTarget = CloneYsonStruct(it->second);
        }

        if (auto it = mergedBundlesState.find(bundleName); it != mergedBundlesState.end()) {
            const auto& bundleState = it->second;
            bundleOrchidInfo->RemovingCellCount = bundleState->RemovingCells.size();
            bundleOrchidInfo->AllocatingTabletNodeCount = bundleState->NodeAllocations.size();
            bundleOrchidInfo->DeallocatingTabletNodeCount = bundleState->NodeDeallocations.size();
            bundleOrchidInfo->AllocatingRpcProxyCount = bundleState->ProxyAllocations.size();
            bundleOrchidInfo->DeallocatingRpcProxyCount = bundleState->ProxyDeallocations.size();

            PopulateAllocatingInstances(bundleState->NodeAllocations, state, bundleOrchidInfo->AllocatingTabletNodes);
            PopulateAllocatingInstances(bundleState->ProxyAllocations, state, bundleOrchidInfo->AllocatingRpcProxies);

            MarkDeallocatingInstances(bundleState->NodeDeallocations, bundleOrchidInfo->AllocatedTabletNodes);
            MarkDeallocatingInstances(bundleState->ProxyDeallocations, bundleOrchidInfo->AllocatedRpcProxies);
        }

        result[bundleName] = bundleOrchidInfo;
    }

    for (const auto& alert : mutations.AlertsToFire) {
        if (!alert.BundleName || !result[*alert.BundleName]) {
            continue;
        }

        auto bundleAlert = New<TAlert>();
        bundleAlert->Id = alert.Id;
        bundleAlert->Description = alert.Description;

        result[*alert.BundleName]->Alerts.push_back(bundleAlert);
    }

    return result;
}

TZonesRacksInfo GetZonesRacksInfo(const TSchedulerInputState& state)
{
    TZonesRacksInfo zoneRacks;

    for (const auto& [zoneName, zoneInfo] : state.ZoneToRacks) {
        for (const auto& [dataCenterName, dataCenterInfo] : zoneInfo) {
            auto& orchidRack = zoneRacks[zoneName][dataCenterName];
            orchidRack = New<TDataCenterRacksInfo>();
            orchidRack->RackToBundleNodes = dataCenterInfo.RackToBundleInstances;
            orchidRack->RackToSpareNodes = dataCenterInfo.RackToSpareInstances;
            orchidRack->RequiredSpareNodeCount = dataCenterInfo.RequiredSpareNodeCount;
        }
    }

    return zoneRacks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer::NOrchid
