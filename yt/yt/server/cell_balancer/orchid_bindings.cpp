#include "orchid_bindings.h"

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellBalancer::Orchid {

////////////////////////////////////////////////////////////////////////////////

void TInstanceInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("resource", &TThis::Resource)
        .DefaultNew();
}

void TAlert::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("description", &TThis::Description)
        .Default();
}

void TBundleInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_quota", &TThis::ResourceQuota)
        .DefaultNew();
    registrar.Parameter("resource_allocated", &TThis::ResourceAllocated)
        .DefaultNew();
    registrar.Parameter("resource_alive", &TThis::ResourceAlive)
        .DefaultNew();

    registrar.Parameter("allocated_tablet_nodes", &TThis::AllocatedTabletNodes)
        .Default();
    registrar.Parameter("allocated_rpc_proxies", &TThis::AllocatedRpcProxies)
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

template <typename TBundleToInstancies, typename TCollection>
void PopulateInstancies(
    const TString& bundleName,
    const TBundleToInstancies& bundleToInstancies,
    const TCollection& instanciesInfo,
    THashMap<TString, TInstanceInfoPtr>& instancies)
{
    auto it = bundleToInstancies.find(bundleName);
    if (it == bundleToInstancies.end()) {
        return;
    }

    for (const auto& name : it->second) {
        auto instance = New<TInstanceInfo>();
        const auto instanceInfo = GetOrCrash(instanciesInfo, name);
        instance->Resource = instanceInfo->Annotations->Resource;
        instancies[name] = instance;
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

        PopulateInstancies(bundleName, state.BundleNodes, state.TabletNodes, bundleOrchidInfo->AllocatedTabletNodes);
        PopulateInstancies(bundleName, state.BundleProxies, state.RpcProxies, bundleOrchidInfo->AllocatedRpcProxies);

        if (auto it = state.ZoneToSpareNodes.find(bundleInfo->Zone); it != state.ZoneToSpareNodes.end()) {
            PopulateInstancies(
                bundleName,
                it->second.UsedByBundle,
                state.TabletNodes,
                bundleOrchidInfo->AssignedSpareTabletNodes);
        }

        if (auto it = state.ZoneToSpareProxies.find(bundleInfo->Zone); it != state.ZoneToSpareProxies.end()) {
            PopulateInstancies(
                bundleName,
                it->second.UsedByBundle,
                state.RpcProxies,
                bundleOrchidInfo->AssignedSpareRpcProxies);
        }

        if (bundleInfo->ResourceQuota) {
            bundleOrchidInfo->ResourceQuota->Vcpu = bundleInfo->ResourceQuota->Vcpu();
            bundleOrchidInfo->ResourceQuota->Memory = bundleInfo->ResourceQuota->Memory;
        } else {
            bundleOrchidInfo->ResourceQuota = CloneYsonSerializable(bundleOrchidInfo->ResourceAllocated);
        }

        if (auto it = state.BundleResourceAlive.find(bundleName); it != state.BundleResourceAlive.end()) {
            bundleOrchidInfo->ResourceAlive = CloneYsonSerializable(it->second);
        }

        if (auto it = state.BundleResourceAllocated.find(bundleName); it != state.BundleResourceAllocated.end()) {
            bundleOrchidInfo->ResourceAllocated = CloneYsonSerializable(it->second);
        }

        if (auto it = mergedBundlesState.find(bundleName); it != mergedBundlesState.end()) {
            const auto& bundleState = it->second;
            bundleOrchidInfo->RemovingCellCount = bundleState->RemovingCells.size();
            bundleOrchidInfo->AllocatingTabletNodeCount = bundleState->NodeAllocations.size();
            bundleOrchidInfo->DeallocatingTabletNodeCount = bundleState->NodeDeallocations.size();
            bundleOrchidInfo->AllocatingRpcProxyCount = bundleState->ProxyAllocations.size();
            bundleOrchidInfo->DeallocatingRpcProxyCount = bundleState->ProxyDeallocations.size();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer::Orchid
