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

void TBundleInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_quota", &TThis::ResourceQuota)
        .DefaultNew();
    registrar.Parameter("resource_allocated", &TThis::ResourceAllocated)
        .DefaultNew();

    registrar.Parameter("allocated_tablet_nodes", &TThis::AllocatedTabletNodes)
        .Default();
    registrar.Parameter("allocated_rpc_proxies", &TThis::AllocatedRpcProxies)
        .Default();

    registrar.Parameter("assigned_spare_tablet_nodes", &TThis::AssignedSpareTabletNodes)
        .Default();
    registrar.Parameter("assigned_spare_rpc_proxies", &TThis::AssignedSpareRpcProxies)
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

TBundlesInfo GetBundlesInfo(const TSchedulerInputState& state)
{
    TBundlesInfo result;
    for (const auto& [bundleName, bundleInfo] : state.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }
        auto bundleOrchidInfo = New<TBundleInfo>();
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

        auto calculateUsage = [&bundleOrchidInfo] (const auto& instancies) {
            auto& allocated = *bundleOrchidInfo->ResourceAllocated;
            for (const auto& [_, instanceInfo] : instancies) {
                allocated.Vcpu += instanceInfo->Resource->Vcpu;
                allocated.Memory += instanceInfo->Resource->Memory;
            }
        };

        calculateUsage(bundleOrchidInfo->AllocatedTabletNodes);
        calculateUsage(bundleOrchidInfo->AllocatedRpcProxies);

        // Fake quota for a while.
        bundleOrchidInfo->ResourceQuota = CloneYsonSerializable(bundleOrchidInfo->ResourceAllocated);

        result[bundleName] = bundleOrchidInfo;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer::Orchid
