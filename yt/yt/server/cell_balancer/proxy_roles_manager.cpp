#include "bundle_scheduler.h"
#include "config.h"
#include "input_state.h"
#include "mutations.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

using TZoneName = std::string;
using TRoleName = std::string;
using TProxyRoleToBundle = THashMap<std::pair<TZoneName, TRoleName>, std::string>;

TPerDataCenterSpareProxiesInfo GetSpareProxiesInfo(
    const std::string& zoneName,
    const TProxyRoleToBundle& proxyRoleToBundle,
    const TSchedulerInputState& input)
{
    auto zoneIt = input.Zones.find(zoneName);
    if (zoneIt == input.Zones.end()) {
        return {};
    }

    auto spareProxiesIt = input.ProxiesAllocatedForBundle.find(
        zoneIt->second->SpareBundleName);
    if (spareProxiesIt == input.ProxiesAllocatedForBundle.end()) {
        return {};
    }

    const auto& spareProxies = spareProxiesIt->second;
    auto zoneAliveProxies = GetAliveProxies(spareProxies, input, EGracePeriodBehaviour::Immediately);

    TPerDataCenterSpareProxiesInfo result;
    for (const auto& [dataCenterName, aliveProxies] : zoneAliveProxies) {
        auto& spareProxies = result[dataCenterName];
        for (const auto& spareProxy : aliveProxies) {
            auto proxyInfo = GetOrCrash(input.RpcProxies, spareProxy);

            bool hasMaintenanceRequests = !proxyInfo->CmsMaintenanceRequests.empty();

            if (auto it = proxyRoleToBundle.find(std::pair{zoneIt->first, proxyInfo->Role}); it != proxyRoleToBundle.end()) {
                const auto& bundleName = it->second;
                YT_VERIFY(!bundleName.empty());
                spareProxies.UsedByBundle[bundleName].push_back(spareProxy);
            } else {
                if (hasMaintenanceRequests) {
                    spareProxies.ScheduledForMaintenance.push_back(spareProxy);
                } else {
                    spareProxies.FreeProxies.push_back(spareProxy);
                }
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TryReleaseSpareProxies(
    const std::string& bundleName,
    int excessProxyCount,
    TSpareProxiesInfo* spareProxiesInfo,
    TSchedulerMutations* mutations)
{
    const auto& usingSpareProxies = spareProxiesInfo->UsedByBundle[bundleName];
    excessProxyCount = std::min<int>(excessProxyCount, std::ssize(usingSpareProxies));
    auto proxiesToRelease = std::span(usingSpareProxies.begin(), usingSpareProxies.begin() + excessProxyCount);

    for (const auto& proxyName : proxiesToRelease) {
        mutations->RemovedProxyRole.insert(mutations->WrapMutation(proxyName));

        YT_TLOG_INFO("Releasing spare proxy for bundle")
            .With("Bundle", bundleName)
            .With("ProxyName", proxyName);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryAssignSpareProxies(
    const std::string& bundleName,
    const std::string& zoneName,
    const std::string& dataCenterName,
    const std::string& proxyRole,
    int proxyCount,
    TSpareInstanceAllocator<TSpareProxiesInfo>& spareProxiesAllocator,
    TSchedulerMutations* mutations)
{
    while (spareProxiesAllocator.HasInstances(zoneName, dataCenterName) && proxyCount > 0) {
        auto proxyName = spareProxiesAllocator.Allocate(zoneName, dataCenterName, bundleName);
        --proxyCount;
        mutations->ChangedProxyRole[proxyName] = mutations->WrapMutation(proxyRole);

        YT_TLOG_INFO("Assigning spare proxy for bundle")
            .With("Bundle", bundleName)
            .With("ProxyName", proxyName);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterOrderForProxies
{
    // Data center does not have enough alive RPC proxy (even with spare proxies).
    bool Unfeasible = false;

    // Data center is forbidden by admin.
    bool Forbidden = false;

    int AssignedProxyCount = 0;

    // How many RPC proxies we have to assign to bundle, i.e. how many proxies do not have needed proxy role.
    int RequiredRpcProxyAssignmentCount = 0;

    // Just dc name alphabetical order for predictability.
    std::string DataCenter;

    auto MakeTuple() const
    {
        return std::tie(Unfeasible, Forbidden, AssignedProxyCount, RequiredRpcProxyAssignmentCount, DataCenter);
    }

    bool operator<(const TDataCenterOrderForProxies& other) const
    {
        return MakeTuple() < other.MakeTuple();
    }
};

////////////////////////////////////////////////////////////////////////////////

int GetAvailableLiveRpcProxyCount(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveProxies,
    const TPerDataCenterSpareProxiesInfo& spareProxies)
{
    int result = 0;

    if (auto it = perDataCenterAliveProxies.find(dataCenterName); it != perDataCenterAliveProxies.end()) {
        result += std::ssize(it->second);
    }

    if (auto it = spareProxies.find(dataCenterName); it != spareProxies.end()) {
        const auto& dataCenterSpare = it->second;
        result += std::ssize(dataCenterSpare.FreeProxies);

        const auto& usedByBundle = dataCenterSpare.UsedByBundle;
        auto bundleIt = usedByBundle.find(bundleName);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

int GetAssignedRpcProxyCount(
    const std::string& bundleName,
    const std::string& rpcProxyRole,
    const std::string& dataCenterName,
    const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveProxies,
    const TPerDataCenterSpareProxiesInfo& spareProxies,
    const TSchedulerInputState& input)
{
    int result = 0;

    if (auto it = perDataCenterAliveProxies.find(dataCenterName); it != perDataCenterAliveProxies.end()) {
        for (const auto& proxyName : it->second) {
            auto proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            if (proxyInfo->Role == rpcProxyRole) {
                ++result;
            }
        }
    }

    if (auto it = spareProxies.find(dataCenterName); it != spareProxies.end()) {
        const auto& usedByBundle = it->second.UsedByBundle;

        auto bundleIt = usedByBundle.find(bundleName);
        if (bundleIt != usedByBundle.end()) {
            result += std::ssize(bundleIt->second);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> GetDataCentersToPopulate(
    const std::string& bundleName,
    const std::string& rpcProxyRole,
    const THashMap<std::string, THashSet<std::string>>& perDataCenterAliveProxies,
    const TPerDataCenterSpareProxiesInfo& spareProxies,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

    if (auto redundantCount = targetConfig->RedundantRpcProxyDataCenterCount;
        redundantCount && (redundantCount < 0 || *redundantCount >= ssize(zoneInfo->DataCenters)))
    {
        mutations->AlertsToFire.push_back({
            .Id = "invalid_bundle_config",
            .Description = Format("Invalid value for \"redundant_rpc_proxy_data_center_count\": "
                "expected in range [%v, %v], got %v",
                0,
                ssize(zoneInfo->DataCenters) - 1,
                redundantCount)
        });

        return {};
    }

    int redundantDataCenterCount = targetConfig->RedundantRpcProxyDataCenterCount.value_or(
         zoneInfo->RedundantDataCenterCount);
    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - redundantDataCenterCount;
    YT_VERIFY(activeDataCenterCount > 0);
    int perDataCenterProxyCount = targetConfig->RpcProxyCount / std::ssize(zoneInfo->DataCenters);

    std::vector<TDataCenterOrderForProxies> dataCentersOrder;
    dataCentersOrder.reserve(std::ssize(zoneInfo->DataCenters));

    for (const auto& [dataCenter, dataCenterInfo] : zoneInfo->DataCenters) {
        int availableProxyCount = GetAvailableLiveRpcProxyCount(
            bundleName,
            dataCenter,
            perDataCenterAliveProxies,
            spareProxies);

        int assignedProxyCount = GetAssignedRpcProxyCount(
            bundleName,
            rpcProxyRole,
            dataCenter,
            perDataCenterAliveProxies,
            spareProxies,
            input);

        bool perBundleForbidden = targetConfig->ForbiddenDataCenters.count(dataCenter) != 0;

        dataCentersOrder.push_back({
            .Unfeasible = availableProxyCount < perDataCenterProxyCount,
            .Forbidden = dataCenterInfo->Forbidden || perBundleForbidden,
            .RequiredRpcProxyAssignmentCount = perDataCenterProxyCount - assignedProxyCount,
            .DataCenter = dataCenter,
        });

        const auto& status = dataCentersOrder.back();

        YT_TLOG_DEBUG("Bundle RPC proxy data center status")
            .With("Bundle", bundleName)
            .With("DataCenter", dataCenter)
            .With("Unfeasible", status.Unfeasible)
            .With("Forbidden", status.Forbidden)
            .With("RequiredPerDataCenterProxyCount", perDataCenterProxyCount)
            .With("RequiredRpcProxyAssignmentCount", status.RequiredRpcProxyAssignmentCount)
            .With("AvailableRpcProxyCount", availableProxyCount);
    }

    std::sort(dataCentersOrder.begin(), dataCentersOrder.end());
    dataCentersOrder.resize(activeDataCenterCount);

    THashSet<std::string> result;
    for (const auto& item : dataCentersOrder) {
        result.insert(item.DataCenter);
    }

    YT_TLOG_DEBUG("Bundle data center preference")
        .With("Bundle", bundleName)
        .With("DataCenters", result);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void AssignProxyRoleForDataCenter(
    const std::string& bundleName,
    const std::string& zoneName,
    const std::string& rpcProxyRole,
    const std::string& dataCenterName,
    int requiredRpcProxyCount,
    const THashSet<std::string>& aliveProxies,
    const TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareProxiesInfo>& spareProxiesAllocator,
    TSchedulerMutations* mutations)
{
    auto& spareProxies = spareProxiesAllocator.SpareInstances[zoneName][dataCenterName];

    for (const auto& proxyName : aliveProxies) {
        auto proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (proxyInfo->Role != rpcProxyRole) {
            YT_TLOG_INFO("Assigning proxy role for bundle RPC proxy")
                .With("Bundle", bundleName)
                .With("DataCenter", dataCenterName)
                .With("ProxyName", proxyName)
                .With("Role", rpcProxyRole);

            mutations->ChangedProxyRole[proxyName] = mutations->WrapMutation(rpcProxyRole);
        }
    }

    auto getUsedSpareProxyCount = [bundleName] (auto& sparesByBundle) {
        if (auto it = sparesByBundle.find(bundleName); it != sparesByBundle.end()) {
            return std::ssize(it->second);
        }
        return 0L;
    };

    int aliveBundleProxyCount = std::ssize(aliveProxies);
    int usedSpareProxyCount = getUsedSpareProxyCount(spareProxies.UsedByBundle);

    int proxyBalance = usedSpareProxyCount + aliveBundleProxyCount - requiredRpcProxyCount;

    YT_TLOG_DEBUG("Checking RPC proxies role for bundle in data center")
        .With("Bundle", bundleName)
        .With("DataCenter", dataCenterName)
        .With("RpcProxyRole", rpcProxyRole)
        .With("ProxyBalance", proxyBalance)
        .With("SpareProxyCount", usedSpareProxyCount)
        .With("BundleProxyCount", aliveBundleProxyCount)
        .With("RequiredRpcProxyCount", requiredRpcProxyCount);

    if (proxyBalance > 0) {
        TryReleaseSpareProxies(bundleName, proxyBalance, &spareProxies, mutations);
    } else {
        TryAssignSpareProxies(bundleName, zoneName, dataCenterName, rpcProxyRole, std::abs(proxyBalance), spareProxiesAllocator, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string GetReleasedProxyRole(const std::string& rpcProxyRole)
{
    return rpcProxyRole + "_released";
}

////////////////////////////////////////////////////////////////////////////////

void ReleaseProxyRoleForDataCenter(
    const std::string& bundleName,
    const std::string& rpcProxyRole,
    const std::string& dataCenterName,
    const THashSet<std::string>& aliveProxies,
    const TSchedulerInputState& input,
    TSpareProxiesInfo* spareProxies,
    TSchedulerMutations* mutations)
{
    auto releasedProxyRole = GetReleasedProxyRole(rpcProxyRole);

    for (const auto& proxyName : aliveProxies) {
        auto proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (proxyInfo->Role != releasedProxyRole) {
            YT_TLOG_INFO("Releasing proxy role for bundle RPC proxy")
                .With("Bundle", bundleName)
                .With("DataCenter", dataCenterName)
                .With("ProxyName", proxyName)
                .With("Role", releasedProxyRole);

            mutations->ChangedProxyRole[proxyName] = mutations->WrapMutation(releasedProxyRole);
        }
    }

    auto getUsedSpareProxyCount = [bundleName] (auto& sparesByBundle) {
        if (auto it = sparesByBundle.find(bundleName); it != sparesByBundle.end()) {
            return std::ssize(it->second);
        }
        return 0L;
    };

    auto usedSpareProxyCount = getUsedSpareProxyCount(spareProxies->UsedByBundle);
    if (usedSpareProxyCount > 0) {
        TryReleaseSpareProxies(bundleName, usedSpareProxyCount, spareProxies, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

void SetProxyRole(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleProxies,
    const TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareProxiesInfo>& spareProxiesAllocator,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneName = bundleInfo->Zone;
    auto perDataCenterAliveProxies = GetAliveProxies(bundleProxies, input, EGracePeriodBehaviour::Immediately);
    auto proxyRole = bundleInfo->RpcProxyRole.value_or(bundleName);

    auto& perDataCenterSpareProxies = spareProxiesAllocator.SpareInstances[zoneName];

    if (proxyRole.empty()) {
        YT_TLOG_WARNING("Empty string assigned as proxy role name for bundle")
            .With("Bundle", bundleName);

        mutations->AlertsToFire.push_back({
            .Id = "invalid_proxy_role_value",
            .Description = Format("Empty string assigned as proxy role name for bundle %Qv.",
                bundleName),
        });
        return;
    }

    auto dataCentersToPopulate = GetDataCentersToPopulate(
        bundleName,
        proxyRole,
        perDataCenterAliveProxies,
        perDataCenterSpareProxies,
        input,
        mutations);

    // May happen in case of invalid bundle config (see YT-26888).
    if (dataCentersToPopulate.empty()) {
        return;
    }

    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);
    int perDataCenterProxyCount = targetConfig->RpcProxyCount / std::ssize(zoneInfo->DataCenters);

    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveProxies = perDataCenterAliveProxies[dataCenterName];
        auto* spareProxies = &perDataCenterSpareProxies[dataCenterName];

        if (dataCentersToPopulate.count(dataCenterName) != 0) {
            AssignProxyRoleForDataCenter(
                bundleName,
                zoneName,
                proxyRole,
                dataCenterName,
                perDataCenterProxyCount,
                aliveProxies,
                input,
                spareProxiesAllocator,
                mutations);
        } else {
            ReleaseProxyRoleForDataCenter(
                bundleName,
                proxyRole,
                dataCenterName,
                aliveProxies,
                input,
                spareProxies,
                mutations);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void InitializeZoneToSpareProxies(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TProxyRoleToBundle proxyRoleToBundle;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }
        if (bundleInfo->RpcProxyRole && !bundleInfo->RpcProxyRole->empty()) {
            proxyRoleToBundle[std::pair{bundleInfo->Zone, *bundleInfo->RpcProxyRole}] = bundleName;
        } else {
            proxyRoleToBundle[std::pair{bundleInfo->Zone, bundleName}] = bundleName;
        }
    }

    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareProxies[zoneName] = GetSpareProxiesInfo(zoneName, proxyRoleToBundle, input);

        const auto& perDCSpareInfo = input.ZoneToSpareProxies[zoneName];

        for (const auto& [dataCenterName, spareInfo] : perDCSpareInfo) {
            if (std::ssize(spareInfo.FreeProxies) == 0 && std::ssize(spareInfo.UsedByBundle) > 0) {
                YT_TLOG_WARNING("No free spare proxies available")
                    .With("Zone", zoneName)
                    .With("DataCenter", dataCenterName);

                mutations->AlertsToFire.push_back({
                    .Id = "no_free_spare_proxies",
                    .Description = Format("No free spare proxies available in zone %Qv in datacenter %Qv.",
                        zoneName,
                        dataCenterName),
                });
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageRpcProxyRoles(
    TSchedulerInputState& input,
    TSpareInstanceAllocator<TSpareProxiesInfo>& spareProxiesAllocator,
    TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableRpcProxyManagement) {
            continue;
        }
        if (auto zoneIt = input.Zones.find(bundleInfo->Zone); zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& bundleProxies = input.ProxiesAllocatedForBundle[bundleName];
        SetProxyRole(bundleName, bundleProxies, input, spareProxiesAllocator, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
