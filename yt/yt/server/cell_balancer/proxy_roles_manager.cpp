#include "bundle_scheduler.h"
#include "config.h"

#include <span>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

using TProxyRoleToBundle = THashMap<std::string, std::string>;

TPerDataCenterSpareProxiesInfo GetSpareProxiesInfo(
    const std::string& zoneName,
    const TProxyRoleToBundle& proxyRoleToBundle,
    const TSchedulerInputState& input)
{
    auto zoneIt = input.Zones.find(zoneName);
    if (zoneIt == input.Zones.end()) {
        return {};
    }

    auto spareBundle = GetSpareBundleName(zoneIt->second);
    auto spareProxiesIt = input.BundleProxies.find(spareBundle);
    if (spareProxiesIt == input.BundleProxies.end()) {
        return {};
    }

    const auto& spareProxies = spareProxiesIt->second;
    auto zoneAliveProxies = GetAliveProxies(spareProxies, input, EGracePeriodBehaviour::Immediately);

    TPerDataCenterSpareProxiesInfo result;
    for (const auto& [dataCenterName, aliveProxies] : zoneAliveProxies) {
        auto& spareProxies = result[dataCenterName];
        for (const auto& spareProxy : aliveProxies) {
            auto proxyInfo = GetOrCrash(input.RpcProxies, spareProxy);
            std::string bundleName;

            if (auto it = proxyRoleToBundle.find(proxyInfo->Role); it != proxyRoleToBundle.end()) {
                bundleName = it->second;
            }

            if (!bundleName.empty()) {
                spareProxies.UsedByBundle[bundleName].push_back(spareProxy);
            } else {
                spareProxies.FreeProxies.push_back(spareProxy);
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
        mutations->RemovedProxyRole.insert(proxyName);

        YT_LOG_INFO("Releasing spare proxy for bundle (Bundle: %v, ProxyName: %v)",
            bundleName,
            proxyName);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryAssignSpareProxies(
    const std::string& bundleName,
    const std::string& proxyRole,
    int proxyCount,
    TSpareProxiesInfo* spareProxiesInfo,
    TSchedulerMutations* mutations)
{
    auto& freeProxies = spareProxiesInfo->FreeProxies;

    while (!freeProxies.empty() && proxyCount > 0) {
        const auto& proxyName = freeProxies.back();
        mutations->ChangedProxyRole[proxyName] = proxyRole;

        YT_LOG_INFO("Assigning spare proxy for bundle (Bundle: %v, ProxyName: %v)",
            bundleName,
            proxyName);

        freeProxies.pop_back();
        --proxyCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterOrderForProxies
{
    // Data center does not have enough alive rpc proxy (even with spare proxies).
    bool Unfeasible = false;

    // Data center is forbidden by admin.
    bool Forbidden = false;

    int AssignedProxyCount = 0;

    // How many rpc proxies we have to assign to bundle, i.e. how many proxies do not have needed proxy role.
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
    const TSchedulerInputState& input)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - zoneInfo->RedundantDataCenterCount;
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

        YT_LOG_DEBUG(
            "Bundle rpc proxy data center status "
            "(Bundle: %v, DataCenter: %v, Unfeasible: %v, Forbidden: %v, RequiredPerDataCenterProxyCount: %v,"
            " RequiredRpcProxyAssignmentCount: %v, AvailableRpcProxyCount: %v)",
            bundleName,
            dataCenter,
            status.Unfeasible,
            status.Forbidden,
            perDataCenterProxyCount,
            status.RequiredRpcProxyAssignmentCount,
            availableProxyCount);
    }

    std::sort(dataCentersOrder.begin(), dataCentersOrder.end());
    dataCentersOrder.resize(activeDataCenterCount);

    THashSet<std::string> result;
    for (const auto& item : dataCentersOrder) {
        result.insert(item.DataCenter);
    }

    YT_LOG_DEBUG("Bundle data center preference (Bundle: %v, DataCenters: %v)",
        bundleName,
        result);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void AssignProxyRoleForDataCenter(
    const std::string& bundleName,
    const std::string& rpcProxyRole,
    const std::string& dataCenterName,
    int requiredRpcProxyCount,
    const THashSet<std::string>& aliveProxies,
    const TSchedulerInputState& input,
    TSpareProxiesInfo* spareProxies,
    TSchedulerMutations* mutations)
{
    for (const auto& proxyName : aliveProxies) {
        auto proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (proxyInfo->Role != rpcProxyRole) {
            YT_LOG_INFO("Assigning proxy role for bundle rpc proxy (Bundle: %v, DataCenter: %v, ProxyName: %v, Role: %v)",
                bundleName,
                dataCenterName,
                proxyName,
                rpcProxyRole);

            mutations->ChangedProxyRole[proxyName] = rpcProxyRole;
        }
    }

    auto getUsedSpareProxyCount = [bundleName] (auto& sparesByBundle) {
        if (auto it = sparesByBundle.find(bundleName); it != sparesByBundle.end()) {
            return std::ssize(it->second);
        }
        return 0L;
    };

    int aliveBundleProxyCount = std::ssize(aliveProxies);
    int usedSpareProxyCount = getUsedSpareProxyCount(spareProxies->UsedByBundle);

    int proxyBalance = usedSpareProxyCount + aliveBundleProxyCount - requiredRpcProxyCount;

    YT_LOG_DEBUG("Checking rpc proxies role for bundle in data center (Bundle: %v, DataCenter: %v, "
        " RpcProxyRole: %v, ProxyBalance: %v, SpareProxyCount: %v, BundleProxyCount: %v, RequiredRpcProxyCount: %v)",
        bundleName,
        dataCenterName,
        rpcProxyRole,
        proxyBalance,
        usedSpareProxyCount,
        aliveBundleProxyCount,
        requiredRpcProxyCount);

    if (proxyBalance > 0) {
        TryReleaseSpareProxies(bundleName, proxyBalance, spareProxies, mutations);
    } else {
        TryAssignSpareProxies(bundleName, rpcProxyRole, std::abs(proxyBalance), spareProxies, mutations);
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
            YT_LOG_INFO("Releasing proxy role for bundle rpc proxy (Bundle: %v, DataCenter: %v, ProxyName: %v, Role: %v)",
                bundleName,
                dataCenterName,
                proxyName,
                releasedProxyRole);

            mutations->ChangedProxyRole[proxyName] = releasedProxyRole;
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
    TPerDataCenterSpareProxiesInfo& perDataCenterSpareProxies,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto perDataCenterAliveProxies = GetAliveProxies(bundleProxies, input, EGracePeriodBehaviour::Immediately);
    auto proxyRole = bundleInfo->RpcProxyRole.value_or(bundleName);

    if (proxyRole.empty()) {
        YT_LOG_WARNING("Empty string assigned as proxy role name for bundle (Bundle: %v)",
            bundleName);

        mutations->AlertsToFire.push_back({
            .Id = "invalid_proxy_role_value",
            .Description = Format("Empty string assigned as proxy role name for bundle %v.",
                bundleName),
        });
        return;
    }

    auto dataCentersToPopulate = GetDataCentersToPopulate(
        bundleName,
        proxyRole,
        perDataCenterAliveProxies,
        perDataCenterSpareProxies,
        input);

    const auto& targetConfig = bundleInfo->TargetConfig;
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);
    int perDataCenterProxyCount = targetConfig->RpcProxyCount / std::ssize(zoneInfo->DataCenters);

    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        const auto& aliveProxies = perDataCenterAliveProxies[dataCenterName];
        auto* spareProxies = &perDataCenterSpareProxies[dataCenterName];

        if (dataCentersToPopulate.count(dataCenterName) != 0) {
            AssignProxyRoleForDataCenter(
                bundleName,
                proxyRole,
                dataCenterName,
                perDataCenterProxyCount,
                aliveProxies,
                input,
                spareProxies,
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

void ManageRpcProxyRoles(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TProxyRoleToBundle proxyRoleToBundle;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }
        if (bundleInfo->RpcProxyRole && !bundleInfo->RpcProxyRole->empty()) {
            proxyRoleToBundle[*bundleInfo->RpcProxyRole] = bundleName;
        } else {
            proxyRoleToBundle[bundleName] = bundleName;
        }
    }

    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareProxies[zoneName] = GetSpareProxiesInfo(zoneName, proxyRoleToBundle, input);

        const auto& perDCSpareInfo = input.ZoneToSpareProxies[zoneName];

        for (const auto& [dataCenterName, spareInfo] : perDCSpareInfo) {
            if (std::ssize(spareInfo.FreeProxies) == 0 && std::ssize(spareInfo.UsedByBundle) > 0) {
                YT_LOG_WARNING("No free spare proxies available (Zone: %v)",
                    zoneName);

                mutations->AlertsToFire.push_back({
                    .Id = "no_free_spare_proxies",
                    .Description = Format("No free spare proxies available in zone: %v in datacenter: %v.",
                        zoneName,
                        dataCenterName),
                });
            }
        }
    }

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableRpcProxyManagement) {
            continue;
        }
        if (auto zoneIt = input.Zones.find(bundleInfo->Zone); zoneIt == input.Zones.end()) {
            continue;
        }

        auto& spareProxies = input.ZoneToSpareProxies[bundleInfo->Zone];
        const auto& bundleProxies = input.BundleProxies[bundleName];
        SetProxyRole(bundleName, bundleProxies, input, spareProxies, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
