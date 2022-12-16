#include "bundle_scheduler.h"
#include "config.h"

#include <span>

namespace NYT::NCellBalancer {

///////////////////////////////////////////////////////////////

static const auto& Logger = BundleControllerLogger;

///////////////////////////////////////////////////////////////

TSpareProxiesInfo GetSpareProxiesInfo(
    const TString& zoneName,
    const TSchedulerInputState& input)
{
    auto spareBundle = GetSpareBundleName(zoneName);
    auto spareProxiesIt = input.BundleProxies.find(spareBundle);
    if (spareProxiesIt == input.BundleProxies.end()) {
        return {};
    }

    const auto& spareProxies = spareProxiesIt->second;
    auto aliveProxies = GetAliveProxies(spareProxies, input);

    TSpareProxiesInfo result;

    for (const auto& spareProxy : spareProxies) {
        auto proxyInfo = GetOrCrash(input.RpcProxies, spareProxy);
        const auto& bundleName = proxyInfo->Role;
        if (!bundleName.empty()) {
            result.UsedByBundle[bundleName].push_back(spareProxy);
        } else {
            result.FreeProxies.push_back(spareProxy);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TryReleaseSpareProxies(
    const TString& bundleName,
    int excessProxyCount,
    TSpareProxiesInfo& spareProxiesInfo,
    TSchedulerMutations* mutations)
{
    auto usingSpareProxies = spareProxiesInfo.UsedByBundle[bundleName];
    excessProxyCount = std::min<int>(excessProxyCount, std::ssize(usingSpareProxies));

    auto proxiesToRelease = std::span(usingSpareProxies.begin(), usingSpareProxies.begin() + excessProxyCount);

    for (const auto& proxyName : proxiesToRelease) {
        mutations->ChangedProxyRole[proxyName] = {};

        YT_LOG_INFO("Releasing spare proxy for bundle (Bundle: %v, ProxyName: %v)",
            bundleName,
            proxyName);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TryAssignSpareProxies(
    const TString& bundleName,
    int proxiesCount,
    TSpareProxiesInfo& spareProxyInfo,
    TSchedulerMutations* mutations)
{
    auto& freeProxies = spareProxyInfo.FreeProxies;

    while (!freeProxies.empty() && proxiesCount > 0) {
        const auto& proxyName = freeProxies.back();
        mutations->ChangedProxyRole[proxyName] = bundleName;

        YT_LOG_INFO("Assigning spare proxy for bundle (Bundle: %v, ProxyName: %v)",
            bundleName,
            proxyName);

        freeProxies.pop_back();
        --proxiesCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

void SetProxyRole(
    const TString& bundleName,
    const std::vector<TString>& bundleProxies,
    const TSchedulerInputState& input,
    TSpareProxiesInfo& spareProxies,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto aliveProxies = GetAliveProxies(bundleProxies, input);

    for (const auto& proxyName : aliveProxies) {
        auto proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (proxyInfo->Role != bundleName) {
            mutations->ChangedProxyRole[proxyName] = bundleName;
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
    int requiredProxyCount = bundleInfo->TargetConfig->RpcProxyCount;

    int proxyBalance = usedSpareProxyCount  + aliveBundleProxyCount - requiredProxyCount;

    YT_LOG_DEBUG("Checking rpc proxies role for bundle (Bundle: %v, ProxyBalance: %v, SpareProxyCount: %v, BundleProxyCount: %v, RequiredProxyCount: %v)",
        bundleName,
        proxyBalance,
        usedSpareProxyCount,
        aliveBundleProxyCount,
        requiredProxyCount);

    if (proxyBalance > 0) {
        TryReleaseSpareProxies(bundleName, proxyBalance, spareProxies, mutations);
    } else {
        TryAssignSpareProxies(bundleName, std::abs(proxyBalance), spareProxies, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageRpcProxyRoles(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [zoneName, _] : input.Zones) {
        input.ZoneToSpareProxies[zoneName] = GetSpareProxiesInfo(zoneName, input);

        const auto& spareInfo = input.ZoneToSpareProxies[zoneName];

        if (std::ssize(spareInfo.FreeProxies) == 0 && std::ssize(spareInfo.UsedByBundle) > 0) {
            YT_LOG_WARNING("No free spare proxies available (Zone: %v)",
                zoneName);

            mutations->AlertsToFire.push_back({
                .Id = "no_free_spare_proxies",
                .Description = Format("No free spare proxies available in zone: %v.",
                    zoneName),
            });
        }
    }

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableRpcProxyManagement) {
            continue;
        }

        auto& spareProxies = input.ZoneToSpareProxies[bundleInfo->Zone];
        const auto& bundleProxies = input.BundleProxies[bundleName];
        SetProxyRole(bundleName, bundleProxies, input, spareProxies, mutations);
    }
}

///////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
