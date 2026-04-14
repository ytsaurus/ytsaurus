#pragma once

#include <yt/yt/server/cell_balancer/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

static constexpr const char* SpareBundleName = "spare";

////////////////////////////////////////////////////////////////////////////////

std::string GetPodIdForInstance(const std::string& name);

void CheckEmptyAlerts(const TSchedulerMutations& mutations);

void ApplyChangedStates(
    TSchedulerInputState* schedulerState,
    const TSchedulerMutations& mutations);

int CountAlertsExcept(
    const std::vector<TAlert>& alerts,
    const THashSet<std::string>& idsToIgnore);

////////////////////////////////////////////////////////////////////////////////

TBundleInfoPtr SetBundleInfo(
    TSchedulerInputState& input,
    const std::string& bundleName,
    int nodeCount,
    int writeThreadCount = 0,
    int proxyCount = 0);

TSchedulerInputState GenerateSimpleInputContext(
    int nodeCount,
    int writeThreadCount = 0,
    int proxyCount = 0);

TSchedulerInputState GenerateMultiDCInputContext(
    int nodeCount,
    int writeThreadCount = 0,
    int proxyCount = 0);

void VerifyMultiDCNodeAllocationRequests(
    const TSchedulerMutations& mutations,
    THashMap<std::string, int> requestsByDataCenter);

void VerifyNodeAllocationRequests(
    const TSchedulerMutations& mutations,
    int expectedCount,
    const std::string& dataCenterName = "default");

void VerifyMultiDCProxyAllocationRequests(
    const TSchedulerMutations& mutations,
    THashMap<std::string, int> requestsByDataCenter);

void VerifyMultiDCNodeDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    THashMap<std::string, int> requestsByDataCenter);

void VerifyMultiDCProxyDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    THashMap<std::string, int> requestsByDataCenter);

struct TGenerateNodeOptions
{
    bool SetFilterTag = false;
    int SlotCount = 5;
    int InstanceIndex = 170;
    std::optional<std::string> DC = "default";
};

THashSet<std::string> GenerateNodesForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int nodeCount,
    const TGenerateNodeOptions& options = {});

THashSet<std::string> GenerateProxiesForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int proxyCount,
    bool setRole = false,
    std::string dataCenterName = "default",
    std::optional<std::string> customProxyRole = {});

void SetTabletSlotsState(
    TSchedulerInputState& inputState,
    const std::string& nodeName,
    const std::string& state);

void GenerateNodeAllocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int count,
    const std::string& dataCenterName = "default");

void GenerateProxyAllocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int count,
    const std::string& dataCenterName = "default");

void GenerateTabletCellsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int cellCount,
    int peerCount = 1);

void GenerateNodeDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    const std::vector<std::string>& nodeNames,
    const std::string& dataCenterName = "default");

void GenerateProxyDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    const std::vector<std::string>& proxyNames,
    const std::string& dataCenterName = "default");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
