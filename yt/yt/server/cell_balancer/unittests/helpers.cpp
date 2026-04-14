#include "helpers.h"

#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/cypress_bindings.h>
#include <yt/yt/server/cell_balancer/input_state.h>
#include <yt/yt/server/cell_balancer/mutations.h>
#include <yt/yt/server/cell_balancer/pod_id_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

std::string GetPodIdForInstance(const std::string& name)
{
    auto endPos = name.find(".");
    YT_VERIFY(endPos != std::string::npos);

    return name.substr(0, endPos);
}

void CheckEmptyAlerts(const TSchedulerMutations& mutations)
{
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));

    for (const auto& alert : mutations.AlertsToFire) {
        EXPECT_EQ("", alert.Id);
        EXPECT_EQ("", alert.BundleName);
        EXPECT_EQ("", alert.Description);
    }
}

void ApplyChangedStates(
    TSchedulerInputState* schedulerState,
    const TSchedulerMutations& mutations)
{
    for (const auto& [bundleName, state] : mutations.ChangedStates) {
        schedulerState->BundleStates[bundleName] = NYTree::CloneYsonStruct(state);
    }
}

int CountAlertsExcept(
    const std::vector<TAlert>& alerts,
    const THashSet<std::string>& idsToIgnore)
{
    int result = 0;

    for (const auto& alert : alerts) {
        if (idsToIgnore.contains(alert.Id)) {
            continue;
        }
        ++result;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TBundleInfoPtr SetBundleInfo(
    TSchedulerInputState& input,
    const std::string& bundleName,
    int nodeCount,
    int writeThreadCount,
    int proxyCount)
{
    auto bundleInfo = New<TBundleInfo>();
    input.Bundles[bundleName] = bundleInfo;
    bundleInfo->Health = NTabletClient::ETabletCellHealth::Good;
    bundleInfo->Zone = "default-zone";
    bundleInfo->NodeTagFilter = "default-zone/" + bundleName;
    bundleInfo->EnableBundleController = true;
    bundleInfo->EnableTabletCellManagement = true;

    bundleInfo->EnableNodeTagFilterManagement = false;
    bundleInfo->EnableTabletNodeDynamicConfig = false;
    bundleInfo->EnableRpcProxyManagement = false;
    bundleInfo->EnableSystemAccountManagement = false;
    bundleInfo->Areas["default"] = New<TBundleArea>();

    auto config = New<TBundleConfig>();
    bundleInfo->TargetConfig = config;
    config->TabletNodeCount = nodeCount;
    config->RpcProxyCount = proxyCount;
    config->TabletNodeResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    config->TabletNodeResourceGuarantee->Vcpu = 9999;
    config->TabletNodeResourceGuarantee->Memory = 88_GB;
    config->TabletNodeResourceGuarantee->NetBytes = 1_GB / 8;
    config->RpcProxyResourceGuarantee->Vcpu = 1111;
    config->RpcProxyResourceGuarantee->Memory = 18_GB;
    config->CpuLimits->WriteThreadPoolSize = writeThreadCount;

    return bundleInfo;
}

TSchedulerInputState GenerateSimpleInputContext(
    int nodeCount,
    int writeThreadCount,
    int proxyCount)
{
    TSchedulerInputState input;

    input.Config = New<TBundleControllerConfig>();
    input.Config->Cluster = "sen-tst";
    input.Config->EnableNetworkLimits = true;

    input.DynamicConfig = New<TBundleControllerDynamicConfig>();

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->DefaultYPCluster = "yp-default";
        zoneInfo->DefaultTabletNodeNannyService = "nanny-tablet-nodes-default";
        zoneInfo->DefaultRpcProxyNannyService = "nanny-rpc-proxies-default";
        zoneInfo->RequiresMinusOneRackGuarantee = false;

        auto defaultTabNode = New<NBundleControllerClient::TInstanceSize>();
        defaultTabNode->ResourceGuarantee->Vcpu = 9999;
        defaultTabNode->ResourceGuarantee->Memory = 88_GB;
        defaultTabNode->ResourceGuarantee->NetBytes = 1_GB / 8;
        zoneInfo->TabletNodeSizes["default"] = defaultTabNode;
    }

    SetBundleInfo(input, "bigd", nodeCount, writeThreadCount, proxyCount);

    return input;
}

TSchedulerInputState GenerateMultiDCInputContext(
    int nodeCount,
    int writeThreadCount,
    int proxyCount)
{
    TSchedulerInputState input;

    input.Config = New<TBundleControllerConfig>();
    input.Config->Cluster = "sen-tst";
    input.Config->EnableNetworkLimits = true;

    input.DynamicConfig = New<TBundleControllerDynamicConfig>();

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->RequiresMinusOneRackGuarantee = false;
        zoneInfo->RedundantDataCenterCount = 1;
        zoneInfo->MaxTabletNodeCount = 100;
        zoneInfo->MaxRpcProxyCount = 100;

        auto defaultTabNode = New<NBundleControllerClient::TInstanceSize>();
        defaultTabNode->ResourceGuarantee->Vcpu = 9999;
        defaultTabNode->ResourceGuarantee->Memory = 88_GB;
        defaultTabNode->ResourceGuarantee->NetBytes = 1_GB / 8;
        zoneInfo->TabletNodeSizes["default"] = defaultTabNode;

        for (int i = 1; i <= 3; ++i) {
            auto dc = New<TDataCenterInfo>();
            auto dataCenterName = Format("dc-%v", i);
            dc->YPCluster = Format("yp-%v", dataCenterName);
            dc->TabletNodeNannyService = Format("nanny-tablet-nodes-%v", dataCenterName);
            dc->RpcProxyNannyService = Format("nanny-rpc-proxies-%v", dataCenterName);

            zoneInfo->DataCenters[dataCenterName] = std::move(dc);
        }
    }

    SetBundleInfo(input, "bigd", nodeCount, writeThreadCount, proxyCount);
    return input;
}

void VerifyMultiDCNodeAllocationRequests(
    const TSchedulerMutations& mutations,
    THashMap<std::string, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());
        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_TRUE(spec->YPCluster.starts_with("yp-"));

        std::string dataCenterName = spec->YPCluster.substr(3);
        --requestsByDataCenter[dataCenterName];

        EXPECT_EQ(spec->NannyService, Format("nanny-tablet-nodes-%v", dataCenterName));
        EXPECT_FALSE(spec->PodIdTemplate.empty());
        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);
        EXPECT_EQ(spec->ResourceRequest->Vcpu, 9999);
        EXPECT_EQ(spec->ResourceRequest->MemoryMb, static_cast<i64>(88_GB / 1_MB));
        EXPECT_EQ(spec->ResourceRequest->NetworkBandwidth, std::optional<i64>(1_GB / 8));
    }

    for (const auto& [dataCenterName, count] : requestsByDataCenter) {
        EXPECT_EQ(count, 0) << "Unexpected allocations count for DataCenter:" << dataCenterName;
    }
}

void VerifyNodeAllocationRequests(
    const TSchedulerMutations& mutations,
    int expectedCount,
    const std::string& dataCenterName)
{
    VerifyMultiDCNodeAllocationRequests(mutations, {{dataCenterName, expectedCount}});
}

void VerifyMultiDCProxyAllocationRequests(
    const TSchedulerMutations& mutations,
    THashMap<std::string, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        EXPECT_TRUE(spec->YPCluster.starts_with("yp-"));
        std::string dataCenterName = spec->YPCluster.substr(3);
        --requestsByDataCenter[dataCenterName];

        EXPECT_EQ(spec->NannyService, Format("nanny-rpc-proxies-%v", dataCenterName));
        EXPECT_FALSE(spec->PodIdTemplate.empty());
        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeRpcProxy);
        EXPECT_EQ(spec->ResourceRequest->Vcpu, 1111);
        EXPECT_EQ(spec->ResourceRequest->MemoryMb, static_cast<i64>(18_GB / 1_MB));
        EXPECT_EQ(spec->ResourceRequest->NetworkBandwidth, std::optional<i64>());
    }

    for (const auto& [dataCenterName, count] : requestsByDataCenter) {
        EXPECT_EQ(count, 0) << "Unexpected proxy allocations count for DataCenter:" << dataCenterName;
    }
}

void VerifyMultiDCNodeDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    THashMap<std::string, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        std::string dataCenterName = spec->YPCluster.substr(3);
        --requestsByDataCenter[dataCenterName];

        EXPECT_EQ(spec->YPCluster, Format("yp-%v", dataCenterName));

        EXPECT_FALSE(spec->PodId.empty());

        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);

        const auto& deallocationState = bundleState->NodeDeallocations[id];

        EXPECT_FALSE(deallocationState->InstanceName.empty());
        EXPECT_EQ(deallocationState->Strategy, DeallocationStrategyHulkRequest);
    }

    for (const auto& [dataCenterName, count] : requestsByDataCenter) {
        EXPECT_EQ(count, 0) << "Unexpected deallocations count for DataCenter:" << dataCenterName;
    }
}

void VerifyMultiDCProxyDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    THashMap<std::string, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        std::string dataCenterName = spec->YPCluster.substr(3);
        --requestsByDataCenter[dataCenterName];

        EXPECT_EQ(spec->YPCluster, Format("yp-%v", dataCenterName));
        EXPECT_FALSE(spec->PodId.empty());

        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeRpcProxy);

        const auto& deallocationState = bundleState->ProxyDeallocations[id];
        EXPECT_FALSE(deallocationState->InstanceName.empty());
        EXPECT_EQ(deallocationState->Strategy, DeallocationStrategyHulkRequest);
    }

    for (const auto& [dataCenterName, count] : requestsByDataCenter) {
        EXPECT_EQ(count, 0) << "Unexpected proxy deallocations count for DataCenter:" << dataCenterName;
    }
}

THashSet<std::string> GenerateNodesForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int nodeCount,
    const TGenerateNodeOptions& options)
{
    THashSet<std::string> result;

    const auto& zoneInfo = inputState.Zones.begin()->second;
    const auto& targetConfig = bundleName == SpareBundleName
        ? zoneInfo->SpareTargetConfig
        : GetOrCrash(inputState.Bundles, bundleName)->TargetConfig;

    for (int index = 0; index < nodeCount; ++index) {
        int nodeIndex = std::ssize(inputState.TabletNodes);
        auto nodeId = Format("seneca-ayt-%v-%v-%03x-tab-%v.%v.yandex.net",
            nodeIndex,
            bundleName,
            options.InstanceIndex + index,
            inputState.Config->Cluster,
            options.DC);
        auto nodeInfo = New<TTabletNodeInfo>();
        nodeInfo->Banned = false;
        nodeInfo->Decommissioned = false;
        nodeInfo->Host = Format("seneca-ayt-%v.%v.yandex.net", nodeIndex, options.DC);
        nodeInfo->State = InstanceStateOnline;
        nodeInfo->BundleControllerAnnotations->Allocated = true;
        nodeInfo->BundleControllerAnnotations->NannyService = Format("nanny-tablet-nodes-%v", options.DC);
        nodeInfo->BundleControllerAnnotations->YPCluster = Format("yp-%v", options.DC);
        nodeInfo->BundleControllerAnnotations->DataCenter = options.DC;
        nodeInfo->BundleControllerAnnotations->AllocatedForBundle = bundleName;
        nodeInfo->BundleControllerAnnotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        nodeInfo->BundleControllerAnnotations->Resource = CloneYsonStruct(targetConfig->TabletNodeResourceGuarantee);

        for (int index = 0; index < options.SlotCount; ++index) {
            nodeInfo->TabletSlots.push_back(New<TTabletSlot>());
        }

        if (options.SetFilterTag) {
            nodeInfo->UserTags.insert(GetOrCrash(inputState.Bundles, bundleName)->NodeTagFilter);
        }

        inputState.TabletNodes[nodeId] = nodeInfo;
        result.insert(nodeId);
    }

    return result;
}

THashSet<std::string> GenerateProxiesForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int proxyCount,
    bool setRole,
    std::string dataCenterName,
    std::optional<std::string> customProxyRole)
{
    THashSet<std::string> result;

    const auto& zoneInfo = inputState.Zones.begin()->second;
    const auto& targetConfig = (bundleName == SpareBundleName)
        ? zoneInfo->SpareTargetConfig
        : GetOrCrash(inputState.Bundles, bundleName)->TargetConfig;

    for (int index = 0; index < proxyCount; ++index) {
        int proxyIndex = std::ssize(inputState.RpcProxies);
        auto proxyName = Format("seneca-ayt-%v-%v-aa-proxy-%v.%v.yandex.net",
            proxyIndex,
            bundleName,
            inputState.Config->Cluster,
            dataCenterName);
        auto proxyInfo = New<TRpcProxyInfo>();
        proxyInfo->Alive = New<TRpcProxyAlive>();
        proxyInfo->BundleControllerAnnotations->Allocated = true;
        proxyInfo->BundleControllerAnnotations->NannyService = Format("nanny-rpc-proxies-%v", dataCenterName);
        proxyInfo->BundleControllerAnnotations->YPCluster = Format("yp-%v", dataCenterName);
        proxyInfo->BundleControllerAnnotations->AllocatedForBundle = bundleName;
        proxyInfo->BundleControllerAnnotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        proxyInfo->BundleControllerAnnotations->Resource = CloneYsonStruct(targetConfig->RpcProxyResourceGuarantee);
        proxyInfo->BundleControllerAnnotations->DataCenter = dataCenterName;

        if (setRole) {
            auto& bundleInfo = GetOrCrash(inputState.Bundles, bundleName);
            std::string role = bundleInfo->RpcProxyRole ? *bundleInfo->RpcProxyRole : bundleName;
            proxyInfo->Role = customProxyRole.value_or(role);
        }

        inputState.RpcProxies[proxyName] = std::move(proxyInfo);
        result.insert(proxyName);
    }

    return result;
}

void SetTabletSlotsState(
    TSchedulerInputState& inputState,
    const std::string& nodeName,
    const std::string& state)
{
    const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);

    for (auto& slot : nodeInfo->TabletSlots) {
        slot = New<TTabletSlot>();
        slot->State = state;
    }
}

void GenerateNodeAllocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int count,
    const std::string& dataCenterName)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (int index = 0; index < count; ++index) {
        auto podIdTemplate = GetInstancePodIdTemplate(inputState.Config->Cluster, bundleName, "tab", index + 1);
        auto requestId = Format("alloc-%v", state->NodeAllocations.size());

        auto requestState = New<TAllocationRequestState>();
        requestState->CreationTime = TInstant::Now();
        requestState->PodIdTemplate = podIdTemplate;
        requestState->DataCenter = dataCenterName;
        state->NodeAllocations[requestId] = requestState;

        inputState.AllocationRequests[requestId] = New<TAllocationRequest>();
        auto& spec = inputState.AllocationRequests[requestId]->Spec;
        spec->NannyService = Format("nanny-tablet-nodes-%v", dataCenterName);
        spec->YPCluster = Format("yp-%v", dataCenterName);
        spec->ResourceRequest->Vcpu = 9999;
        spec->ResourceRequest->MemoryMb = 88_GB / 1_MB;
        spec->ResourceRequest->NetworkBandwidth = 1_GB / 8;
        spec->PodIdTemplate = podIdTemplate;
    }
}

void GenerateProxyAllocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int count,
    const std::string& dataCenterName)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (int index = 0; index < count; ++index) {
        auto podIdTemplate = GetInstancePodIdTemplate(inputState.Config->Cluster, bundleName, "rpc", index);

        auto requestId = Format("proxy-alloc-%v", state->ProxyAllocations.size());

        auto requestState = New<TAllocationRequestState>();
        requestState->CreationTime = TInstant::Now();
        requestState->PodIdTemplate = podIdTemplate;
        requestState->DataCenter = dataCenterName;
        state->ProxyAllocations[requestId] = requestState;

        inputState.AllocationRequests[requestId] = New<TAllocationRequest>();
        auto& spec = inputState.AllocationRequests[requestId]->Spec;
        spec->NannyService = Format("nanny-rpc-proxies-%v", dataCenterName);
        spec->YPCluster = Format("yp-%v", dataCenterName);
        spec->ResourceRequest->Vcpu = 1111;
        spec->ResourceRequest->MemoryMb = 18_GB / 1_MB;
        spec->PodIdTemplate = podIdTemplate;
    }
}

void GenerateTabletCellsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    int cellCount,
    int peerCount)
{
    auto bundleInfo = GetOrCrash(inputState.Bundles, bundleName);

    for (int index = 0; index < cellCount; ++index) {
        auto cellId = Format("tablet-cell-%v-%v", bundleName, bundleInfo->TabletCellIds.size());
        auto cellInfo = New<TTabletCellInfo>();
        cellInfo->Peers.resize(peerCount, New<TTabletCellPeer>());
        bundleInfo->TabletCellIds.push_back(cellId);
        inputState.TabletCells[cellId] = cellInfo;
    }
}

void GenerateNodeDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    const std::vector<std::string>& nodeNames,
    const std::string& dataCenterName)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (const auto& nodeName : nodeNames) {
        const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);
        nodeInfo->Decommissioned = true;
        SetTabletSlotsState(inputState, nodeName, TabletSlotStateEmpty);

        auto requestId = Format("dealloc-%v", state->NodeDeallocations.size());

        auto deallocationState = New<TDeallocationRequestState>();
        state->NodeDeallocations[requestId] = deallocationState;
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->InstanceName = nodeName;
        deallocationState->Strategy = DeallocationStrategyHulkRequest;
        deallocationState->HulkRequestCreated = true;
        deallocationState->DataCenter = dataCenterName;

        inputState.DeallocationRequests[requestId] = New<TDeallocationRequest>();
        auto& spec = inputState.DeallocationRequests[requestId]->Spec;
        spec->YPCluster = Format("yp-%v", dataCenterName);
        spec->PodId = "random_pod_id";
    }
}

void GenerateProxyDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const std::string& bundleName,
    const std::vector<std::string>& proxyNames,
    const std::string& dataCenterName)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (const auto& proxyName : proxyNames) {
        auto requestId = Format("proxy-dealloc-%v", state->ProxyDeallocations.size());

        auto deallocationState = New<TDeallocationRequestState>();
        state->ProxyDeallocations[requestId] = deallocationState;
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->InstanceName = proxyName;
        deallocationState->HulkRequestCreated = true;
        deallocationState->Strategy = DeallocationStrategyHulkRequest;

        inputState.DeallocationRequests[requestId] = New<TDeallocationRequest>();
        auto& spec = inputState.DeallocationRequests[requestId]->Spec;
        spec->YPCluster = Format("yp-%v", dataCenterName);
        spec->PodId = "random_pod_id";
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
