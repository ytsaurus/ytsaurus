#include <optional>

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/orchid_bindings.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/new.h>

#include <util/random/shuffle.h>

namespace NYT::NCellBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString SpareBundleName = "spare";
constexpr int DefaultNodeCount = 0;
constexpr int DefaultCellCount = 0;
constexpr bool SetNodeTagFilters = true;

////////////////////////////////////////////////////////////////////////////////

TString GetPodIdForInstance(const TString& name)
{
    auto endPos = name.find(".");
    YT_VERIFY(endPos != TString::npos);

    return name.substr(0, endPos);
}

////////////////////////////////////////////////////////////////////////////////

void CheckEmptyAlerts(const TSchedulerMutations& mutations)
{
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));

    for (const auto& alert : mutations.AlertsToFire) {
        EXPECT_EQ("", alert.Id);
        EXPECT_EQ("", alert.BundleName);
        EXPECT_EQ("", alert.Description);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ApplyChangedStates(TSchedulerInputState* schedulerState, const TSchedulerMutations& mutations)
{
    for (const auto& [bundleName, state] : mutations.ChangedStates) {
        schedulerState->BundleStates[bundleName] = NYTree::CloneYsonStruct(state);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool Contains(const auto& container, const auto& item)
{
    return std::find(container.begin(), container.end(), item) != container.end();
}

////////////////////////////////////////////////////////////////////////////////

TBundleInfoPtr SetBundleInfo(
    TSchedulerInputState& input,
    const TString& bundleName,
    int nodeCount,
    int writeThreadCount = 0,
    int proxyCount = 0)
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

    auto config = New<TBundleConfig>();
    bundleInfo->TargetConfig = config;
    config->TabletNodeCount = nodeCount;
    config->RpcProxyCount = proxyCount;
    config->TabletNodeResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    config->TabletNodeResourceGuarantee->Vcpu = 9999;
    config->TabletNodeResourceGuarantee->Memory = 88_GB;
    config->TabletNodeResourceGuarantee->Net = 1_GB;
    config->RpcProxyResourceGuarantee->Vcpu = 1111;
    config->RpcProxyResourceGuarantee->Memory = 18_GB;
    config->CpuLimits->WriteThreadPoolSize = writeThreadCount;

    return bundleInfo;
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerInputState GenerateSimpleInputContext(int nodeCount, int writeThreadCount = 0, int proxyCount = 0)
{
    TSchedulerInputState input;
    input.Config = New<TBundleControllerConfig>();
    input.Config->Cluster = "sen-tst";
    input.Config->EnableNetworkLimits = true;

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->DefaultYPCluster = "yp-default";
        zoneInfo->DefaultTabletNodeNannyService = "nanny-tablet-nodes-default";
        zoneInfo->DefaultRpcProxyNannyService = "nanny-rpc-proxies-default";
        zoneInfo->RequiresMinusOneRackGuarantee = false;
    }

    SetBundleInfo(input, "bigd", nodeCount, writeThreadCount, proxyCount);

    return input;
}

TSchedulerInputState GenerateMultiDCInputContext(int nodeCount, int writeThreadCount = 0, int proxyCount = 0)
{
    TSchedulerInputState input;
    input.Config = New<TBundleControllerConfig>();
    input.Config->Cluster = "sen-tst";
    input.Config->EnableNetworkLimits = true;

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->RequiresMinusOneRackGuarantee = false;
        zoneInfo->RedundantDataCenterCount = 1;
        zoneInfo->MaxTabletNodeCount = 100;
        zoneInfo->MaxRpcProxyCount = 100;

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
    THashMap<TString, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());
        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_TRUE(spec->YPCluster.StartsWith("yp-"));

        TString dataCenterName = spec->YPCluster.substr(3);
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

void VerifyNodeAllocationRequests(const TSchedulerMutations& mutations, int expectedCount, const TString& dataCenterName = "default")
{
    VerifyMultiDCNodeAllocationRequests(mutations, {{dataCenterName, expectedCount}});
}

void VerifyMultiDCProxyAllocationRequests(
    const TSchedulerMutations& mutations,
    THashMap<TString, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        EXPECT_TRUE(spec->YPCluster.StartsWith("yp-"));
        TString dataCenterName = spec->YPCluster.substr(3);
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
    THashMap<TString, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        TString dataCenterName = spec->YPCluster.substr(3);
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
    THashMap<TString, int> requestsByDataCenter)
{
    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));

        TString dataCenterName = spec->YPCluster.substr(3);
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

struct TGenerateNodeOptions
{
    bool SetFilterTag = false;
    int SlotCount = 5;
    int InstanceIndex = 170;
    TString DC = "default";
};

THashSet<TString> GenerateNodesForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int nodeCount,
    const TGenerateNodeOptions& options = {})
{
    THashSet<TString> result;

    const auto& zoneInfo = inputState.Zones.begin()->second;
    const auto& targetConfig = (bundleName == SpareBundleName)
        ? zoneInfo->SpareTargetConfig
        : GetOrCrash(inputState.Bundles, bundleName)->TargetConfig;

    for (int index = 0; index < nodeCount; ++index) {
        int nodeIndex = std::ssize(inputState.TabletNodes);
        auto nodeId = Format("seneca-ayt-%v-%v-%v-tab-%v.%v.yandex.net",
            nodeIndex,
            bundleName,
            options.InstanceIndex + index,
            inputState.Config->Cluster,
            options.DC);
        auto nodeInfo = New<TTabletNodeInfo>();
        nodeInfo->Banned = false;
        nodeInfo->Decommissioned = false;
        nodeInfo->Host = Format("seneca-ayt-%v.%v.yandex.net", nodeIndex, options.DC);
        nodeInfo->State = "online";
        nodeInfo->Annotations->Allocated = true;
        nodeInfo->Annotations->NannyService = Format("nanny-tablet-nodes-%v", options.DC);
        nodeInfo->Annotations->YPCluster = Format("yp-%v", options.DC);
        nodeInfo->Annotations->DataCenter = options.DC;
        nodeInfo->Annotations->AllocatedForBundle = bundleName;
        nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        nodeInfo->Annotations->Resource = CloneYsonStruct(targetConfig->TabletNodeResourceGuarantee);

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

THashSet<TString> GenerateProxiesForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int proxyCount,
    bool setRole = false,
    TString dataCenterName = "default")
{
    THashSet<TString> result;

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
        proxyInfo->Annotations->Allocated = true;
        proxyInfo->Annotations->NannyService = Format("nanny-rpc-proxies-%v", dataCenterName);
        proxyInfo->Annotations->YPCluster = Format("yp-%v", dataCenterName);
        proxyInfo->Annotations->AllocatedForBundle = bundleName;
        proxyInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        proxyInfo->Annotations->Resource = CloneYsonStruct(targetConfig->RpcProxyResourceGuarantee);
        proxyInfo->Annotations->DataCenter = dataCenterName;

        if (setRole) {
            auto& bundleInfo = GetOrCrash(inputState.Bundles, bundleName);
            TString role = bundleInfo->RpcProxyRole ? *bundleInfo->RpcProxyRole : bundleName;
            proxyInfo->Role = role;
        }

        inputState.RpcProxies[proxyName] = std::move(proxyInfo);
        result.insert(proxyName);
    }

    return result;
}

void SetTabletSlotsState(TSchedulerInputState& inputState, const TString& nodeName, const TString& state)
{
    const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);

    for (auto& slot : nodeInfo->TabletSlots) {
        slot = New<TTabletSlot>();
        slot->State = state;
    }
}

void GenerateNodeAllocationsForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int count,
    const TString& dataCenterName = "default")
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
    const TString& bundleName,
    int count,
    const TString& dataCenterName = "default")
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
    const TString& bundleName,
    int cellCount,
    int peerCount = 1)
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
    const TString& bundleName,
    const std::vector<TString>& nodeNames,
    const TString& dataCenterName = "default")
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
    const TString& bundleName,
    const std::vector<TString>& proxyNames,
    const TString& dataCenterName = "default")
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

THashSet<TString> GetRandomElements(const auto& collection, int count)
{
    std::vector<TString> result(collection.begin(), collection.end());
    Shuffle(result.begin(), result.end());
    result.resize(count);
    return {result.begin(), result.end()};
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EZoneSetup,
    (Simple)
    (MultiCluster)
);

class TBundleSchedulerTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<EZoneSetup, int>>
{
protected:
    TSchedulerInputState GenerateInputContext(int nodeCount, int writeThreadCount = 0, int proxyCount = 0)
    {
        auto setup = std::get<0>(GetParam());

        if (setup == EZoneSetup::Simple) {
            return GenerateSimpleInputContext(nodeCount, writeThreadCount, proxyCount);
        } else {
            return GenerateMultiDCInputContext(nodeCount, writeThreadCount, proxyCount);
        }
    }

    int GetDataCenterCount()
    {
        return std::get<1>(GetParam());
    }

    int GetActiveDataCenterCount()
    {
        auto setup = std::get<0>(GetParam());

        if (setup == EZoneSetup::Simple) {
            return GetDataCenterCount();
        } else {
            return GetDataCenterCount() - 1;
        }
    }

    std::vector<TString> GetDataCenters(const TSchedulerInputState& inputContext)
    {
        auto setup = std::get<0>(GetParam());

        if (setup == EZoneSetup::Simple) {
            return { "default" };
        }

        std::vector<TString> result;

        const auto& dataCenters = inputContext.Zones.begin()->second->DataCenters;
        for (const auto& [dataCenterName, _] : dataCenters) {
            result.push_back(dataCenterName);
        }

        return result;
    }

    THashMap<TString, int> ForEachDataCenter(const TSchedulerInputState& inputContext, int count)
    {
        THashMap<TString, int> expected;
        for (const TString& dataCenter : GetDataCenters(inputContext)) {
            expected[dataCenter] = count;
        }

        return expected;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTagsFilterManager
    : public TBundleSchedulerTest
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, AllocationCreated)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = false, .SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
        GenerateNodeAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 3));

    EXPECT_EQ(GetDataCenterCount() * 4, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");

    THashMap<TString, THashSet<TString>> templates;
    for (auto& [allocId, request] : mutations.NewAllocations) {
        templates[request->Spec->YPCluster].insert(request->Spec->PodIdTemplate);

        auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingTabletNodes, allocId);
        EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
        EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "REQUEST_CREATED");
        EXPECT_FALSE(orchidAllocatingInfo->InstanceInfo);
    }

    EXPECT_EQ(std::ssize(templates), GetDataCenterCount());

    for (const auto& [_, dcTemplates] : templates) {
        EXPECT_GE(std::ssize(dcTemplates), 3);
        EXPECT_TRUE(dcTemplates.count(GetInstancePodIdTemplate(input.Config->Cluster, "bigd", "tab", 3)));
        EXPECT_TRUE(dcTemplates.count(GetInstancePodIdTemplate(input.Config->Cluster, "bigd", "tab", 4)));
        EXPECT_TRUE(dcTemplates.count(GetInstancePodIdTemplate(input.Config->Cluster, "bigd", "tab", 5)));
    }
}

TEST_P(TBundleSchedulerTest, AllocationsAreDisabled)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableInstanceAllocation = false;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 0));
}

TEST_P(TBundleSchedulerTest, InitializeTargetConfig)
{
    auto input = GenerateInputContext(0);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    bundleInfo->EnableTabletNodeDynamicConfig = true;
    bundleInfo->EnableRpcProxyManagement = true;
    bundleInfo->EnableSystemAccountManagement = true;

    EXPECT_EQ(0, std::ssize(mutations.InitializedBundleTargetConfig));

    bundleInfo->TargetConfig = {};
    mutations = TSchedulerMutations{};
    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

    ScheduleBundles(input, &mutations);

    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));
    EXPECT_EQ(1, std::ssize(mutations.InitializedBundleTargetConfig));
}

TEST_P(TBundleSchedulerTest, AllocationQuotaExceeded)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());

    {
        auto& bundleInfo = input.Bundles["bigd"];
        bundleInfo->ResourceQuota = New<TResourceQuota>();
        bundleInfo->ResourceQuota->Cpu = 0.1;
        bundleInfo->ResourceQuota->Memory = 10_TB;

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(mutations.AlertsToFire.front().Id, "bundle_resource_quota_exceeded");
    }

    {
        auto& bundleInfo = input.Bundles["bigd"];
        bundleInfo->ResourceQuota->Cpu = 100;
        bundleInfo->ResourceQuota->Memory = 1;

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(mutations.AlertsToFire.front().Id, "bundle_resource_quota_exceeded");
    }

    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->ResourceQuota->Cpu = 1000;
    bundleInfo->ResourceQuota->Memory = 10_TB;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 5));

    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
}

TEST_P(TBundleSchedulerTest, AllocationProgressTrackCompleted)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    const TString bundleName = "bigd";

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 2, {.DC = dataCenter,});
    }

    const TString dataCenterName = dataCenters.front();

    // To get bundle to node mapping
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);
    }

    GenerateNodeAllocationsForBundle(input, bundleName, 1, dataCenterName);

    const TString nodeId = input.BundleNodes[bundleName].at(dataCenterName).front();

    GetOrCrash(input.TabletNodes, nodeId)->Annotations = New<TInstanceAnnotations>();

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = GetOrCrash(input.TabletNodes, nodeId)->Host;
        status->PodId = GetPodIdForInstance(nodeId);
        status->State = "COMPLETED";
    }

    // Check Setting node attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        VerifyNodeAllocationRequests(mutations, 0);
        EXPECT_EQ(1, std::ssize(input.BundleStates["bigd"]->NodeAllocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangeNodeAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
        EXPECT_EQ(annotations->YPCluster, Format("yp-%v", dataCenterName));
        EXPECT_EQ(annotations->DataCenter, dataCenterName);
        EXPECT_EQ(annotations->AllocatedForBundle, "bigd");
        EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
        EXPECT_EQ(annotations->NannyService, Format("nanny-tablet-nodes-%v", dataCenterName));
        EXPECT_EQ(annotations->Resource->Vcpu, 9999);
        EXPECT_EQ(annotations->Resource->Memory, static_cast<i64>(88_GB));
        EXPECT_EQ(annotations->Resource->Net, std::optional<i64>(1_GB));
        EXPECT_TRUE(annotations->Allocated);
        EXPECT_FALSE(annotations->DeallocatedAt);

        auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");
        for (auto& [allocId, allocState] : input.BundleStates["bigd"]->NodeAllocations) {
            auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingTabletNodes, allocId);
            EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
            EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "COMPLETED");
            EXPECT_TRUE(orchidAllocatingInfo->InstanceInfo);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->YPCluster, Format("yp-%v", dataCenterName));
            EXPECT_FALSE(orchidAllocatingInfo->InstanceInfo->PodId.empty());
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->Resource->Vcpu, 9999);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->Resource->Memory, static_cast<i64>(88_GB));
        }

        input.TabletNodes[nodeId]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

TEST_P(TBundleSchedulerTest, AllocationProgressTrackFailed)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount());
    const TString bundleName = "bigd";
    auto dataCenters = GetDataCenters(input);

    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 2, {.DC = dataCenter});
        GenerateNodeAllocationsForBundle(input, bundleName, 1, dataCenter);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(mutations.ChangedStates.count("bigd"), 0u);
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_allocation_failed");
}

TEST_P(TBundleSchedulerTest, AllocationProgressTrackCompletedButNoNode)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount());
    const TString bundleName = "bigd";
    auto dataCenters = GetDataCenters(input);

    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 2, {.DC = dataCenter});
        GenerateNodeAllocationsForBundle(input, bundleName, 1, dataCenter);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(GetDataCenterCount(), std::ssize(input.BundleStates[bundleName]->NodeAllocations));
}

TEST_P(TBundleSchedulerTest, AllocationProgressTrackStaledAllocation)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    const TString bundleName = "bigd";

    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 2, {.DC = dataCenter});
        GenerateNodeAllocationsForBundle(input, bundleName, 1, dataCenter);
    }

    for (auto& [_, allocState] : input.BundleStates[bundleName]->NodeAllocations) {
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_allocation");
}

TEST_P(TBundleSchedulerTest, DoNotCreateNewDeallocationsWhileInProgress)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), DefaultCellCount);
    auto dataCenters = GetDataCenters(input);
    const TString bundleName = "bigd";

    for (const auto& dataCenter : dataCenters) {
        auto nodes = GenerateNodesForBundle(input, bundleName, 5, {
            .SetFilterTag = SetNodeTagFilters,
            .SlotCount = DefaultCellCount,
            .DC = dataCenter});
        GenerateNodeDeallocationsForBundle(input, bundleName, { *nodes.begin()}, dataCenter);
    }

    EXPECT_EQ(GetDataCenterCount(), std::ssize(input.BundleStates[bundleName]->NodeDeallocations));
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count(bundleName));
}

TEST_P(TBundleSchedulerTest, DoNotCreateNewDeallocationsIfSomeNodesAreNotReady)
{
    constexpr int TabletSlotCount = 10;

    auto input = GenerateInputContext(2 * GetDataCenterCount(), TabletSlotCount);
    auto dataCenters = GetDataCenters(input);

    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;

    GenerateTabletCellsForBundle(input, "bigd", 2 * TabletSlotCount * GetActiveDataCenterCount());

    // Do not deallocate nodes if node tag filter is not set for all alive nodes
    for (const auto& dataCenter : dataCenters) {
        auto nodes = GenerateNodesForBundle(input, "bigd", 5, {.SetFilterTag = !SetNodeTagFilters, .SlotCount = TabletSlotCount, .DC = dataCenter});

        auto intactNodes = GetRandomElements(nodes, 1);
        for (const auto& nodeName : intactNodes) {
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->UserTags = { bundleInfo->NodeTagFilter };
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));

    // Do not deallocate nodes if cell cout is not actual for all the nodes
    input.TabletNodes.clear();
    for (const auto& dataCenter : dataCenters) {
        auto nodes = GenerateNodesForBundle(input, "bigd", 5, {.SetFilterTag = SetNodeTagFilters, .SlotCount = TabletSlotCount / 2, .DC = dataCenter});

        auto intactNodes = GetRandomElements(nodes, 1);
        for (const auto& nodeName : intactNodes) {
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->TabletSlots.resize(TabletSlotCount);
            SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
        }
    }
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));

    // Finally init deallocations if nodes are up to date.
    input.TabletNodes.clear();
    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SetFilterTag = SetNodeTagFilters, .SlotCount = TabletSlotCount, .DC = dataCenter});
    }
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));
}

TEST_P(TBundleSchedulerTest, CreateNewDeallocationsIfAllNodesAreNotReady)
{
    constexpr int TabletSlotCount = 10;

    auto input = GenerateInputContext(2 * GetDataCenterCount(), TabletSlotCount);
    auto dataCenters = GetDataCenters(input);

    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;

    GenerateTabletCellsForBundle(input, "bigd", 2 * TabletSlotCount * GetActiveDataCenterCount());

    // Do not deallocate nodes if node tag filter is not set for all alive nodes
    for (const auto& dataCenter : dataCenters) {
        auto nodes = GenerateNodesForBundle(input, "bigd", 5, {.SetFilterTag = !SetNodeTagFilters, .SlotCount = TabletSlotCount, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));
}

TEST_P(TBundleSchedulerTest, CreateNewDeallocations)
{
    constexpr int TabletSlotCount = 10;

    auto input = GenerateInputContext(2 * GetDataCenterCount(), TabletSlotCount);
    auto dataCenters = GetDataCenters(input);
    const TString bundleName = "bigd";

    GenerateTabletCellsForBundle(input, bundleName, 2 * TabletSlotCount * GetActiveDataCenterCount());

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 5, {.SetFilterTag = SetNodeTagFilters, .SlotCount = TabletSlotCount, .DC = dataCenter});
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedStates[bundleName]->NodeDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), bundleName);

    for (auto& [nodeName, state] : mutations.ChangedStates[bundleName]->NodeDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);
        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedTabletNodes, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));

    std::vector<TString> nodesToRemove;
    for (auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        GetOrCrash(input.TabletNodes, nodeName)->Decommissioned = decommissioned;
        EXPECT_TRUE(decommissioned);
        nodesToRemove.push_back(nodeName);

        SetTabletSlotsState(input, nodeName, PeerStateLeading);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Node are decommissioned but tablet slots have to be empty.
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));

    for (const auto& nodeName : nodesToRemove) {
        SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are finally created.
    auto& bundleState = mutations.ChangedStates[bundleName];
    VerifyMultiDCNodeDeallocationRequests(mutations, bundleState, ForEachDataCenter(input, 3));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));

    for (auto& [_, state] : mutations.ChangedStates[bundleName]->NodeDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);
    }
}

TEST_P(TBundleSchedulerTest, DeallocationsAreDisabled)
{
    constexpr int TabletSlotCount = 10;

    auto input = GenerateInputContext(2 * GetDataCenterCount(), TabletSlotCount);
    auto dataCenters = GetDataCenters(input);
    const TString bundleName = "bigd";
    const auto& bundleInfo = input.Bundles[bundleName];
    bundleInfo->EnableInstanceAllocation = false;

    GenerateTabletCellsForBundle(input, bundleName, 2 * TabletSlotCount * GetActiveDataCenterCount());

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, bundleName, 5, {.SetFilterTag = SetNodeTagFilters, .SlotCount = TabletSlotCount, .DC = dataCenter});
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates[bundleName]->NodeDeallocations));
}

TEST_P(TBundleSchedulerTest, DeallocationProgressTrackFailed)
{
    auto input = GenerateInputContext(GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    const TString bundleName = "bigd";

    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        auto bundleNodes = GenerateNodesForBundle(input, bundleName, 2, {.DC = dataCenter});
        GenerateNodeDeallocationsForBundle(input, bundleName, { *bundleNodes.begin()}, dataCenter);
    }

    for (const auto& [_, request] : input.DeallocationRequests) {
        request->Status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_deallocation_failed");
}

TEST_P(TBundleSchedulerTest, DeallocationProgressTrackCompleted)
{
    auto input = GenerateInputContext(GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> nodesToDeallocate;

    for (const auto& dataCenter : dataCenters) {
        auto bundleNodes = GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});

        const TString& nodeId = *bundleNodes.begin();
        nodesToDeallocate.insert(nodeId);

        GenerateNodeDeallocationsForBundle(input, "bigd", {nodeId});
    }

    for (const auto& [_, request] : input.DeallocationRequests) {
        auto& status = request->Status;
        status->State = "COMPLETED";
    }

    // Check Setting node attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(GetDataCenterCount(), std::ssize(input.BundleStates["bigd"]->NodeDeallocations));

        EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangeNodeAnnotations));

        for (const auto& [nodeId, annotations] : mutations.ChangeNodeAnnotations) {
            EXPECT_TRUE(annotations->YPCluster.empty());
            EXPECT_TRUE(annotations->AllocatedForBundle.empty());
            EXPECT_TRUE(annotations->NannyService.empty());
            EXPECT_FALSE(annotations->Allocated);
            EXPECT_TRUE(annotations->DeallocatedAt);
            EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
            EXPECT_TRUE(TInstant::Now() - *annotations->DeallocatedAt < TDuration::Minutes(10));
            EXPECT_EQ(nodesToDeallocate.count(nodeId), 1u);

            input.TabletNodes[nodeId]->Annotations = annotations;
        }
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));

    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

TEST_P(TBundleSchedulerTest, DeallocationProgressTrackStaledAllocation)
{
    auto input = GenerateInputContext(GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        auto bundleNodes = GenerateNodesForBundle(input, "bigd", 2 * GetDataCenterCount(), {.DC = dataCenter});
        const TString nodeId = *bundleNodes.begin();
        GenerateNodeDeallocationsForBundle(input, "bigd", {nodeId}, dataCenter);
    }

    for (const auto& [_, allocState] : input.BundleStates["bigd"]->NodeDeallocations) {
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_deallocation");
}

TEST_P(TBundleSchedulerTest, CreateNewCellsCreation)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 3);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(1, std::ssize(mutations.CellsToCreate));

    EXPECT_EQ(2 * GetActiveDataCenterCount() * 5 - 3, mutations.CellsToCreate.at("bigd"));
}

TEST_P(TBundleSchedulerTest, CreateNewCellsNoRemoveNoCreate)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", GetActiveDataCenterCount() * 2 * 5);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
}

TEST_P(TBundleSchedulerTest, CreateNewCellsRemove)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", GetActiveDataCenterCount() * 2 * 5 + 3);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(3, std::ssize(mutations.CellsToRemove));
}

TEST_P(TBundleSchedulerTest, TestSpareNodesAllocate)
{
    auto input = GenerateInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, TestSpareNodesDeallocate)
{
    auto input = GenerateInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];

    zoneInfo->SpareTargetConfig->TabletNodeCount = 2 * GetDataCenterCount();
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangedStates[SpareBundleName]->NodeDeallocations));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, CheckDisruptedState)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 4, {.DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->State = InstanceStateOffline;
    }

    ScheduleBundles(input, &mutations);

    EXPECT_TRUE(std::ssize(mutations.AlertsToFire) > 0);
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, CheckSingleDCDisruptedState)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();
    zoneInfo->MaxTabletNodeCount = 10 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 4, {.DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        if (nodeInfo->Annotations->DataCenter == dataCenters.front()) {
            nodeInfo->State = InstanceStateOffline;
        }
    }

    ScheduleBundles(input, &mutations);

    EXPECT_TRUE(std::ssize(mutations.AlertsToFire) > 0);
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    // Unaffected data centers are allocating new instancies.
    EXPECT_EQ(GetDataCenterCount() - 1, std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, CheckAllocationLimit)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 4, {.DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    zoneInfo->MaxTabletNodeCount = 5 * GetDataCenterCount();

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
}

TEST_P(TBundleSchedulerTest, CheckDynamicConfig)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableTabletNodeDynamicConfig = true;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Check that new dynamic config is set for bundles.
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_TRUE(mutations.DynamicConfig);

    input.DynamicConfig = *mutations.DynamicConfig;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config did not change.
    EXPECT_FALSE(mutations.DynamicConfig);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    input.Bundles["bigd"]->TargetConfig->CpuLimits->WriteThreadPoolSize = 212;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config is changed.
    EXPECT_TRUE(mutations.DynamicConfig);
    CheckEmptyAlerts(mutations);
}

TEST_P(TBundleSchedulerTest, CheckMediumThroughputLimits)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableTabletNodeDynamicConfig = true;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Check that new dynamic config is set for bundles.
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_TRUE(mutations.DynamicConfig);

    input.DynamicConfig = *mutations.DynamicConfig;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config did not change.
    EXPECT_FALSE(mutations.DynamicConfig);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    input.Bundles["bigd"]->TargetConfig->MediumThroughputLimits = {
        {"ssd_journals",  New<TMediumThroughputLimits>()},
    };
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config is changed.
    EXPECT_TRUE(mutations.DynamicConfig);
    CheckEmptyAlerts(mutations);
}

////////////////////////////////////////////////////////////////////////////////

struct TFooBarStruct
    : public TYsonStructAttributes<TFooBarStruct>
{
    TString Foo;
    int Bar;

    REGISTER_YSON_STRUCT(TFooBarStruct);

    static void Register(TRegistrar registrar)
    {
        RegisterAttribute(registrar, "foo", &TThis::Foo)
            .Default();
        RegisterAttribute(registrar, "bar", &TThis::Bar)
            .Default(0);
    }
};

TEST(TBundleSchedulerTest, CheckCypressBindings)
{
    EXPECT_EQ(TFooBarStruct::GetAttributes().size(), 2u);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, ProxyAllocationCreated)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 1, false, dataCenter);
        GenerateProxyAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCProxyAllocationRequests(mutations, ForEachDataCenter(input, 3));

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
    EXPECT_EQ(4 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->ProxyAllocations));
}

TEST_P(TBundleSchedulerTest, ProxyAllocationProgressTrackCompleted)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const TString& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
    }

    const TString dataCenterName = dataCenters.front();
    // To get bundle to proxy mapping
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);
    }

    GenerateProxyAllocationsForBundle(input, "bigd", 1, dataCenterName);

    const TString proxyName = input.BundleProxies["bigd"].at(dataCenterName).front();
    GetOrCrash(input.RpcProxies, proxyName)->Annotations = New<TInstanceAnnotations>();

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->PodId = GetPodIdForInstance(proxyName);
        status->State = "COMPLETED";
    }

    // Check Setting proxy attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(1, std::ssize(input.BundleStates["bigd"]->ProxyAllocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangedProxyAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangedProxyAnnotations, proxyName);
        EXPECT_EQ(annotations->YPCluster, Format("yp-%v", dataCenterName));
        EXPECT_EQ(annotations->AllocatedForBundle, "bigd");
        EXPECT_EQ(annotations->DataCenter, dataCenterName);
        EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
        EXPECT_EQ(annotations->NannyService, Format("nanny-rpc-proxies-%v", dataCenterName));
        EXPECT_EQ(annotations->Resource->Vcpu, 1111);
        EXPECT_EQ(annotations->Resource->Memory, static_cast<i64>(18_GB));
        EXPECT_TRUE(annotations->Allocated);
        EXPECT_FALSE(annotations->DeallocatedAt);

        auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");
        for (auto& [allocId, allocState] : input.BundleStates["bigd"]->ProxyAllocations) {
            auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingRpcProxies, allocId);
            EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
            EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "COMPLETED");
            EXPECT_TRUE(orchidAllocatingInfo->InstanceInfo);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->YPCluster, Format("yp-%v", dataCenterName));
            EXPECT_FALSE(orchidAllocatingInfo->InstanceInfo->PodId.empty());
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->Resource->Vcpu, 1111);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->Resource->Memory, static_cast<i64>(18_GB));
        }

        input.RpcProxies[proxyName]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->ProxyAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyAnnotations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, ProxyAllocationProgressTrackFailed)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
        GenerateProxyAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_allocation_failed");
}

TEST_P(TBundleSchedulerTest, ProxyAllocationProgressTrackCompletedButNoProxy)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
        GenerateProxyAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->PodId = "non-existing-pod";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(input.BundleStates["bigd"]->ProxyAllocations));
}

TEST_P(TBundleSchedulerTest, ProxyAllocationProgressTrackStaledAllocation)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const TString& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
        GenerateProxyAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    for (const auto& [_, allocState] : input.BundleStates["bigd"]->ProxyAllocations) {
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    for (const auto& [_, request] : input.AllocationRequests) {
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->PodId = "non-existing-pod";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_allocation");

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));
}

TEST_P(TBundleSchedulerTest, ProxyCreateNewDeallocations)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const TString& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->ProxyDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");

    for (auto& [_, state] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are created.
    auto& bundleState = mutations.ChangedStates["bigd"];
    VerifyMultiDCProxyDeallocationRequests(mutations, bundleState, ForEachDataCenter(input, 3));
    orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");

    for (auto& [_, state] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }
}

TEST_P(TBundleSchedulerTest, ProxyCreateNewDeallocationsLegacyInstancies)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 2 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Annotations->DeallocationStrategy.clear();
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->ProxyDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");

    for (auto& [_, state] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are created.
    auto& bundleState = mutations.ChangedStates["bigd"];
    VerifyMultiDCProxyDeallocationRequests(mutations, bundleState, ForEachDataCenter(input, 3));
    orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "bigd");

    for (auto& [_, state] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }
}

TEST_P(TBundleSchedulerTest, ProxyDeallocationProgressTrackFailed)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        auto bundleProxies = GenerateProxiesForBundle(input, "bigd", 1, false, dataCenter);
        GenerateProxyDeallocationsForBundle(input, "bigd", { *bundleProxies.begin()}, dataCenter);
    }

    TSchedulerMutations mutations;

    for (const auto& [_, request] : input.DeallocationRequests){
        request->Status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_deallocation_failed");
}

TEST_P(TBundleSchedulerTest, ProxyDeallocationProgressTrackCompleted)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        auto bundleProxies = GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
        const TString proxyName = *bundleProxies.begin();
        GenerateProxyDeallocationsForBundle(input, "bigd", {proxyName}, dataCenter);
    }

    for (const auto& [_, request] : input.DeallocationRequests){
        auto& status = request->Status;
        status->State = "COMPLETED";
    }

    // Check Setting proxy attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(GetDataCenterCount(), std::ssize(input.BundleStates["bigd"]->ProxyDeallocations));

        EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangedProxyAnnotations));
        for (const auto& [proxyName, annotations] : mutations.ChangedProxyAnnotations) {
            EXPECT_TRUE(annotations->YPCluster.empty());
            EXPECT_TRUE(annotations->AllocatedForBundle.empty());
            EXPECT_TRUE(annotations->NannyService.empty());
            EXPECT_FALSE(annotations->DataCenter);
            EXPECT_FALSE(annotations->Allocated);

            EXPECT_TRUE(annotations->DeallocatedAt);
            EXPECT_TRUE(TInstant::Now() - *annotations->DeallocatedAt < TDuration::Minutes(10));

            input.RpcProxies[proxyName]->Annotations = annotations;
        }
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangedProxyRole));
    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        ASSERT_EQ(role, TrashRole);
        input.RpcProxies[proxyName]->Role = role;
    }

    // Schedule one more time with annotation tags set
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->ProxyDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyAnnotations));
}

TEST_P(TBundleSchedulerTest, ProxyDeallocationProgressTrackStaledAllocation)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        auto bundleProxies = GenerateProxiesForBundle(input, "bigd", 2, false, dataCenter);
        const TString proxyName = *bundleProxies.begin();
        GenerateProxyDeallocationsForBundle(input, "bigd", {proxyName}, dataCenter);
    }

    for (const auto& [_, allocState] : input.BundleStates["bigd"]->ProxyDeallocations) {
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_deallocation");
}

TEST_P(TBundleSchedulerTest, TestSpareProxiesAllocate)
{
    auto input = GenerateInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = GetDataCenterCount();

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.NewAllocations));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangedStates[SpareBundleName]->ProxyAllocations));

    auto dataCenters = GetDataCenters(input);
    THashSet<TString> index(dataCenters.begin(), dataCenters.end());

    for (const auto& [_, state] : mutations.ChangedStates[SpareBundleName]->ProxyAllocations) {
        EXPECT_TRUE(index.erase(*state->DataCenter));
    }

    EXPECT_EQ(0, std::ssize(index));
}

TEST_P(TBundleSchedulerTest, TestSpareProxyDeallocate)
{
    auto input = GenerateInputContext(0);

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 2 * GetDataCenterCount();

    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, SpareBundleName, 3, false, dataCenter);
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.ChangedStates[SpareBundleName]->ProxyDeallocations));
}

TEST_P(TBundleSchedulerTest, CheckProxyZoneDisruptedState)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, SpareBundleName, 3, false, dataCenter);
        GenerateProxiesForBundle(input, "bigd", 4, false, dataCenter);
    }

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Alive.Reset();
    }

    ScheduleBundles(input, &mutations);

    EXPECT_TRUE(std::ssize(mutations.AlertsToFire) > 0);
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, ProxyCheckAllocationLimit)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3 * GetDataCenterCount();

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, SpareBundleName, 3, false, dataCenter);
        GenerateProxiesForBundle(input, "bigd", 4, false, dataCenter);
    }

    zoneInfo->MaxRpcProxyCount = 5 * GetDataCenterCount();

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(GetDataCenterCount(), std::ssize(mutations.AlertsToFire));
}

struct TExpectedLimits
{
    i64 Nodes = 0;
    i64 Chunks = 0;
    i64 SsdBlobs = 0;
    i64 Default = 0;
    i64 SsdJournal = 0;
};

void CheckLimits(const TExpectedLimits& limits, const TAccountResourcesPtr& resource)
{
    ASSERT_EQ(limits.Chunks, resource->ChunkCount);
    ASSERT_EQ(limits.Nodes, resource->NodeCount);
    ASSERT_EQ(limits.SsdJournal, resource->DiskSpacePerMedium["ssd_journal"]);
    ASSERT_EQ(limits.Default, resource->DiskSpacePerMedium["default"]);
    ASSERT_EQ(limits.SsdBlobs, resource->DiskSpacePerMedium["ssd_blobs"]);
}

TEST(TBundleSchedulerSimpleTest, CheckSystemAccountLimit)
{
    auto input = GenerateSimpleInputContext(2, 5);

    input.RootSystemAccount = New<TSystemAccount>();
    auto& bundleInfo1 = input.Bundles["bigd"];

    bundleInfo1->Options->ChangelogAccount = "default-bundle-account";
    bundleInfo1->Options->SnapshotAccount = "default-bundle-account";
    bundleInfo1->Options->ChangelogPrimaryMedium = "ssd_journal";
    bundleInfo1->Options->SnapshotPrimaryMedium = "default";
    bundleInfo1->EnableSystemAccountManagement = true;

    input.SystemAccounts["default-bundle-account"] = New<TSystemAccount>();

    bundleInfo1->SystemAccountQuotaMultiplier = 1.5;
    input.Config->ChunkCountPerCell = 2;
    input.Config->NodeCountPerCell = 3;
    input.Config->JournalDiskSpacePerCell = 5_MB;
    input.Config->SnapshotDiskSpacePerCell = 7_MB;
    input.Config->MinNodeCount = 9;
    input.Config->MinChunkCount = 7;

    GenerateNodesForBundle(input, "bigd", 2);
    {
        auto& limits = input.RootSystemAccount->ResourceLimits;
        limits->NodeCount = 1000;
        limits->ChunkCount = 2000;
        limits->DiskSpacePerMedium["default"] = 1_MB;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.LiftedSystemAccountLimit));
    EXPECT_EQ(0, std::ssize(mutations.LoweredSystemAccountLimit));

    CheckLimits(
        TExpectedLimits{
            .Nodes = 45,
            .Chunks = 30,
            .Default = 105_MB,
            .SsdJournal = 75_MB,
        },
        mutations.LiftedSystemAccountLimit["default-bundle-account"]);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 1045,
            .Chunks = 2030,
            .Default = 106_MB,
            .SsdJournal = 75_MB
        },
        mutations.ChangedRootSystemAccountLimit);

    // Check nothing changed is limits are ok
    input.SystemAccounts["default-bundle-account"]->ResourceLimits = NYTree::CloneYsonStruct(mutations.LiftedSystemAccountLimit["default-bundle-account"]);
    input.RootSystemAccount->ResourceLimits = NYTree::CloneYsonStruct(mutations.ChangedRootSystemAccountLimit);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.LiftedSystemAccountLimit));
    EXPECT_EQ(0, std::ssize(mutations.LoweredSystemAccountLimit));
    EXPECT_FALSE(mutations.ChangedRootSystemAccountLimit);

    // Check lowered limits
    bundleInfo1->TargetConfig->CpuLimits->WriteThreadPoolSize = 3;

    // With lifted ones
    SetBundleInfo(input, "default-bundle2", 10, 20);
    auto& bundleInfo2 = input.Bundles["default-bundle2"];
    bundleInfo2->SystemAccountQuotaMultiplier = 2;
    bundleInfo2->EnableSystemAccountManagement = true;
    bundleInfo2->Options->ChangelogAccount = "default-bundle2-account";
    bundleInfo2->Options->SnapshotAccount = "default-bundle2-account";
    bundleInfo2->Options->ChangelogPrimaryMedium = "ssd_journal";
    bundleInfo2->Options->SnapshotPrimaryMedium = "ssd_blobs";
    input.SystemAccounts["default-bundle2-account"] = New<TSystemAccount>();

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.LiftedSystemAccountLimit));
    EXPECT_EQ(1, std::ssize(mutations.LoweredSystemAccountLimit));

    CheckLimits(
        TExpectedLimits{
            .Nodes = 27,
            .Chunks = 18,
            .Default = 63_MB,
            .SsdJournal = 45_MB
        },
        mutations.LoweredSystemAccountLimit["default-bundle-account"]);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 1200,
            .Chunks = 800,
            .SsdBlobs = 2800_MB,
            .SsdJournal = 2000_MB
        },
        mutations.LiftedSystemAccountLimit["default-bundle2-account"]);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 2227,
            .Chunks = 2818,
            .SsdBlobs = 2800_MB,
            .Default = 64_MB,
            .SsdJournal = 2045_MB
        },
        mutations.ChangedRootSystemAccountLimit);

    // Test account actual cells count
    GenerateTabletCellsForBundle(input, "default-bundle2", 300);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 1800,
            .Chunks = 1200,
            .SsdBlobs = 4200_MB,
            .SsdJournal = 3000_MB
        },
        mutations.LiftedSystemAccountLimit["default-bundle2-account"]);
}

TEST_P(TBundleSchedulerTest, ReAllocateOutdatedNodes)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    input.Config->ReallocateInstanceBudget = 2;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->Annotations->Resource->Vcpu /= 2;
        EXPECT_TRUE(nodeInfo->Annotations->Resource->Vcpu);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 2));
}

TEST_P(TBundleSchedulerTest, ReAllocateOutdatedNetworkLimits)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    input.Config->ReallocateInstanceBudget = 2;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->Annotations->Resource->Net = *nodeInfo->Annotations->Resource->Net / 2;
        EXPECT_TRUE(nodeInfo->Annotations->Resource->Vcpu);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 2));
}

TEST_P(TBundleSchedulerTest, DoNotReAllocateOutdatedNetworkLimitsIfDisabled)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    input.Config->ReallocateInstanceBudget = 2;
    input.Config->EnableNetworkLimits = false;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->Annotations->Resource->Net = nodeInfo->Annotations->Resource->Net.value_or(1024) / 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST_P(TBundleSchedulerTest, ReAllocateOutdatedProxies)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    input.Config->ReallocateInstanceBudget = 4;

    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Annotations->Resource->Memory /= 2;
        EXPECT_TRUE(proxyInfo->Annotations->Resource->Memory);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCProxyAllocationRequests(mutations, ForEachDataCenter(input, 4));
}

TEST_P(TBundleSchedulerTest, ReAllocateOutdatedNetworkProxies)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    input.Config->ReallocateInstanceBudget = 4;
    auto dataCenters = GetDataCenters(input);
    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Annotations->Resource->Net = 1024;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCProxyAllocationRequests(mutations, ForEachDataCenter(input, 4));
}

TEST_P(TBundleSchedulerTest, DeallocateOutdatedNodes)
{
    auto input = GenerateInputContext(10 * GetDataCenterCount(), DefaultCellCount);
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> nodesToRemove;

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 13, {.SetFilterTag = true, .SlotCount = DefaultCellCount, .DC = dataCenter});

        // Mark random nodes as outdated
        auto dataCenterNodesToRemove = GetRandomElements(nodeNames, 3);
        for (const auto& nodeName : dataCenterNodesToRemove) {
            nodesToRemove.insert(nodeName);
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->Annotations->Resource->Memory *= 2;
            EXPECT_TRUE(nodeInfo->Annotations->Resource->Memory);
        }
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(std::ssize(nodesToRemove), std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));

    // Verify that only outdated nodes are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["bigd"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        nodesToRemove.erase(deallocation->InstanceName);
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }

    EXPECT_EQ(0, std::ssize(nodesToRemove));
}

TEST_P(TBundleSchedulerTest, DeallocateOutdatedProxies)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 10 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    auto zoneInfo = input.Zones["default-zone"];

    THashSet<TString> proxiesToRemove;

    for (const auto& dataCenter : dataCenters) {
        auto proxyNames = GenerateProxiesForBundle(input, "bigd", 13, false, dataCenter);

        // Mark random proxies as outdated
        auto dataCenterProxiesToRemove = GetRandomElements(proxyNames, 3);
        for (const auto& proxyName : dataCenterProxiesToRemove) {
            proxiesToRemove.insert(proxyName);
            auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            proxyInfo->Annotations->Resource->Vcpu *= 2;
            EXPECT_TRUE(proxyInfo->Annotations->Resource->Vcpu);
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(std::ssize(proxiesToRemove), std::ssize(mutations.ChangedStates["bigd"]->ProxyDeallocations));

    // Verify that only outdated proxies are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_TRUE(proxiesToRemove.count(deallocation->InstanceName));
        proxiesToRemove.erase(deallocation->InstanceName);
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }

    EXPECT_EQ(0, std::ssize(proxiesToRemove));
}

TEST_P(TBundleSchedulerTest, ReallocateNodesUnderMaintenance)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    input.Config->ReallocateInstanceBudget = 2;
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->CmsMaintenanceRequests["test_service2"] = New<TCmsMaintenanceRequest>();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 2));
}

TEST_P(TBundleSchedulerTest, ReallocateProxiesUnderMaintenance)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);
    input.Config->ReallocateInstanceBudget = 4;

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->CmsMaintenanceRequests["test_service"] = New<TCmsMaintenanceRequest>();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCProxyAllocationRequests(mutations, ForEachDataCenter(input, 4));
}

TEST_P(TBundleSchedulerTest, ReallocateNodeUnderMaintenanceAndOutdated)
{
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    input.Config->ReallocateInstanceBudget = 2;

    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->CmsMaintenanceRequests["test_service"] = New<TCmsMaintenanceRequest>();
        nodeInfo->Annotations->Resource->Vcpu /= 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyMultiDCNodeAllocationRequests(mutations, ForEachDataCenter(input, 2));
}

TEST_P(TBundleSchedulerTest, DeallocateNodesUnderMaintenance)
{
    auto input = GenerateInputContext(10 * GetDataCenterCount(), DefaultCellCount);
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> nodesToRemove;

    for (const auto& dataCenter : dataCenters) {
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 13, {.SetFilterTag = true, .SlotCount = DefaultCellCount, .DC = dataCenter});

        auto dcNodesToRemove = GetRandomElements(nodeNames, 3);
        for (const auto& nodeName : dcNodesToRemove) {
            nodesToRemove.insert(nodeName);
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->CmsMaintenanceRequests["test_service"] = New<TCmsMaintenanceRequest>();
        }
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(std::ssize(nodesToRemove), std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));

    // Verify that only nodes under maintenance are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["bigd"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        nodesToRemove.erase(deallocation->InstanceName);
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }

    EXPECT_EQ(0, std::ssize(nodesToRemove));
}

TEST_P(TBundleSchedulerTest, DeallocateProxiesUnderMaintenance)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 10 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> proxiesToRemove;
    for (const auto& dataCenter : dataCenters) {
        auto proxyNames = GenerateProxiesForBundle(input, "bigd", 13, false, dataCenter);

        // Mark random proxies as outdated
        auto dcProxiesToRemove = GetRandomElements(proxyNames, 3);
        for (auto& proxyName : dcProxiesToRemove) {
            proxiesToRemove.insert(proxyName);
            auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            proxyInfo->CmsMaintenanceRequests["test_service"] = New<TCmsMaintenanceRequest>();
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(std::ssize(proxiesToRemove), std::ssize(mutations.ChangedStates["bigd"]->ProxyDeallocations));

    // Verify that only outdated proxies are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["bigd"]->ProxyDeallocations) {
        EXPECT_TRUE(proxiesToRemove.count(deallocation->InstanceName));
        proxiesToRemove.erase(deallocation->InstanceName);
    }

    EXPECT_EQ(0, std::ssize(proxiesToRemove));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, RemoveProxyCypressNodes)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 10 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

    THashSet<TString> proxiesToRemove;
    for (const auto& dataCenter : dataCenters) {
        auto proxyNames = GenerateProxiesForBundle(input, "bigd", 13, false, dataCenter);

        auto dcProxiesToRemove = GetRandomElements(proxyNames, 3);
        for (auto& proxyName : dcProxiesToRemove) {
            proxiesToRemove.insert(proxyName);
            auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            proxyInfo->Annotations->Allocated = false;
            proxyInfo->Annotations->DeallocatedAt = DateInThePast;
            proxyInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));

    for (auto& proxyName : proxiesToRemove) {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        proxyInfo->Alive.Reset();
    }

    mutations = {};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(std::ssize(proxiesToRemove), std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));

    for (const auto& proxyName : mutations.ProxiesToCleanup) {
        EXPECT_TRUE(proxiesToRemove.count(proxyName));
        proxiesToRemove.erase(proxyName);
    }

    EXPECT_EQ(0, std::ssize(proxiesToRemove));
}

TEST_P(TBundleSchedulerTest, RemoveTabletNodeCypressNodes)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 10 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> nodesToRemove;
    for (const auto& dataCenter : dataCenters) {
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 13, {.DC = dataCenter});
        const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

        auto dcNodesToRemove = GetRandomElements(nodeNames, 3);
        for (const auto& nodeName : dcNodesToRemove) {
            nodesToRemove.insert(nodeName);
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->Annotations->Allocated = false;
            nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
            nodeInfo->Annotations->DeallocatedAt = DateInThePast;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));

    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->State = InstanceStateOffline;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(std::ssize(nodesToRemove), std::ssize(mutations.NodesToCleanup));

    for (const auto& nodeName : mutations.NodesToCleanup) {
        EXPECT_TRUE(nodesToRemove.count(nodeName));
        nodesToRemove.erase(nodeName);
    }

    EXPECT_EQ(0, std::ssize(nodesToRemove));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, CheckBundleShortName)
{
    auto input = GenerateSimpleInputContext(5 * GetDataCenterCount());

    const TString bundleName = "bigd";
    const TString clusterShortName = "seneca";

    auto bundleInfo = GetOrCrash(input.Bundles, bundleName);
    bundleInfo->ShortName = "bigc";
    input.Zones["default-zone"]->ShortName = "seneca";

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedBundleShortName));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.NewAllocations));

    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.ChangedStates[bundleName]->NodeAllocations));

    THashSet<TString> templates;
    for (auto& [_, request] : mutations.NewAllocations) {
        templates.insert(request->Spec->PodIdTemplate);
        EXPECT_TRUE(request->Spec->PodIdTemplate.find(*bundleInfo->ShortName) != TString::npos);
        EXPECT_TRUE(request->Spec->PodIdTemplate.find(clusterShortName) != TString::npos);

        EXPECT_TRUE(request->Spec->PodIdTemplate.find(bundleName) == TString::npos);
        EXPECT_TRUE(request->Spec->PodIdTemplate.find(input.Config->Cluster) == TString::npos);
    }

    EXPECT_EQ(std::ssize(templates), 5 * GetDataCenterCount());
}


TEST_P(TBundleSchedulerTest, CheckBundleAutoShorteningName)
{
    const TString LongBundleName = "m_looooooong_bundle-name";

    auto input = GenerateSimpleInputContext(0);

    SetBundleInfo(input, LongBundleName, 5 * GetDataCenterCount());

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.NewAllocations));

    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.ChangedStates[LongBundleName]->NodeAllocations));

    EXPECT_EQ(1, std::ssize(mutations.ChangedBundleShortName));

    for (auto& [bundleName, shortName] : mutations.ChangedBundleShortName) {
        EXPECT_EQ(LongBundleName, bundleName);
        EXPECT_EQ(shortName, "m-looooo1");
    }

    THashSet<TString> templates;
    for (auto& [_, request] : mutations.NewAllocations) {
        templates.insert(request->Spec->PodIdTemplate);

        EXPECT_TRUE(request->Spec->PodIdTemplate.find(LongBundleName) == TString::npos);
    }

    EXPECT_EQ(std::ssize(templates), 5 * GetDataCenterCount());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, OfflineInstanceGracePeriod)
{
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateInputContext(5 * GetDataCenterCount());
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;

    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));

    for (const auto& [_, tabletInfo] : input.TabletNodes) {
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));

    for (const auto& [_, tabletInfo] : input.TabletNodes) {
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod * 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.NewAllocations));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, OfflineRpcProxiesGracePeriod)
{
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 5 * GetDataCenterCount());

    auto dataCenters = GetDataCenters(input);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;

    for (const auto& dataCenter : dataCenters) {
        GenerateProxiesForBundle(input, "bigd", 5, false, dataCenter);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));

    for (const auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Alive.Reset();
        proxyInfo->ModificationTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));

    for (const auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Alive.Reset();
        proxyInfo->ModificationTime = TInstant::Now() - OfflineInstanceGracePeriod * 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.NewAllocations));
    EXPECT_EQ(5 * GetDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->ProxyAllocations));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, CheckResourceLimits)
{
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);
    auto input = GenerateInputContext(5 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 5, {.SlotCount = 5, .InstanceIndex = 2, .DC = dataCenter});
        GenerateNodesForBundle(input, SpareBundleName, 3, {.DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedTabletStaticMemory));

    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 10_GB;

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.ChangedTabletStaticMemory));
    EXPECT_EQ(50_GBs * GetDataCenterCount(), mutations.ChangedTabletStaticMemory["bigd"]);

    bundleInfo->ResourceLimits->TabletStaticMemory = 50_GB * GetDataCenterCount();
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedTabletStaticMemory));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, DeallocateAdoptedNodes)
{
    auto input = GenerateInputContext(10 * GetDataCenterCount(), DefaultCellCount);
    auto dataCenters = GetDataCenters(input);

    THashSet<TString> nodesToRemove;

    for (const auto& dataCenter : dataCenters) {
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 13, {.SetFilterTag = true, .SlotCount = DefaultCellCount, .DC = dataCenter});

        // Mark random nodes as outdated
        auto dcNodesToRemove = GetRandomElements(nodeNames, 3);
        for (auto& nodeName : dcNodesToRemove) {
            nodesToRemove.insert(nodeName);
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->Annotations->Resource->Memory *= 2;
            nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyReturnToBB;
            EXPECT_TRUE(nodeInfo->Annotations->Resource->Memory);
            nodeInfo->EnableBundleBalancer = false;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(std::ssize(nodesToRemove), std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));

    // Verify that only outdated nodes are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["bigd"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        EXPECT_TRUE(!deallocation->InstanceName.empty());
        EXPECT_EQ(deallocation->Strategy, DeallocationStrategyReturnToBB);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));

    for (auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        GetOrCrash(input.TabletNodes, nodeName)->Decommissioned = decommissioned;
        EXPECT_TRUE(decommissioned);
        EXPECT_TRUE(nodesToRemove.count(nodeName));

        SetTabletSlotsState(input, nodeName, PeerStateLeading);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Node are decommissioned but tablet slots have to be empty.
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));

    for (const auto& nodeName : nodesToRemove) {
        SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are not created for BB nodes.
    // auto& bundleState = mutations.ChangedStates["bigd"];
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));

    // Check Setting node attributes
    {
        EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangeNodeAnnotations));
        for (auto& nodeId : nodesToRemove) {
            const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
            EXPECT_TRUE(annotations->YPCluster.empty());
            EXPECT_TRUE(annotations->AllocatedForBundle.empty());
            EXPECT_TRUE(annotations->NannyService.empty());
            EXPECT_FALSE(annotations->Allocated);
            EXPECT_TRUE(annotations->DeallocatedAt);
            EXPECT_TRUE(TInstant::Now() - *annotations->DeallocatedAt < TDuration::Minutes(10));
            EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyReturnToBB);

            input.TabletNodes[nodeId]->Annotations = annotations;
        }
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_EQ(0, std::ssize(tags));
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        EXPECT_TRUE(nodesToRemove.count(nodeName));
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedEnableBundleBalancerFlag));
    for (auto& [nodeName, enableBundleBalancer] : mutations.ChangedEnableBundleBalancerFlag) {
        GetOrCrash(input.TabletNodes, nodeName)->EnableBundleBalancer = enableBundleBalancer;
        EXPECT_TRUE(enableBundleBalancer);
    }

    // Finally!
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedEnableBundleBalancerFlag));
    EXPECT_EQ(3 * GetDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    for (auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_FALSE(decommissioned);
        EXPECT_TRUE(nodesToRemove.count(nodeName));
    }

    EXPECT_EQ(1, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->NodeDeallocations));
}

TEST_P(TBundleSchedulerTest, DontRemoveTabletNodeCypressNodesFromBB)
{
    auto input = GenerateInputContext(DefaultNodeCount, DefaultCellCount, 10 * GetDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    for (const auto& dataCenter : dataCenters) {
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 13, {.DC = dataCenter});
        const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

        auto nodesToRemove = GetRandomElements(nodeNames, 3);
        for (auto& nodeName : nodesToRemove) {
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->Annotations->Allocated = false;
            nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyReturnToBB;
            nodeInfo->Annotations->DeallocatedAt = DateInThePast;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TBundleSchedulerTest, CreateNewCellsCreationMultiPeer)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 3 * GetActiveDataCenterCount());
    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->Options->PeerCount = 2;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(1, std::ssize(mutations.CellsToCreate));

    EXPECT_EQ(2 * GetActiveDataCenterCount(), mutations.CellsToCreate.at("bigd"));
}

TEST_P(TBundleSchedulerTest, CreateNewCellsNoRemoveNoCreateMultiPeer)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 5 * GetActiveDataCenterCount());
    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->Options->PeerCount = 2;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
}

TEST_P(TBundleSchedulerTest, CreateNewCellsRemoveMultiPeer)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);
    TSchedulerMutations mutations;

    for (const auto& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 5 * GetActiveDataCenterCount() + 3);
    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->Options->PeerCount = 2;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(3, std::ssize(mutations.CellsToRemove));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProxyRoleManagement, TestBundleProxyRolesAssigned)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;

    GenerateProxiesForBundle(input, "bigd", 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        ASSERT_EQ(role, "bigd");
        input.RpcProxies[proxyName]->Role = role;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
}

TEST(TProxyRoleManagement, TestBundleProxyCustomRolesAssigned)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;
    input.Bundles["bigd"]->RpcProxyRole = "custom-role";

    GenerateProxiesForBundle(input, "bigd", 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        ASSERT_EQ(role, "custom-role");
        input.RpcProxies[proxyName]->Role = role;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
}

TEST(TProxyRoleManagement, TestBundleProxyBanned)
{
    const bool SetProxyRole = true;

    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 3);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;

    auto bundleProxies = GenerateProxiesForBundle(input, "bigd", 3, SetProxyRole);

    // Generate Spare proxies
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    auto spareProxies = GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    // Ban bundle proxy
    {
        auto& proxy = GetOrCrash(input.RpcProxies, *bundleProxies.begin());
        proxy->Banned = true;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(1, std::ssize(mutations.ChangedProxyRole));
    EXPECT_EQ(1, std::ssize(mutations.NewAllocations));

    for (auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_EQ(role, "bigd");
        EXPECT_TRUE(spareProxies.find(proxyName) != spareProxies.end());
    }
}

TEST(TProxyRoleManagement, TestBundleProxyRolesWithSpare)
{
    const bool SetProxyRole = true;

    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 3);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;

    GenerateProxiesForBundle(input, "bigd", 1, SetProxyRole);

    // Generate Spare proxies
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    auto spareProxies = GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));

    THashSet<TString> usedSpare;

    for (auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_EQ(role, "bigd");
        EXPECT_TRUE(spareProxies.find(proxyName) != spareProxies.end());

        usedSpare.insert(proxyName);
        input.RpcProxies[proxyName]->Role = role;
    }

    EXPECT_EQ(2, std::ssize(usedSpare));

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));

    // Add new proxies to bundle
    auto newProxies = GenerateProxiesForBundle(input, "bigd", 1, SetProxyRole);;

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_TRUE(usedSpare.count(proxyName) != 0);
        input.RpcProxies[proxyName]->Role = role;
    }
    EXPECT_EQ(1, std::ssize(mutations.RemovedProxyRole));

    // Check no more changes
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
}

TEST(TProxyRoleManagement, TestBundleProxyRolesWentOffline)
{
    const bool SetProxyRole = true;
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;

    auto proxies = GenerateProxiesForBundle(input, "bigd", 2, SetProxyRole);

    // Generate Spare proxies
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    auto spareProxies = GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));

    for (const auto& proxyName : proxies) {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        proxyInfo->ModificationTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
        proxyInfo->Alive.Reset();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));
}

TEST(TProxyRoleManagement, TestBundleProxyCustomRolesWithSpare)
{
    const bool SetProxyRole = true;

    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 3);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;
    input.Bundles["bigd"]->RpcProxyRole = "custom-role";

    GenerateProxiesForBundle(input, "bigd", 1, SetProxyRole);

    // Generate Spare proxies
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    auto spareProxies = GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));

    THashSet<TString> usedSpare;

    for (auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_EQ(role, "custom-role");
        EXPECT_TRUE(spareProxies.find(proxyName) != spareProxies.end());

        usedSpare.insert(proxyName);
        input.RpcProxies[proxyName]->Role = role;
    }

    EXPECT_EQ(2, std::ssize(usedSpare));

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));

    // Add new proxies to bundle
    auto newProxies = GenerateProxiesForBundle(input, "bigd", 1, SetProxyRole);;

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_TRUE(usedSpare.count(proxyName) != 0);
        EXPECT_EQ(role, "");
        input.RpcProxies[proxyName]->Role = role;
    }
    EXPECT_EQ(1, std::ssize(mutations.RemovedProxyRole));

    // Check no more changes
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
}

TEST(TProxyRoleManagement, TestFreeSpareProxiesExhausted)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 4);
    input.Bundles["bigd"]->EnableRpcProxyManagement = true;

    // Generate Spare proxies
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    auto spareProxies = GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(3, std::ssize(mutations.ChangedProxyRole));

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        ASSERT_EQ(role, "bigd");
        input.RpcProxies[proxyName]->Role = role;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "no_free_spare_proxies");
}

////////////////////////////////////////////////////////////////////////////////

TEST(SchedulerUtilsTest, CheckGetIndexFromPodId)
{
    static const TString Cluster = "hume";
    static const TString InstanceType = "tab";
    static const TString Bundle = "venus212";

    EXPECT_EQ(1, FindNextInstanceId({}, Cluster, InstanceType));
    EXPECT_EQ(1, FindNextInstanceId({"sas4-5335-venus212-0aa-tab-hume"}, Cluster, InstanceType));
    EXPECT_EQ(1, FindNextInstanceId({"sas4-5335-venus212-000-tab-hume"}, Cluster, InstanceType));
    EXPECT_EQ(2, FindNextInstanceId({"sas4-5335-venus212-001-tab-hume", "trash"}, Cluster, InstanceType));

    EXPECT_EQ(4, FindNextInstanceId(
        {
            "sas4-5335-venus212-001-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-002-tab-hume",
            "sas4-5335-venus212-003-tab-hume",
            "sas4-5335-venus212-005-tab-hume",
        },
        Cluster,
        InstanceType));

    EXPECT_EQ(6, FindNextInstanceId(
        {
            "sas4-5335-venus212-001-tab-hume",
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 2),
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 3),
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 4),
            "sas4-5335-venus212-005-tab-hume",
            GetInstancePodIdTemplate(Cluster, Bundle, InstanceType, 7),
        },
        Cluster,
        InstanceType));
}

TEST(SchedulerUtilsTest, CheckGeneratePodTemplate)
{
    EXPECT_EQ("<short-hostname>-venus212-0ab-exe-shtern", GetInstancePodIdTemplate("shtern", "venus212", "exe", 171));
    EXPECT_EQ("<short-hostname>-venus212-2710-exe-shtern", GetInstancePodIdTemplate("shtern", "venus212", "exe", 10000));
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TNodeTagsFilterManager, TestBundleWithNoTagFilter)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 5);
    auto dataCenters = GetDataCenters(input);

    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    input.Bundles["bigd"]->NodeTagFilter = {};

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 10 * GetActiveDataCenterCount());

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(1, std::ssize(mutations.InitializedNodeTagFilters));
    EXPECT_EQ(mutations.InitializedNodeTagFilters["bigd"], "default-zone/bigd");
}

TEST_P(TNodeTagsFilterManager, TestBundleNodeTagsAssigned)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 11);
    auto dataCenters = GetDataCenters(input);

    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = bundleInfo->NodeTagFilter;
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.DC = dataCenter});

        GenerateNodesForBundle(input, SpareBundleName, 3, {.SetFilterTag = false, .SlotCount = 15, .DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 11 * GetActiveDataCenterCount());

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->BundleNodeReleasements));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));

    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));

    THashSet<TString> newTabletNodes;
    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_TRUE(mutations.ChangedDecommissionedFlag.at(nodeName));
        EXPECT_TRUE(tags.find(bundleNodeTagFilter) != tags.end());

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        nodeInfo->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];

        newTabletNodes.insert(nodeName);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Nothing happen until node prepared (applied dyn config)
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& node : newTabletNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, node);
        nodeInfo->TabletSlots.resize(bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize.value());
        SetTabletSlotsState(input, node, TabletSlotStateEmpty);
        nodeInfo->Statistics->Memory->TabletStatic->Limit = *bundleInfo->TargetConfig->MemoryLimits->TabletStatic;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(newTabletNodes.find(nodeName) != newTabletNodes.end());
        EXPECT_FALSE(decommissioned);

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Finally bundle nodes assigned
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_TRUE(mutations.ChangedStates.at("bigd")->BundleNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates.at("bigd")->BundleNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates.at("bigd")->SpareNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates.at("bigd")->SpareNodeReleasements.empty());

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
}

TEST_P(TNodeTagsFilterManager, TestExtraBundleNodesReleasement)
{
    auto input = GenerateInputContext(2 * GetDataCenterCount(), 11);
    auto dataCenters = GetDataCenters(input);
    auto zoneInfo = input.Zones["default-zone"];

    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = bundleInfo->NodeTagFilter;
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = true, .DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", 11 * GetActiveDataCenterCount());

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(2 * zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeReleasements));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    THashSet<TString> releasingNodes;
    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(decommissioned);

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;

        EXPECT_EQ(dataCenters.back(), nodeInfo->Annotations->DataCenter);

        releasingNodes.insert(nodeName);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    // Nothing happen until tablet cells running on nodes.
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    for (const auto& nodeName : releasingNodes) {
        SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(2 * zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        EXPECT_TRUE(tags.count(bundleNodeTagFilter) == 0);
        EXPECT_EQ(1u, releasingNodes.count(nodeName));
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    // Recommission node back
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(2 * zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_FALSE(decommissioned);
        EXPECT_TRUE(releasingNodes.count(nodeName) != 0);

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    // Finally bundle nodes released
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    // Finally bundle nodes assigned
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    if (zoneInfo->RedundantDataCenterCount) {
        EXPECT_TRUE(mutations.ChangedStates.at("bigd")->BundleNodeAssignments.empty());
        EXPECT_TRUE(mutations.ChangedStates.at("bigd")->BundleNodeReleasements.empty());
        EXPECT_TRUE(mutations.ChangedStates.at("bigd")->SpareNodeAssignments.empty());
        EXPECT_TRUE(mutations.ChangedStates.at("bigd")->SpareNodeReleasements.empty());
    }

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
}

TEST_P(TNodeTagsFilterManager, TestBundleNodesWithSpare)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(2 * GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    THashSet<TString> activeDataCenters;

    int dataCenterIndex = 0;
    for (const TString& dataCenter : dataCenters) {
        bool setNodeTagFilter = ++dataCenterIndex <= GetActiveDataCenterCount();
        if (setNodeTagFilter) {
            activeDataCenters.insert(dataCenter);
        }

        GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = setNodeTagFilter, .SlotCount = SlotCount, .DC = dataCenter});
        GenerateNodeAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    GenerateTabletCellsForBundle(input, "bigd", 13 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    THashSet<TString> spareNodes;

    for (const TString& dataCenter : dataCenters) {
        auto dataCenterSpareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, {.SetFilterTag = false, .SlotCount = 15, .DC = dataCenter});

        for (const auto& nodeName : dataCenterSpareNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->TabletSlots.clear();
            spareNodes.insert(nodeName);
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->BundleNodeAssignments));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->BundleNodeReleasements));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));

    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    ApplyChangedStates(&input, mutations);

    const TString BundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;
    THashSet<TString> usedSpare;

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_TRUE(mutations.ChangedDecommissionedFlag.at(nodeName));
        EXPECT_TRUE(tags.find(BundleNodeTagFilter) != tags.end());
        EXPECT_TRUE(spareNodes.find(nodeName) != spareNodes.end());
        usedSpare.insert(nodeName);

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        nodeInfo->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];
    }

    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(usedSpare));

    for (const TString& dataCenter : activeDataCenters) {
        EXPECT_EQ(1, std::ssize(input.ZoneToSpareNodes["default-zone"][dataCenter].FreeNodes));
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Nothing happen until node prepared (applied dyn config)
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (auto& node : spareNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, node);
        nodeInfo->TabletSlots.resize(bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize.value());
        SetTabletSlotsState(input, node, TabletSlotStateEmpty);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (auto& node : spareNodes) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, node);
        nodeInfo->Statistics->Memory->TabletStatic->Limit = *bundleInfo->TargetConfig->MemoryLimits->TabletStatic;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(spareNodes.find(nodeName) != spareNodes.end());
        EXPECT_FALSE(decommissioned);

        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Finally spare node assigned
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates));
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));

    // Populate slots with cell peers.
    for (const auto& spareNode : usedSpare) {
        SetTabletSlotsState(input, spareNode, PeerStateLeading);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));

    // Add new nodes to bundle
    THashSet<TString> newNodes;
    for (const auto& dataCenter : activeDataCenters) {
        auto dcNodes = GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = false, .SlotCount = SlotCount, .DC = dataCenter});
        newNodes.insert(dcNodes.begin(), dcNodes.end());
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        EXPECT_EQ(1u, newNodes.count(nodeName));
        nodeInfo->Decommissioned = mutations.ChangedDecommissionedFlag.at(nodeName);

        nodeInfo->Statistics->Memory->TabletStatic->Limit = *bundleInfo->TargetConfig->MemoryLimits->TabletStatic;
        nodeInfo->TabletSlots.resize(bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize.value());
        SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));

    THashSet<TString> spareNodeToRelease;

    for (const auto& [nodeName, decommission] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(usedSpare.count(nodeName) != 0);
        EXPECT_TRUE(decommission);
        input.TabletNodes[nodeName]->Decommissioned = decommission;
        spareNodeToRelease.insert(nodeName);
    }

    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Populate slots with cell peers.
    for (const auto& spareNode : spareNodeToRelease) {
        SetTabletSlotsState(input, spareNode, TabletSlotStateEmpty);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_TRUE(Contains(spareNodeToRelease, nodeName));
        EXPECT_TRUE(tags.count(BundleNodeTagFilter) == 0);
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& dataCenter : activeDataCenters) {
        for (const auto& node : input.ZoneToSpareNodes["default-zone"][dataCenter].ReleasingByBundle["bigd"]) {
            EXPECT_TRUE(Contains(spareNodeToRelease, node));
        }
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    for (const auto& dataCenter : activeDataCenters) {
        auto& defaultDCSpareNodes = input.ZoneToSpareNodes["default-zone"][dataCenter];
        for (const auto& node : defaultDCSpareNodes.ReleasingByBundle["bigd"]) {
            EXPECT_TRUE(Contains(spareNodeToRelease, node));

            EXPECT_FALSE(Contains(defaultDCSpareNodes.FreeNodes, node));
            EXPECT_FALSE(Contains(defaultDCSpareNodes.UsedByBundle["bigd"], node));
        }
    }

    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& dataCenter : activeDataCenters) {
        auto& zoneSpare = input.ZoneToSpareNodes["default-zone"][dataCenter];
        EXPECT_TRUE(std::any_of(spareNodeToRelease.begin(), spareNodeToRelease.end(), [&] (const auto& nodeName) {
            return Contains(zoneSpare.FreeNodes, nodeName);
        }));
        EXPECT_EQ(1, std::ssize(zoneSpare.UsedByBundle["bigd"]));
    }

    // Add one more node to bundle
    for (const auto& dataCenter : activeDataCenters) {
        GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = true, .SlotCount = SlotCount, .DC = dataCenter});
    }
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeAssignments.empty());
}

TEST_P(TNodeTagsFilterManager, TestSeveralBundlesNodesLookingForSpare)
{
    auto input = GenerateInputContext(0, 5);
    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    bundleInfo->EnableTabletCellManagement = false;
    GenerateTabletCellsForBundle(input, "bigd", 9 * GetActiveDataCenterCount());
    auto dataCenters = GetDataCenters(input);

    SetBundleInfo(input, "bigc", 0, 10);
    auto& bundleInfo2 = input.Bundles["bigc"];
    bundleInfo2->EnableNodeTagFilterManagement = true;
    bundleInfo2->EnableTabletCellManagement = false;
    GenerateTabletCellsForBundle(input, "bigc", 17 * GetActiveDataCenterCount());

    SetBundleInfo(input, "bige", 2 * GetDataCenterCount(), 10);
    GenerateNodesForBundle(input, "bige", 1, {.SetFilterTag = true, .SlotCount = 10});
    auto& bundleInfo3 = input.Bundles["bige"];
    bundleInfo3->EnableNodeTagFilterManagement = true;
    bundleInfo3->EnableBundleController = false;
    GenerateTabletCellsForBundle(input, "bige", 17 * GetActiveDataCenterCount());

    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 4 * GetDataCenterCount();

    for (const TString& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 4, {.SlotCount = 0, .DC = dataCenter});
        for (const auto& nodeName : spareNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->TabletSlots.clear();
        }

        // Alien bundle uses spare node
        for (const auto& nodeName : spareNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->UserTags = {bundleInfo3->NodeTagFilter};
            break;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(3 * GetActiveDataCenterCount() + zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(3 * GetActiveDataCenterCount() + zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedNodeUserTags));

    THashSet<TString> oversubscribedDataCenters;

    for (const TString& dataCenter : dataCenters) {
        auto& zoneSpare = input.ZoneToSpareNodes["default-zone"][dataCenter];
        if (std::ssize(zoneSpare.FreeNodes) == 0) {
            oversubscribedDataCenters.insert(dataCenter);
        }
    }

    EXPECT_GE(std::ssize(oversubscribedDataCenters), 1);
    EXPECT_LE(std::ssize(oversubscribedDataCenters), GetActiveDataCenterCount());

    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    for (const TString& dataCenter : dataCenters) {
        auto& zoneSpare = input.ZoneToSpareNodes["default-zone"][dataCenter];
        auto usingSpareNodeCount = std::ssize(zoneSpare.UsedByBundle["bigd"]) + std::ssize(zoneSpare.UsedByBundle["bigc"]);

        if (oversubscribedDataCenters.count(dataCenter)) {
            EXPECT_EQ(0, std::ssize(zoneSpare.FreeNodes));
            EXPECT_EQ(3, usingSpareNodeCount);
        } else {
            EXPECT_EQ(1, std::ssize(zoneSpare.FreeNodes));
            EXPECT_EQ(2, usingSpareNodeCount);
        }
        EXPECT_EQ(0, std::ssize(zoneSpare.ReleasingByBundle));
    }
}

TEST_P(TNodeTagsFilterManager, TestBundleNodesGracePeriod)
{
    const bool SetNodeFilterTag = true;
    const int SlotCount = 5;
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateInputContext(2 * GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;

    THashSet<TString> nodes;
    for (const TString& dataCenter : dataCenters) {
        auto dataNodes = GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = SetNodeFilterTag, .SlotCount = SlotCount, .DC = dataCenter});
        nodes.insert(dataNodes.begin(), dataNodes.end());
    }

    GenerateTabletCellsForBundle(input, "bigd", 10 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
    }

    for (const auto& nodeName : nodes) {
        auto& tabletInfo = GetOrCrash(input.TabletNodes, nodeName);
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Checking that grace period does not affect spare nodes assignments
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
}

TEST_P(TNodeTagsFilterManager, TestBundleNodesDisabledTabletCells)
{
    const bool SetNodeFilterTag = true;
    const int SlotCount = 5;
    auto input = GenerateInputContext(2 * GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;

    THashSet<TString> nodes;
    for (const TString& dataCenter : dataCenters) {
        auto dataNodes = GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = SetNodeFilterTag, .SlotCount = SlotCount, .DC = dataCenter});
        nodes.insert(dataNodes.begin(), dataNodes.end());
    }

    GenerateTabletCellsForBundle(input, "bigd", 10 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
    }

    for (const auto& nodeName : nodes) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->DisableTabletCells = true;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Checking that grace period does not affect spare nodes assignments
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
}

TEST_P(TNodeTagsFilterManager, SpareNodesExhausted)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(0, SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    input.Bundles["bigd"]->EnableTabletCellManagement = false;
    GenerateTabletCellsForBundle(input, "bigd", 20 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    for (const TString& dataCenter : dataCenters) {
        GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(3 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(3 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        input.TabletNodes[nodeName]->UserTags = tags;
        input.TabletNodes[nodeName]->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];
    }
    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "no_free_spare_nodes");
}

TEST_P(TNodeTagsFilterManager, SpareNodesOffline)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(0, SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    input.Bundles["bigd"]->EnableTabletCellManagement = false;

    GenerateTabletCellsForBundle(input, "bigd", 10 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    THashSet<TString> aliveSpare;

    for (const TString& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
        auto dcAliveSpare = GetRandomElements(spareNodes, 1);
        aliveSpare.insert(dcAliveSpare.begin(), dcAliveSpare.end());

        for (const auto& spareNode : spareNodes) {
            if (dcAliveSpare.count(spareNode) == 0) {
                input.TabletNodes[spareNode]->State = InstanceStateOffline;
                input.TabletNodes[spareNode]->LastSeenTime = TInstant::Now();
            }
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        input.TabletNodes[nodeName]->UserTags = tags;
        input.TabletNodes[nodeName]->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];

        EXPECT_TRUE(aliveSpare.count(nodeName) != 0);
    }
    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "no_free_spare_nodes");
}

TEST_P(TNodeTagsFilterManager, SpareNodesWentOfflineAfterAssigning)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(0, SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    input.Bundles["bigd"]->EnableTabletCellManagement = false;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();

    THashSet<TString> usingSpareNode;
    int dataCenterIndex = 0;
    for (const TString& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
        if (++dataCenterIndex > GetActiveDataCenterCount()) {
            break;
        }

        auto dcUsingSpareNode = GetRandomElements(spareNodes, 1);
        usingSpareNode.insert(dcUsingSpareNode.begin(), dcUsingSpareNode.end());

        for (const auto& spareNode : dcUsingSpareNode) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
            nodeInfo->UserTags = { bundleNodeTagFilter };
            nodeInfo->Decommissioned = false;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Using spare node went offline
    for (const auto& spareNode : usingSpareNode) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
        nodeInfo->State = InstanceStateOffline;
        nodeInfo->LastSeenTime = TInstant::Now();
    }
    ApplyChangedStates(&input, mutations);

    mutations = {};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        input.TabletNodes[nodeName]->UserTags = tags;
        input.TabletNodes[nodeName]->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];

        EXPECT_TRUE(usingSpareNode.count(nodeName) == 0);
    }
}

TEST_P(TNodeTagsFilterManager, SpareNodesDecommissionedAfterAssigning)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(0, SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    input.Bundles["bigd"]->EnableTabletCellManagement = false;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();
    THashSet<TString> activeDataCenters;
    THashSet<TString> usingSpareNode;
    int dataCenterIndex = 0;

    for (const auto& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = SlotCount, .DC = dataCenter});
        if (++dataCenterIndex > GetActiveDataCenterCount()) {
            break;
        }

        auto dcUsingSpareNode = GetRandomElements(spareNodes, 1);
        usingSpareNode.insert(dcUsingSpareNode.begin(), dcUsingSpareNode.end());
        activeDataCenters.insert(dataCenter);

        for (const auto& spareNode : dcUsingSpareNode) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
            nodeInfo->UserTags = { bundleNodeTagFilter };
            nodeInfo->Decommissioned = false;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Using spare node were externally decommissioned
    for (const auto& spareNode : usingSpareNode) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
        nodeInfo->Decommissioned = true;
    }
    ApplyChangedStates(&input, mutations);

    mutations = {};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "externally_decommissioned_spare_nodes");

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        nodeInfo->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];

        EXPECT_TRUE(activeDataCenters.count(*nodeInfo->Annotations->DataCenter) != 0);
        EXPECT_TRUE(usingSpareNode.count(nodeName) == 0);
    }

    ApplyChangedStates(&input, mutations);
    mutations = {};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "externally_decommissioned_spare_nodes");

    for (const TString& dataCenter : activeDataCenters) {
        const auto& spareNodesInfo = input.ZoneToSpareNodes["default-zone"][dataCenter];
        EXPECT_EQ(1, std::ssize(spareNodesInfo.FreeNodes));
        EXPECT_EQ(1, std::ssize(spareNodesInfo.UsedByBundle.at("bigd")));
        EXPECT_EQ(1, std::ssize(spareNodesInfo.ExternallyDecommissioned));
    }
}

TEST_P(TNodeTagsFilterManager, SpareNodesWentOfflineDuringAssigning)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodeAllocationsForBundle(input, "bigd", GetDataCenterCount(), dataCenter);
    }
    GenerateTabletCellsForBundle(input, "bigd", 5 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();
    for (const TString& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 15, .DC = dataCenter});
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeReleasements.empty());

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    ApplyChangedStates(&input, mutations);
    THashSet<TString> assigningSpare;

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        assigningSpare.insert(nodeName);
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
        nodeInfo->Decommissioned = mutations.ChangedDecommissionedFlag[nodeName];
        // Set wrong settings
        nodeInfo->TabletSlots.clear();
        nodeInfo->Statistics->Memory->TabletStatic->Limit = 212;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Nothing happen until node prepared (applied dyn config)
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Here our spare nodes tragically went offline.
    for (const auto& node : assigningSpare) {
        input.TabletNodes[node]->State = InstanceStateOffline;
        input.TabletNodes[node]->LastSeenTime = TInstant::Now();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // At this point we expect the followings:
    // - Old spare node assignment should be finished
    // - New node assignment should be started

    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeReleasements.empty());

    for (const auto& node : assigningSpare) {
        EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeAssignments.count(node) != 0);
    }

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->UserTags = tags;
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates["bigd"]->SpareNodeAssignments));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& node : assigningSpare) {
        EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeAssignments.count(node) == 0);
    }

    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeReleasements.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->BundleNodeAssignments.empty());
    EXPECT_TRUE(mutations.ChangedStates["bigd"]->SpareNodeReleasements.empty());
}

TEST_P(TNodeTagsFilterManager, SpareNodesWentOfflineDuringReleasing)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodeAllocationsForBundle(input, "bigd", 1, dataCenter);
    }

    GenerateTabletCellsForBundle(input, "bigd", 5 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];

    THashSet<TString> usingSpareNode;

    zoneInfo->SpareTargetConfig->TabletNodeCount = 3 * GetDataCenterCount();
    THashSet<TString> activeDataCenters;
    int dataCenterIndex = 0;
    for (const TString& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SlotCount = SlotCount, .DC = dataCenter};
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, flags);
        if (++dataCenterIndex > GetActiveDataCenterCount()) {
            break;
        }

        activeDataCenters.insert(dataCenter);
        auto dcUsingSpareNode = GetRandomElements(spareNodes, 1);
        for (const auto& spareNode : dcUsingSpareNode) {
            usingSpareNode.insert(spareNode);
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
            nodeInfo->UserTags = { bundleNodeTagFilter };
            nodeInfo->Decommissioned = false;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Populate slots with cell peers.
    for (const auto& spareNode : usingSpareNode) {
        SetTabletSlotsState(input, spareNode, PeerStateLeading);
    }

    EXPECT_EQ(std::ssize(activeDataCenters), GetActiveDataCenterCount());
    EXPECT_EQ(std::ssize(usingSpareNode), GetActiveDataCenterCount());

    // Add new node to bundle
    for (const auto& dataCenter : activeDataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = true, .SlotCount = SlotCount, .DC = dataCenter};
        auto nodeNames = GenerateNodesForBundle(input, "bigd", 1, flags);
    }

    mutations = {};
    ScheduleBundles(input, &mutations);

    // Start releasing spare node
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedStates.at("bigd")->SpareNodeReleasements));

    for (const auto& [nodeName, decommission] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(usingSpareNode.count(nodeName) != 0);
        EXPECT_TRUE(decommission);
        input.TabletNodes[nodeName]->Decommissioned = decommission;
    }

    ApplyChangedStates(&input, mutations);
    mutations = {};
    ScheduleBundles(input, &mutations);

    // Nothing happens until spare node is populated with cells
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));

    // Using spare node went offline
    for (const auto& spareNode : usingSpareNode) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
        nodeInfo->State = InstanceStateOffline;
        nodeInfo->LastSeenTime = TInstant::Now();
        nodeInfo->TabletSlots.clear();
    }

    mutations = {};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0u, mutations.ChangedStates.count("bigd"));

    mutations = {};
    ScheduleBundles(input, &mutations);
    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_TRUE(usingSpareNode.count(nodeName) != 0);
        EXPECT_TRUE(tags.count(bundleNodeTagFilter) == 0);
        input.TabletNodes[nodeName]->UserTags = tags;
    }
    ApplyChangedStates(&input, mutations);

    mutations = {};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["bigd"]->SpareNodeReleasements));
}

TEST_P(TNodeTagsFilterManager, TestMultiPeerBundleLookingForSpare)
{
    auto input = GenerateInputContext(0, 5);
    auto dataCenters = GetDataCenters(input);

    auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->EnableNodeTagFilterManagement = true;

    THashSet<TString> activeDataCenters;
    int dataCenterIndex = 0;

    for (const TString& dataCenter : dataCenters) {
        bool setNodeTagFilter = ++dataCenterIndex <= GetActiveDataCenterCount();
        if (setNodeTagFilter) {
            activeDataCenters.insert(dataCenter);
        }

        GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = setNodeTagFilter, .DC = dataCenter});
    }

    EXPECT_EQ(std::ssize(activeDataCenters), GetActiveDataCenterCount());

    static constexpr int PeerCount = 3;
    GenerateTabletCellsForBundle(input, "bigd", 5 * GetActiveDataCenterCount(), PeerCount);
    bundleInfo->Options->PeerCount = PeerCount;
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 212212;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 4 * GetDataCenterCount();

    THashSet<TString> usingSpareNode;

    for (const TString& dataCenter : dataCenters) {
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 4, {.SlotCount = 0, .DC = dataCenter});

        for (const auto& nodeName : spareNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->TabletSlots.clear();
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2 * GetActiveDataCenterCount(), std::ssize(mutations.ChangedNodeUserTags));

    for (const TString& dataCenter : activeDataCenters) {
        EXPECT_EQ(2, std::ssize(input.ZoneToSpareNodes["default-zone"][dataCenter].FreeNodes));
    }

    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    for (const auto& dataCenter : activeDataCenters) {
        auto& zoneSpare = input.ZoneToSpareNodes["default-zone"][dataCenter];
        EXPECT_EQ(2, std::ssize(zoneSpare.FreeNodes));
        EXPECT_EQ(2, std::ssize(zoneSpare.UsedByBundle.at("bigd")));
        EXPECT_EQ(0, std::ssize(zoneSpare.ReleasingByBundle));
    }
}

TEST_P(TNodeTagsFilterManager, ReleasingExtraSpareNodes)
{
    const int SlotCount = 5;
    auto input = GenerateInputContext(GetDataCenterCount(), SlotCount);
    auto dataCenters = GetDataCenters(input);
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    for (const TString& dataCenter : dataCenters) {
        GenerateNodeAllocationsForBundle(input, "bigd", 1, dataCenter);
    }
    GenerateTabletCellsForBundle(input, "bigd", 15 * GetActiveDataCenterCount());

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    THashSet<TString> usingSpareNode;

    zoneInfo->SpareTargetConfig->TabletNodeCount = 5 * GetDataCenterCount();
    for (const TString& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SlotCount = SlotCount, .DC = dataCenter};
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 5, flags);

        auto dcUsingSpareNode = GetRandomElements(spareNodes, 3);
        for (const auto& spareNode : dcUsingSpareNode) {
            usingSpareNode.insert(spareNode);
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, spareNode);
            nodeInfo->UserTags = { bundleNodeTagFilter };
            nodeInfo->Decommissioned = false;
        }
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(3 * zoneInfo->RedundantDataCenterCount, std::ssize(mutations.ChangedDecommissionedFlag));

    THashSet<TString> releasingDC;
    for (const auto& [nodeName, _] : mutations.ChangedDecommissionedFlag) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        releasingDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(zoneInfo->RedundantDataCenterCount, std::ssize(releasingDC));
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    TBundleSchedulerTest,
    TBundleSchedulerTest,
    ::testing::Values(
        std::tuple(EZoneSetup::Simple, 1),
        std::tuple(EZoneSetup::MultiCluster, 3)
    )
);

INSTANTIATE_TEST_SUITE_P(
    TNodeTagsFilterManager,
    TNodeTagsFilterManager,
    ::testing::Values(
        std::tuple(EZoneSetup::Simple, 1),
        std::tuple(EZoneSetup::MultiCluster, 3)
    )
);

////////////////////////////////////////////////////////////////////////////////

TEST(TDataCentersPriority, AlphaNumDC)
{
    constexpr int SlotCount = 5;
    constexpr int PerDataCenterCount = 3;

    auto input = GenerateMultiDCInputContext(3 * PerDataCenterCount, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 5 * dataCenters.size();

    // Add new node to bundle
    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = false, .SlotCount = SlotCount, .DC = dataCenter};
        GenerateNodesForBundle(input, "bigd", PerDataCenterCount, flags);
        GenerateNodesForBundle(input, SpareBundleName, 5, {.DC = dataCenter});
    }

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * PerDataCenterCount * 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    const auto& assignments = mutations.ChangedStates.at("bigd")->BundleNodeAssignments;
    EXPECT_EQ(6, std::ssize(assignments));

    THashSet<TString> assigningDC;
    for (const auto& [nodeName, _] : assignments) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        assigningDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(2, std::ssize(assigningDC));
    EXPECT_EQ(assigningDC, THashSet<TString>({"dc-1", "dc-2"}));
}

TEST(TDataCentersPriority, Feasibility)
{
    constexpr int SlotCount = 5;

    auto input = GenerateMultiDCInputContext(3, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 5 * dataCenters.size();

    // Add new node to bundle
    TString offlineDC = *GetRandomElements(dataCenters, 1).begin();

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = true, .SlotCount = SlotCount, .DC = dataCenter};
        auto nodes = GenerateNodesForBundle(input, "bigd", 1, flags);
        auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 5, {.DC = dataCenter});

        if (dataCenter != offlineDC) {
            continue;
        }

        for (auto& nodeName : spareNodes) {
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->State = InstanceStateOffline;
        }
    }

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));
    EXPECT_EQ(4, std::ssize(mutations.ChangedStates.at("bigd")->SpareNodeAssignments));

    THashSet<TString> assigningDC;
    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->SpareNodeAssignments) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        assigningDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(2, std::ssize(assigningDC));
    dataCenters.erase(offlineDC);
    EXPECT_EQ(assigningDC, dataCenters);
}

TEST(TDataCentersPriority, Forbidden)
{
    constexpr int SlotCount = 5;

    auto input = GenerateMultiDCInputContext(9, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];

    // Add new node to bundle
    auto offlineDC = *GetRandomElements(dataCenters, 1).begin();

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = false, .SlotCount = SlotCount, .DC = dataCenter};
        GenerateNodesForBundle(input, "bigd", 3, flags);
    }

    zoneInfo->DataCenters[offlineDC]->Forbidden = true;

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(6, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));

    THashSet<TString> assigningDC;
    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeAssignments) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        assigningDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(2, std::ssize(assigningDC));
    dataCenters.erase(offlineDC);
    EXPECT_EQ(assigningDC, dataCenters);
}

TEST(TDataCentersPriority, PerBundleForbidden)
{
    constexpr int SlotCount = 5;

    auto input = GenerateMultiDCInputContext(9, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];

    // Add new node to bundle
    TString offlineDC = *GetRandomElements(dataCenters, 1).begin();

    input.Bundles["bigd"]->ForbiddenDataCenters = { offlineDC };

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = false, .SlotCount = SlotCount, .DC = dataCenter};
        GenerateNodesForBundle(input, "bigd", 3, flags);
    }


    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(6, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));

    THashSet<TString> assigningDC;
    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeAssignments) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        assigningDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(2, std::ssize(assigningDC));
    dataCenters.erase(offlineDC);
    EXPECT_EQ(assigningDC, dataCenters);

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "bundle_has_forbidden_dc");
}

TEST(TDataCentersPriority, DisruptionMinimizing)
{
    constexpr int SlotCount = 5;
    auto input = GenerateMultiDCInputContext(9, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];

    // Add new node to bundle
    TString offlineDC = *GetRandomElements(dataCenters, 1).begin();

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = false, .SlotCount = SlotCount, .DC = dataCenter};
        auto nodes = GenerateNodesForBundle(input, "bigd", 3, flags);

        if (dataCenter == offlineDC) {
            continue;
        }

        for (auto& nodeName : GetRandomElements(nodes, 2)) {
            auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            nodeInfo->UserTags = { bundleNodeTagFilter };
            nodeInfo->Decommissioned = false;
        }
    }

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeAssignments));

    THashSet<TString> assigningDC;
    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeAssignments) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        assigningDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(2, std::ssize(assigningDC));
    dataCenters.erase(offlineDC);
    EXPECT_EQ(assigningDC, dataCenters);
}

TEST(TDataCentersPriority, MinimizingTabletMoves)
{
    constexpr int SlotCount = 5;
    auto input = GenerateMultiDCInputContext(9, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];

    // Add new node to bundle
    TString offlineDC = *GetRandomElements(dataCenters, 1).begin();

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = true, .SlotCount = SlotCount, .DC = dataCenter};
        auto nodes = GenerateNodesForBundle(input, "bigd", 3, flags);

        if (dataCenter == offlineDC) {
            continue;
        }

        for (auto& nodeName : nodes) {
            SetTabletSlotsState(input, nodeName, PeerStateLeading);
        }
    }

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeReleasements));

    THashSet<TString> releasingDC;
    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeReleasements) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        releasingDC.insert(*nodeInfo->Annotations->DataCenter);
    }

    // Releasing data centers are all from single datacenter.
    EXPECT_EQ(1, std::ssize(releasingDC));
    EXPECT_TRUE(releasingDC.count(offlineDC) != 0);
}


TEST(TDataCentersPriority, ChangeForbiddenSeveralTimes)
{
    constexpr int SlotCount = 5;

    auto input = GenerateMultiDCInputContext(9, SlotCount);
    auto dataCenters = THashSet<TString>{"dc-1", "dc-2", "dc-3"};
    input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
    auto bundleNodeTagFilter = input.Bundles["bigd"]->NodeTagFilter;

    auto zoneInfo = input.Zones["default-zone"];

    for (const auto& dataCenter : dataCenters) {
        auto flags = TGenerateNodeOptions{.SetFilterTag = true, .SlotCount = SlotCount, .DC = dataCenter};
        GenerateNodesForBundle(input, "bigd", 3, flags);
    }

    zoneInfo->DataCenters["dc-1"]->Forbidden = true;

    GenerateTabletCellsForBundle(input, "bigd", SlotCount * 6);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeReleasements));

    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeReleasements) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        EXPECT_EQ(nodeInfo->Annotations->DataCenter, "dc-1");
    }

    EXPECT_EQ(3, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(decommissioned);
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Decommissioned = decommissioned;
    }

    zoneInfo->DataCenters["dc-1"]->Forbidden = false;
    zoneInfo->DataCenters["dc-2"]->Forbidden = true;

    ApplyChangedStates(&input, mutations);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(6, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates.at("bigd")->BundleNodeReleasements));

    for (const auto& [nodeName, _] : mutations.ChangedStates.at("bigd")->BundleNodeReleasements) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        EXPECT_EQ(nodeInfo->Annotations->DataCenter, "dc-2");
    }

    for (const auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        EXPECT_EQ(decommissioned, nodeInfo->Annotations->DataCenter == "dc-2");
        nodeInfo->Decommissioned = decommissioned;
    }

    ApplyChangedStates(&input, mutations);
    ScheduleBundles(input, &mutations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellBalancer
