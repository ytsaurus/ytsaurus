#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/orchid_bindings.h>

#include <yt/yt/core/ytree/yson_serializable.h>

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

void ApplyChangedStates(TSchedulerInputState* schedulerState, const TSchedulerMutations& mutations)
{
    for (const auto& [bundleName, state] : mutations.ChangedStates) {
        schedulerState->BundleStates[bundleName] = NYTree::CloneYsonSerializable(state);
    }
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

    auto config = New<TBundleConfig>();
    bundleInfo->TargetConfig = config;
    config->TabletNodeCount = nodeCount;
    config->RpcProxyCount = proxyCount;
    config->TabletNodeResourceGuarantee = New<TInstanceResources>();
    config->TabletNodeResourceGuarantee->Vcpu = 9999;
    config->TabletNodeResourceGuarantee->Memory = 88_GB;
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
    input.Config->Cluster = "default-cluster";

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->YPCluster = "pre-pre";
        zoneInfo->TabletNodeNannyService = "nanny-bunny-tablet-nodes";
        zoneInfo->RpcProxyNannyService = "nanny-bunny-rpc-proxies";
    }

    SetBundleInfo(input, "default-bundle", nodeCount, writeThreadCount, proxyCount);

    return input;
}

void VerifyNodeAllocationRequests(const TSchedulerMutations& mutations, int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewAllocations));

    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_EQ(spec->NannyService, "nanny-bunny-tablet-nodes");
        EXPECT_FALSE(spec->PodIdTemplate.empty());
        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);
        EXPECT_EQ(spec->ResourceRequest->Vcpu, 9999);
        EXPECT_EQ(spec->ResourceRequest->MemoryMb, static_cast<i64>(88_GB / 1_MB));
    }
}

void VerifyProxyAllocationRequests(const TSchedulerMutations& mutations, int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewAllocations));

    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_EQ(spec->NannyService, "nanny-bunny-rpc-proxies");
        EXPECT_FALSE(spec->PodIdTemplate.empty());
        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeRpcProxy);
        EXPECT_EQ(spec->ResourceRequest->Vcpu, 1111);
        EXPECT_EQ(spec->ResourceRequest->MemoryMb, static_cast<i64>(18_GB / 1_MB));
    }
}

void VerifyNodeDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewDeallocations));

    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_FALSE(spec->PodId.empty());

        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);

        const auto& deallocationState = bundleState->NodeDeallocations[id];

        EXPECT_FALSE(deallocationState->InstanceName.empty());
        EXPECT_EQ(deallocationState->Strategy, DeallocationStrategyHulkRequest);
    }
}

void VerifyProxyDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewDeallocations));

    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_FALSE(spec->PodId.empty());

        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeRpcProxy);

        const auto& deallocationState = bundleState->ProxyDeallocations[id];
        EXPECT_FALSE(deallocationState->InstanceName.empty());
        EXPECT_EQ(deallocationState->Strategy, DeallocationStrategyHulkRequest);
    }
}

THashSet<TString> GenerateNodesForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int nodeCount,
    bool setFilterTag = false,
    int slotCount = 5,
    int instanceIndex = 170)
{
    THashSet<TString> result;

    const auto& zoneInfo = inputState.Zones.begin()->second;
    const auto& targetConfig = (bundleName == SpareBundleName)
        ? zoneInfo->SpareTargetConfig
        : GetOrCrash(inputState.Bundles, bundleName)->TargetConfig;

    for (int index = 0; index < nodeCount; ++index) {
        int nodeIndex = std::ssize(inputState.TabletNodes);
        auto nodeId = Format("seneca-ayt-%v-%v-%v-tab-%v.search.yandex.net",
            nodeIndex,
            bundleName,
            instanceIndex + index,
            inputState.Config->Cluster);
        auto nodeInfo = New<TTabletNodeInfo>();
        nodeInfo->Banned = false;
        nodeInfo->Decommissioned = false;
        nodeInfo->Host = Format("seneca-ayt-%v.search.yandex.net", nodeIndex);
        nodeInfo->State = "online";
        nodeInfo->Annotations->Allocated = true;
        nodeInfo->Annotations->NannyService = "nanny-bunny-tablet-nodes";
        nodeInfo->Annotations->YPCluster = "pre-pre";
        nodeInfo->Annotations->AllocatedForBundle = bundleName;
        nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        nodeInfo->Annotations->Resource = CloneYsonSerializable(targetConfig->TabletNodeResourceGuarantee);

        for (int index = 0; index < slotCount; ++index) {
            nodeInfo->TabletSlots.push_back(New<TTabletSlot>());
        }

        if (setFilterTag) {
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
    bool setRole = false)
{
    THashSet<TString> result;

    const auto& zoneInfo = inputState.Zones.begin()->second;
    const auto& targetConfig = (bundleName == SpareBundleName)
        ? zoneInfo->SpareTargetConfig
        : GetOrCrash(inputState.Bundles, bundleName)->TargetConfig;

    for (int index = 0; index < proxyCount; ++index) {
        int proxyIndex = std::ssize(inputState.RpcProxies);
        auto proxyName = Format("seneca-ayt-%v-%v-aa-proxy-%v.search.yandex.net",
            proxyIndex,
            bundleName,
            inputState.Config->Cluster);
        auto proxyInfo = New<TRpcProxyInfo>();
        proxyInfo->Alive = New<TRpcProxyAlive>();
        proxyInfo->Annotations->Allocated = true;
        proxyInfo->Annotations->NannyService = "nanny-bunny-rpc-proxies";
        proxyInfo->Annotations->YPCluster = "pre-pre";
        proxyInfo->Annotations->AllocatedForBundle = bundleName;
        proxyInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        proxyInfo->Annotations->Resource = CloneYsonSerializable(targetConfig->RpcProxyResourceGuarantee);

        if (setRole) {
            proxyInfo->Role = bundleName;
        }

        inputState.RpcProxies[proxyName] = proxyInfo;
        result.insert(proxyName);
    }

    return result;
}

void SetTabletSlotsState(TSchedulerInputState& inputState, const TString& nodeName, const TString& state)
{
    const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);

    for (const auto& slot : nodeInfo->TabletSlots) {
        slot->State = state;
    }
}

void GenerateNodeAllocationsForBundle(TSchedulerInputState& inputState, const TString& bundleName, int count)
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
        state->NodeAllocations[requestId] = requestState;

        inputState.AllocationRequests[requestId] = New<TAllocationRequest>();
        auto& spec = inputState.AllocationRequests[requestId]->Spec;
        spec->NannyService = "nanny-bunny-tablet-nodes";
        spec->YPCluster = "pre-pre";
        spec->ResourceRequest->Vcpu = 9999;
        spec->ResourceRequest->MemoryMb = 88_GB / 1_MB;
        spec->PodIdTemplate = podIdTemplate;
    }
}

void GenerateProxyAllocationsForBundle(TSchedulerInputState& inputState, const TString& bundleName, int count)
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
        state->ProxyAllocations[requestId] = requestState;

        inputState.AllocationRequests[requestId] = New<TAllocationRequest>();
        auto& spec = inputState.AllocationRequests[requestId]->Spec;
        spec->NannyService = "nanny-bunny-rpc-proxies";
        spec->YPCluster = "pre-pre";
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
        cellInfo->TabletCount = 2;
        cellInfo->TabletCellBundle = bundleName;
        cellInfo->Peers.resize(peerCount, New<TTabletCellPeer>());
        bundleInfo->TabletCellIds.push_back(cellId);
        inputState.TabletCells[cellId] = cellInfo;
    }
}

void GenerateNodeDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    const std::vector<TString>& nodeNames)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (const auto& nodeName : nodeNames) {
        const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);
        nodeInfo->Decommissioned = true;
        SetTabletSlotsState(inputState, nodeName, TabletSlotStateEmpty);

        auto requestId = Format("dealloc-%v", state->NodeAllocations.size());

        auto deallocationState = New<TDeallocationRequestState>();
        state->NodeDeallocations[requestId] = deallocationState;
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->InstanceName = nodeName;
        deallocationState->Strategy = DeallocationStrategyHulkRequest;
        deallocationState->HulkRequestCreated = true;

        inputState.DeallocationRequests[requestId] = New<TDeallocationRequest>();
        auto& spec = inputState.DeallocationRequests[requestId]->Spec;
        spec->YPCluster = "pre-pre";
        spec->PodId = "random_pod_id";
    }
}

void GenerateProxyDeallocationsForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    const std::vector<TString>& proxyNames)
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
        spec->YPCluster = "pre-pre";
        spec->PodId = "random_pod_id";
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, AllocationCreated)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 1, false, 5, 2);
    GenerateNodeAllocationsForBundle(input, "default-bundle", 1);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 3);

    EXPECT_EQ(4, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    THashSet<TString> templates;
    for (auto& [allocId, request] : mutations.NewAllocations) {
        templates.insert(request->Spec->PodIdTemplate);

        auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingTabletNodes, allocId);
        EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
        EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "REQUEST_CREATED");
        EXPECT_FALSE(orchidAllocatingInfo->InstanceInfo);
    }

    EXPECT_EQ(templates.size(), 3u);
    EXPECT_TRUE(templates.count(GetInstancePodIdTemplate(input.Config->Cluster, "default-bundle", "tab", 3)));
    EXPECT_TRUE(templates.count(GetInstancePodIdTemplate(input.Config->Cluster, "default-bundle", "tab", 4)));
    EXPECT_TRUE(templates.count(GetInstancePodIdTemplate(input.Config->Cluster, "default-bundle", "tab", 5)));
}

TEST(TBundleSchedulerTest, AllocationQuotaExceeded)
{
    auto input = GenerateSimpleInputContext(5);

    {
        auto& bundleInfo = input.Bundles["default-bundle"];
        bundleInfo->ResourceQuota = New<TResourceQuota>();
        bundleInfo->ResourceQuota->Cpu = 0.1;
        bundleInfo->ResourceQuota->Memory = 10_TB;

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(mutations.AlertsToFire.front().Id, "bundle_resource_quota_exceeded");
    }

    {
        auto& bundleInfo = input.Bundles["default-bundle"];
        bundleInfo->ResourceQuota->Cpu = 100;
        bundleInfo->ResourceQuota->Memory = 1;

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(mutations.AlertsToFire.front().Id, "bundle_resource_quota_exceeded");
    }

    auto& bundleInfo = input.Bundles["default-bundle"];
    bundleInfo->ResourceQuota->Cpu = 100;
    bundleInfo->ResourceQuota->Memory = 10_TB;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 5);

    EXPECT_EQ(5, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(2);

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateNodeAllocationsForBundle(input, "default-bundle", 1);

    const TString nodeId = input.TabletNodes.begin()->first;
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
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->NodeAllocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangeNodeAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
        EXPECT_EQ(annotations->YPCluster, "pre-pre");
        EXPECT_EQ(annotations->AllocatedForBundle, "default-bundle");
        EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
        EXPECT_EQ(annotations->NannyService, "nanny-bunny-tablet-nodes");
        EXPECT_EQ(annotations->Resource->Vcpu, 9999);
        EXPECT_EQ(annotations->Resource->Memory, static_cast<i64>(88_GB));
        EXPECT_TRUE(annotations->Allocated);
        EXPECT_FALSE(annotations->DeallocatedAt);

        auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");
        for (auto& [allocId, allocState] : input.BundleStates["default-bundle"]->NodeAllocations) {
            auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingTabletNodes, allocId);
            EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
            EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "COMPLETED");
            EXPECT_TRUE(orchidAllocatingInfo->InstanceInfo);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->YPCluster, "pre-pre");
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
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_NO_THROW(Orchid::GetBundlesInfo(input, mutations));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateNodeAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(mutations.ChangedStates.count("default-bundle"), 0u);
    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));

    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_allocation_failed");
    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackCompletedButNoNode)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateNodeAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->NodeAllocations));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateNodeAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& allocState = input.BundleStates["default-bundle"]->NodeAllocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_allocation");
}

TEST(TBundleSchedulerTest, DoNotCreateNewDeallocationsWhileInProgress)
{
    auto input = GenerateSimpleInputContext(2, DefaultCellCount);
    auto nodes = GenerateNodesForBundle(input, "default-bundle", 5, SetNodeTagFilters, DefaultCellCount);
    GenerateNodeDeallocationsForBundle(input, "default-bundle", { *nodes.begin()});

    EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->NodeDeallocations));
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));
}

TEST(TBundleSchedulerTest, DoNotCreateNewDeallocationsIfNodesAreNotReady)
{
    constexpr int TabletSlotsCount = 10;

    auto input = GenerateSimpleInputContext(2, TabletSlotsCount);
    GenerateTabletCellsForBundle(input, "default-bundle", 2 * TabletSlotsCount);

    // Do not deallocate nodes if node tag filter is not set for all alive nodes
    GenerateNodesForBundle(input, "default-bundle", 5, !SetNodeTagFilters, TabletSlotsCount);
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    // Do not deallocate nodes if cell cout is not actual for all the nodes
    input.TabletNodes.clear();
    GenerateNodesForBundle(input, "default-bundle", 5, SetNodeTagFilters, TabletSlotsCount / 2);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    // Finally init deallocations if nodes are up to date.
    input.TabletNodes.clear();
    GenerateNodesForBundle(input, "default-bundle", 5, SetNodeTagFilters, TabletSlotsCount);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));
}

TEST(TBundleSchedulerTest, CreateNewDeallocations)
{
    constexpr int TabletSlotsCount = 10;

    auto input = GenerateSimpleInputContext(2, TabletSlotsCount);
    GenerateTabletCellsForBundle(input, "default-bundle", 2 * TabletSlotsCount);
    GenerateNodesForBundle(input, "default-bundle", 5, SetNodeTagFilters, TabletSlotsCount);

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    for (auto& [nodeName, state] : mutations.ChangedStates["default-bundle"]->NodeDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);
        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedTabletNodes, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedDecommissionedFlag));

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
    auto& bundleState = mutations.ChangedStates["default-bundle"];
    VerifyNodeDeallocationRequests(mutations, bundleState, 3);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));

    for (auto& [_, state] : mutations.ChangedStates["default-bundle"]->NodeDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);
    }
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(1);
    TSchedulerMutations mutations;

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateNodeDeallocationsForBundle(input, "default-bundle", { *bundleNodes.begin()});

    {
        auto& request = input.DeallocationRequests.begin()->second;
        request->Status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_deallocation_failed");
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(1);

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    const TString nodeId = *bundleNodes.begin();

    GenerateNodeDeallocationsForBundle(input, "default-bundle", {nodeId});

    {
        auto& request = input.DeallocationRequests.begin()->second;
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
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->NodeDeallocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangeNodeAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
        EXPECT_TRUE(annotations->YPCluster.empty());
        EXPECT_TRUE(annotations->AllocatedForBundle.empty());
        EXPECT_TRUE(annotations->NannyService.empty());
        EXPECT_FALSE(annotations->Allocated);
        EXPECT_TRUE(annotations->DeallocatedAt);
        EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
        EXPECT_TRUE(TInstant::Now() - *annotations->DeallocatedAt < TDuration::Minutes(10));

        input.TabletNodes[nodeId]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));
    VerifyNodeAllocationRequests(mutations, 0);
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(1);
    TSchedulerMutations mutations;

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    const TString nodeId = *bundleNodes.begin();

    GenerateNodeDeallocationsForBundle(input, "default-bundle", {nodeId});

    {
        auto& allocState = input.BundleStates["default-bundle"]->NodeDeallocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_deallocation");
}

TEST(TBundleSchedulerTest, CreateNewCellsCreation)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 3);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(1, std::ssize(mutations.CellsToCreate));

    EXPECT_EQ(7, mutations.CellsToCreate.at("default-bundle"));
}

TEST(TBundleSchedulerTest, CreateNewCellsNoRemoveNoCreate)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
}

TEST(TBundleSchedulerTest, CreateNewCellsRemove)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 13);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(3, std::ssize(mutations.CellsToRemove));
}

TEST(TBundleSchedulerTest, PeekRightCellToRemove)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 11);

    auto cellId = input.Bundles["default-bundle"]->TabletCellIds[RandomNumber<ui32>(11)];
    input.TabletCells[cellId]->TabletCount = 0;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(1, std::ssize(mutations.CellsToRemove));

    EXPECT_EQ(cellId, mutations.CellsToRemove.front());
}

TEST(TBundleSchedulerTest, TestSpareNodesAllocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(3, std::ssize(mutations.NewAllocations));
}

TEST(TBundleSchedulerTest, TestSpareNodesDeallocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];

    zoneInfo->SpareTargetConfig->TabletNodeCount = 2;
    GenerateNodesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates[SpareBundleName]->NodeDeallocations));
}

////////////////////////////////////////////////////////////////////////////////

void CheckEmptyAlerts(const TSchedulerMutations& mutations)
{
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));

    for (const auto& alert : mutations.AlertsToFire) {
        EXPECT_EQ("", alert.Id);
        EXPECT_EQ("", alert.Description);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TNodeTagsFilterManager, TestBundleWithNoTagFilter)
{
    auto input = GenerateSimpleInputContext(2, 5);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;
    input.Bundles["default-bundle"]->NodeTagFilter = {};

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ("bundle_with_no_tag_filter", mutations.AlertsToFire.front().Id);

    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
}

TEST(TNodeTagsFilterManager, TestBundleNodeTagsAssigned)
{
    auto input = GenerateSimpleInputContext(2, 5);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
}

TEST(TNodeTagsFilterManager, TestBundleNodesWithSpare)
{
    const bool SetNodeFilterTag = true;
    const int SlotCount = 5;

    auto input = GenerateSimpleInputContext(2, SlotCount);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;

    GenerateNodesForBundle(input, "default-bundle", 1, SetNodeFilterTag, SlotCount);
    GenerateTabletCellsForBundle(input, "default-bundle", 15);

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, false, SlotCount);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2, std::ssize(mutations.ChangedNodeUserTags));

    const TString BundleNodeTagFilter = input.Bundles["default-bundle"]->NodeTagFilter;

    THashSet<TString> usedSpare;

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_FALSE(mutations.ChangedDecommissionedFlag.at(nodeName));
        EXPECT_TRUE(tags.find(BundleNodeTagFilter) != tags.end());
        EXPECT_TRUE(spareNodes.find(nodeName) != spareNodes.end());

        usedSpare.insert(nodeName);
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    EXPECT_EQ(2, std::ssize(usedSpare));

    // Populate slots with cell peers.
    for (const auto& spareNode : usedSpare) {
        SetTabletSlotsState(input, spareNode, PeerStateLeading);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Add new node to bundle
    auto newNodes = GenerateNodesForBundle(input, "default-bundle", 1, SetNodeFilterTag, SlotCount);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(1, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    TString spareNodeToRelease;

    for (const auto& [nodeName, decommission] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(usedSpare.count(nodeName) != 0);
        EXPECT_TRUE(decommission);
        input.TabletNodes[nodeName]->Decommissioned = decommission;
        spareNodeToRelease = nodeName;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Populate slots with cell peers.
    SetTabletSlotsState(input, spareNodeToRelease, TabletSlotStateEmpty);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(1, std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_EQ(spareNodeToRelease, nodeName);
        EXPECT_TRUE(tags.count(BundleNodeTagFilter) == 0);
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
}

TEST(TNodeTagsFilterManager, TestBundleNodesGracePeriod)
{
    const bool SetNodeFilterTag = true;
    const int SlotCount = 5;
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateSimpleInputContext(2, SlotCount);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;

    auto nodes = GenerateNodesForBundle(input, "default-bundle", 2, SetNodeFilterTag, SlotCount);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    auto spareNodes = GenerateNodesForBundle(input, SpareBundleName, 3, false, SlotCount);

    for (const auto& nodeName : nodes) {
        auto& tabletInfo = GetOrCrash(input.TabletNodes, nodeName);
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Checking that grace period does not affect spare nodes assignments
    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2, std::ssize(mutations.ChangedNodeUserTags));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, CheckDisruptedState)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, SpareBundleName, 3);
    GenerateNodesForBundle(input, "default-bundle", 4);

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->State = InstanceStateOffline;
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST(TBundleSchedulerTest, CheckAllocationLimit)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, SpareBundleName, 3);
    GenerateNodesForBundle(input, "default-bundle", 4);

    zoneInfo->MaxTabletNodeCount = 5;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
}

TEST(TBundleSchedulerTest, CheckDynamicConfig)
{
    auto input = GenerateSimpleInputContext(5, 5);
    input.Bundles["default-bundle"]->EnableTabletNodeDynamicConfig = true;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, SpareBundleName, 3);
    GenerateNodesForBundle(input, "default-bundle", 5);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Check that new dynamic config is set for bundles.
    EXPECT_TRUE(mutations.DynamicConfig);

    input.DynamicConfig = *mutations.DynamicConfig;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config did not change.
    EXPECT_FALSE(mutations.DynamicConfig);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    input.Bundles["default-bundle"]->TargetConfig->CpuLimits->WriteThreadPoolSize = 212;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config is changed.
    EXPECT_TRUE(mutations.DynamicConfig);
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

TEST(TBundleSchedulerTest, ProxyAllocationCreated)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 5);
    TSchedulerMutations mutations;

    GenerateProxiesForBundle(input, "default-bundle", 1);
    GenerateProxyAllocationsForBundle(input, "default-bundle", 1);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyProxyAllocationRequests(mutations, 3);

    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
    EXPECT_EQ(4, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyAllocations));
}

TEST(TBundleSchedulerTest, ProxyAllocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);

    GenerateProxiesForBundle(input, "default-bundle", 2);
    GenerateProxyAllocationsForBundle(input, "default-bundle", 1);

    const TString proxyName = input.RpcProxies.begin()->first;
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
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->ProxyAllocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangedProxyAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangedProxyAnnotations, proxyName);
        EXPECT_EQ(annotations->YPCluster, "pre-pre");
        EXPECT_EQ(annotations->AllocatedForBundle, "default-bundle");
        EXPECT_EQ(annotations->DeallocationStrategy, DeallocationStrategyHulkRequest);
        EXPECT_EQ(annotations->NannyService, "nanny-bunny-rpc-proxies");
        EXPECT_EQ(annotations->Resource->Vcpu, 1111);
        EXPECT_EQ(annotations->Resource->Memory, static_cast<i64>(18_GB));
        EXPECT_TRUE(annotations->Allocated);
        EXPECT_FALSE(annotations->DeallocatedAt);

        auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");
        for (auto& [allocId, allocState] : input.BundleStates["default-bundle"]->ProxyAllocations) {
            auto orchidAllocatingInfo = GetOrCrash(orchidInfo->AllocatingRpcProxies, allocId);
            EXPECT_FALSE(orchidAllocatingInfo->HulkRequestLink.empty());
            EXPECT_EQ(orchidAllocatingInfo->HulkRequestState, "COMPLETED");
            EXPECT_TRUE(orchidAllocatingInfo->InstanceInfo);
            EXPECT_EQ(orchidAllocatingInfo->InstanceInfo->YPCluster, "pre-pre");
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
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyAnnotations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST(TBundleSchedulerTest, ProxyAllocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    TSchedulerMutations mutations;

    GenerateProxiesForBundle(input, "default-bundle", 2);
    GenerateProxyAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_allocation_failed");
}

TEST(TBundleSchedulerTest, ProxyAllocationProgressTrackCompletedButNoProxy)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    TSchedulerMutations mutations;

    GenerateProxiesForBundle(input, "default-bundle", 2);
    GenerateProxyAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->PodId = "non-existing-pod";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->ProxyAllocations));
}

TEST(TBundleSchedulerTest, ProxyAllocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    TSchedulerMutations mutations;

    GenerateProxiesForBundle(input, "default-bundle", 2);
    GenerateProxyAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& allocState = input.BundleStates["default-bundle"]->ProxyAllocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->PodId = "non-existing-pod";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_allocation");

    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));
}

TEST(TBundleSchedulerTest, ProxyCreateNewDeallocations)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    GenerateProxiesForBundle(input, "default-bundle", 5);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    for (auto& [_, state] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are created.
    auto& bundleState = mutations.ChangedStates["default-bundle"];
    VerifyProxyDeallocationRequests(mutations, bundleState, 3);
    orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    for (auto& [_, state] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }
}

TEST(TBundleSchedulerTest, ProxyCreateNewDeallocationsLegacyInstancies)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    GenerateProxiesForBundle(input, "default-bundle", 5);

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Annotations->DeallocationStrategy.clear();
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyDeallocations));

    auto orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    for (auto& [_, state] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_FALSE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are created.
    auto& bundleState = mutations.ChangedStates["default-bundle"];
    VerifyProxyDeallocationRequests(mutations, bundleState, 3);
    orchidInfo = GetOrCrash(Orchid::GetBundlesInfo(input, mutations), "default-bundle");

    for (auto& [_, state] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_TRUE(state->HulkRequestCreated);

        auto orchidInstanceInfo = GetOrCrash(orchidInfo->AllocatedRpcProxies, state->InstanceName);
        EXPECT_TRUE(*orchidInstanceInfo->Removing);
    }
}

TEST(TBundleSchedulerTest, ProxyDeallocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 1);
    TSchedulerMutations mutations;

    auto bundleProxies = GenerateProxiesForBundle(input, "default-bundle", 1);
    GenerateProxyDeallocationsForBundle(input, "default-bundle", { *bundleProxies.begin()});

    {
        auto& request = input.DeallocationRequests.begin()->second;
        request->Status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    // BundleController state did not change
    EXPECT_EQ(0u, mutations.ChangedStates.count("default-bundle"));
    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "instance_deallocation_failed");
}

TEST(TBundleSchedulerTest, ProxyDeallocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 1);

    auto bundleProxies = GenerateProxiesForBundle(input, "default-bundle", 2);
    const TString proxyName = *bundleProxies.begin();

    GenerateProxyDeallocationsForBundle(input, "default-bundle", {proxyName});

    {
        auto& request = input.DeallocationRequests.begin()->second;
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
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->ProxyDeallocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangedProxyAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangedProxyAnnotations, proxyName);
        EXPECT_TRUE(annotations->YPCluster.empty());
        EXPECT_TRUE(annotations->AllocatedForBundle.empty());
        EXPECT_TRUE(annotations->NannyService.empty());
        EXPECT_FALSE(annotations->Allocated);

        EXPECT_TRUE(annotations->DeallocatedAt);
        EXPECT_TRUE(TInstant::Now() - *annotations->DeallocatedAt < TDuration::Minutes(10));

        input.RpcProxies[proxyName]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyAnnotations));
}

TEST(TBundleSchedulerTest, ProxyDeallocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 1);
    TSchedulerMutations mutations;

    auto bundleProxies = GenerateProxiesForBundle(input, "default-bundle", 2);
    const TString proxyName = *bundleProxies.begin();

    GenerateProxyDeallocationsForBundle(input, "default-bundle", {proxyName});

    {
        auto& allocState = input.BundleStates["default-bundle"]->ProxyDeallocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_instance_deallocation");
}

TEST(TBundleSchedulerTest, TestSpareProxiesAllocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(3, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates[SpareBundleName]->ProxyAllocations));
}

TEST(TBundleSchedulerTest, TestSpareProxyDeallocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];

    zoneInfo->SpareTargetConfig->RpcProxyCount = 2;
    GenerateProxiesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates[SpareBundleName]->ProxyDeallocations));
}

TEST(TBundleSchedulerTest, CheckProxyZoneDisruptedState)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    GenerateProxiesForBundle(input, SpareBundleName, 3);
    GenerateProxiesForBundle(input, "default-bundle", 4);

    for (auto& [_, proxyInfo] : input.RpcProxies) {
        proxyInfo->Alive.Reset();
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
}

TEST(TBundleSchedulerTest, ProxyCheckAllocationLimit)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->RpcProxyCount = 3;
    GenerateProxiesForBundle(input, SpareBundleName, 3);
    GenerateProxiesForBundle(input, "default-bundle", 4);

    zoneInfo->MaxRpcProxyCount = 5;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProxyRoleManagement, TestBundleProxyRolesAssigned)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 2);
    input.Bundles["default-bundle"]->EnableRpcProxyManagement = true;

    GenerateProxiesForBundle(input, "default-bundle", 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(2, std::ssize(mutations.ChangedProxyRole));

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        ASSERT_EQ(role, "default-bundle");
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
    input.Bundles["default-bundle"]->EnableRpcProxyManagement = true;

    auto bundleProxies = GenerateProxiesForBundle(input, "default-bundle", 3, SetProxyRole);

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
        EXPECT_EQ(role, "default-bundle");
        EXPECT_TRUE(spareProxies.find(proxyName) != spareProxies.end());
    }
}

TEST(TProxyRoleManagement, TestBundleProxyRolesWithSpare)
{
    const bool SetProxyRole = true;

    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 3);
    input.Bundles["default-bundle"]->EnableRpcProxyManagement = true;

    GenerateProxiesForBundle(input, "default-bundle", 1, SetProxyRole);

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
        EXPECT_EQ(role, "default-bundle");
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
    auto newProxies = GenerateProxiesForBundle(input, "default-bundle", 1, SetProxyRole);;

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    for (const auto& [proxyName, role] : mutations.ChangedProxyRole) {
        EXPECT_TRUE(usedSpare.count(proxyName) != 0);
        input.RpcProxies[proxyName]->Role = role;
    }
    EXPECT_EQ(1, std::ssize(mutations.ChangedProxyRole));

    // Check no more changes
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedProxyRole));
}

////////////////////////////////////////////////////////////////////////////////

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

TEST(TBundleSchedulerTest, CheckSystemAccountLimit)
{
    auto input = GenerateSimpleInputContext(2, 5);

    input.RootSystemAccount = New<TSystemAccount>();
    auto& bundleInfo1 = input.Bundles["default-bundle"];

    bundleInfo1->Options->ChangelogAccount = "default-bundle-account";
    bundleInfo1->Options->SnapshotAccount = "default-bundle-account";
    bundleInfo1->Options->ChangelogPrimaryMedium = "ssd_journal";
    bundleInfo1->Options->SnapshotPrimaryMedium = "default";
    bundleInfo1->EnableSystemAccountManagement = true;

    input.SystemAccounts["default-bundle-account"] = New<TSystemAccount>();

    input.Config->QuotaMultiplier = 1.5;
    input.Config->ChunkCountPerCell = 2;
    input.Config->NodeCountPerCell = 3;
    input.Config->JournalDiskSpacePerCell = 5_MB;
    input.Config->SnapshotDiskSpacePerCell = 7_MB;
    input.Config->MinNodeCount = 9;
    input.Config->MinChunkCount = 7;

    GenerateNodesForBundle(input, "default-bundle", 2);
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
    input.SystemAccounts["default-bundle-account"]->ResourceLimits = NYTree::CloneYsonSerializable(mutations.LiftedSystemAccountLimit["default-bundle-account"]);
    input.RootSystemAccount->ResourceLimits = NYTree::CloneYsonSerializable(mutations.ChangedRootSystemAccountLimit);
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
            .Nodes = 900,
            .Chunks = 600,
            .SsdBlobs = 2100_MB,
            .SsdJournal = 1500_MB
        },
        mutations.LiftedSystemAccountLimit["default-bundle2-account"]);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 1927,
            .Chunks = 2618,
            .SsdBlobs = 2100_MB,
            .Default = 64_MB,
            .SsdJournal = 1545_MB
        },
        mutations.ChangedRootSystemAccountLimit);

    // Test account actual cells count
    GenerateTabletCellsForBundle(input, "default-bundle2", 300);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckLimits(
        TExpectedLimits{
            .Nodes = 1350,
            .Chunks = 900,
            .SsdBlobs = 3150_MB,
            .SsdJournal = 2250_MB
        },
        mutations.LiftedSystemAccountLimit["default-bundle2-account"]);
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

TEST(TBundleSchedulerTest, ReAllocateOutdatedNodes)
{
    auto input = GenerateSimpleInputContext(5);
    input.Config->ReallocateInstanceBudget = 2;
    GenerateNodesForBundle(input, "default-bundle", 5, false, 5, 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes)
    {
        nodeInfo->Annotations->Resource->Vcpu /= 2;
        EXPECT_TRUE(nodeInfo->Annotations->Resource->Vcpu);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 2);
}

TEST(TBundleSchedulerTest, ReAllocateOutdatedProxies)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 5);
    input.Config->ReallocateInstanceBudget = 4;
    GenerateProxiesForBundle(input, "default-bundle", 5);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, proxyInfo] : input.RpcProxies)
    {
        proxyInfo->Annotations->Resource->Memory /= 2;
        EXPECT_TRUE(proxyInfo->Annotations->Resource->Memory);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyProxyAllocationRequests(mutations, 4);
}

THashSet<TString> GetRandomElements(const auto& collection, int count)
{
    std::vector<TString> nodesToRemove(collection.begin(), collection.end());
    Shuffle(nodesToRemove.begin(), nodesToRemove.end());
    nodesToRemove.resize(count);
    return {nodesToRemove.begin(), nodesToRemove.end()};
}

TEST(TBundleSchedulerTest, DeallocateOutdatedNodes)
{
    auto input = GenerateSimpleInputContext(10, DefaultCellCount);
    auto nodeNames = GenerateNodesForBundle(input, "default-bundle", 13, SetNodeTagFilters, DefaultCellCount);

    // Mark random nodes as outdated
    auto nodesToRemove = GetRandomElements(nodeNames, 3);
    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Annotations->Resource->Memory *= 2;
        EXPECT_TRUE(nodeInfo->Annotations->Resource->Memory);
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    // Verify that only outdated nodes are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["default-bundle"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }
}

TEST(TBundleSchedulerTest, DeallocateOutdatedProxies)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 10);
    auto proxyNames = GenerateProxiesForBundle(input, "default-bundle", 13);

    // Mark random proxies as outdated
    auto proxiesToRemove = GetRandomElements(proxyNames, 3);
    for (auto& proxyName : proxiesToRemove) {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        proxyInfo->Annotations->Resource->Vcpu *= 2;
        EXPECT_TRUE(proxyInfo->Annotations->Resource->Vcpu);
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyDeallocations));

    // Verify that only outdated proxies are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_TRUE(proxiesToRemove.count(deallocation->InstanceName));
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, ReallocateNodesUnderMaintenance)
{
    auto input = GenerateSimpleInputContext(5);
    input.Config->ReallocateInstanceBudget = 2;
    GenerateNodesForBundle(input, "default-bundle", 5, false, 5, 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, nodeInfo] : input.TabletNodes)
    {
        nodeInfo->MaintenanceRequests["test_service2"] = New<TMaintenanceRequest>();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 2);
}

TEST(TBundleSchedulerTest, ReallocateProxiesUnderMaintenance)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 5);
    input.Config->ReallocateInstanceBudget = 4;
    GenerateProxiesForBundle(input, "default-bundle", 5);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    for (auto& [_, proxyInfo] : input.RpcProxies)
    {
        proxyInfo->MaintenanceRequests["test_service"] = New<TMaintenanceRequest>();
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyProxyAllocationRequests(mutations, 4);
}

TEST(TBundleSchedulerTest, ReallocateNodeUnderMaintenanceAndOutdated)
{
    auto input = GenerateSimpleInputContext(5);
    input.Config->ReallocateInstanceBudget = 2;
    GenerateNodesForBundle(input, "default-bundle", 5, false, 5, 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    {
        auto nodeInfo = input.TabletNodes.begin()->second;
        nodeInfo->MaintenanceRequests["test_service"] = New<TMaintenanceRequest>();
        nodeInfo->Annotations->Resource->Vcpu /= 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyNodeAllocationRequests(mutations, 1);
}

TEST(TBundleSchedulerTest, DeallocateNodesUnderMaintenance)
{
    auto input = GenerateSimpleInputContext(10, DefaultCellCount);
    auto nodeNames = GenerateNodesForBundle(input, "default-bundle", 13, SetNodeTagFilters, DefaultCellCount);

    auto nodesToRemove = GetRandomElements(nodeNames, 3);
    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->MaintenanceRequests["test_service"] = New<TMaintenanceRequest>();
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    // Verify that only nodes under maintenance are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["default-bundle"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        EXPECT_TRUE(!deallocation->InstanceName.empty());
    }
}

TEST(TBundleSchedulerTest, DeallocateProxiesUnderMaintenance)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 10);
    auto proxyNames = GenerateProxiesForBundle(input, "default-bundle", 13);

    // Mark random proxies as outdated
    auto proxiesToRemove = GetRandomElements(proxyNames, 3);
    for (auto& proxyName : proxiesToRemove) {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        proxyInfo->MaintenanceRequests["test_service"] = New<TMaintenanceRequest>();
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->ProxyDeallocations));

    // Verify that only outdated proxies are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["default-bundle"]->ProxyDeallocations) {
        EXPECT_TRUE(proxiesToRemove.count(deallocation->InstanceName));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, RemoveProxyCypressNodes)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 10);
    auto proxyNames = GenerateProxiesForBundle(input, "default-bundle", 13);

    const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

    auto proxiesToRemove = GetRandomElements(proxyNames, 3);
    for (auto& proxyName : proxiesToRemove) {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        proxyInfo->Annotations->Allocated = false;
        proxyInfo->Annotations->DeallocatedAt = DateInThePast;
        proxyInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(3, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));

    for (const auto& proxyName : mutations.ProxiesToCleanup) {
        EXPECT_TRUE(proxiesToRemove.count(proxyName));
    }
}

TEST(TBundleSchedulerTest, RemoveTabletNodeCypressNodes)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 10);
    auto nodeNames = GenerateNodesForBundle(input, "default-bundle", 13);

    const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

    auto nodesToRemove = GetRandomElements(nodeNames, 3);
    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Annotations->Allocated = false;
        nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyHulkRequest;
        nodeInfo->Annotations->DeallocatedAt = DateInThePast;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(3, std::ssize(mutations.NodesToCleanup));

    for (const auto& nodeName : mutations.NodesToCleanup) {
        EXPECT_TRUE(nodesToRemove.count(nodeName));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, CheckBundleShortName)
{
    auto input = GenerateSimpleInputContext(5);
    auto bundleInfo = GetOrCrash(input.Bundles, "default-bundle");
    bundleInfo->ShortName = "short-xyz";

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(5, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(5, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));

    THashSet<TString> templates;
    for (auto& [_, request] : mutations.NewAllocations) {
        templates.insert(request->Spec->PodIdTemplate);
        EXPECT_TRUE(request->Spec->PodIdTemplate.find("short-xyz") != std::string::npos);
        EXPECT_TRUE(request->Spec->PodIdTemplate.find("default-bundle") == std::string::npos);
    }

    EXPECT_EQ(templates.size(), 5u);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, OfflineInstanceGracePeriod)
{
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);

    auto input = GenerateSimpleInputContext(5);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    auto nodes = GenerateNodesForBundle(input, "default-bundle", 5, false, 5, 2);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));

    for (const auto& nodeName : nodes) {
        auto& tabletInfo = GetOrCrash(input.TabletNodes, nodeName);
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod / 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));

    for (const auto& nodeName : nodes) {
        auto& tabletInfo = GetOrCrash(input.TabletNodes, nodeName);
        tabletInfo->State = InstanceStateOffline;
        tabletInfo->LastSeenTime = TInstant::Now() - OfflineInstanceGracePeriod * 2;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(5, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(5, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, CheckResourceLimits)
{
    const auto OfflineInstanceGracePeriod = TDuration::Minutes(40);
    auto input = GenerateSimpleInputContext(5);
    input.Config->OfflineInstanceGracePeriod = OfflineInstanceGracePeriod;
    auto nodes = GenerateNodesForBundle(input, "default-bundle", 5, false, 5, 2);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, SpareBundleName, 3);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedTabletStaticMemory));

    auto& bundleInfo = input.Bundles["default-bundle"];
    bundleInfo->TargetConfig->MemoryLimits->TabletStatic = 10_GB;

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(1, std::ssize(mutations.ChangedTabletStaticMemory));
    EXPECT_EQ(static_cast<i64>(50_GB), mutations.ChangedTabletStaticMemory["default-bundle"]);

    bundleInfo->ResourceLimits->TabletStaticMemory = 50_GB;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedTabletStaticMemory));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, DeallocateAdoptedNodes)
{
    auto input = GenerateSimpleInputContext(10, DefaultCellCount);
    auto nodeNames = GenerateNodesForBundle(input, "default-bundle", 13, SetNodeTagFilters, DefaultCellCount);

    // Mark random nodes as outdated
    auto nodesToRemove = GetRandomElements(nodeNames, 3);
    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Annotations->Resource->Memory *= 2;
        nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyReturnToBB;
        EXPECT_TRUE(nodeInfo->Annotations->Resource->Memory);
        nodeInfo->EnableBundleBalancer = false;
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));

    // Verify that only outdated nodes are peeked
    for (const auto& [_, deallocation] : mutations.ChangedStates["default-bundle"]->NodeDeallocations) {
        EXPECT_TRUE(nodesToRemove.count(deallocation->InstanceName));
        EXPECT_TRUE(!deallocation->InstanceName.empty());
        EXPECT_EQ(deallocation->Strategy, DeallocationStrategyReturnToBB);
    }

    ApplyChangedStates(&input, mutations);
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedDecommissionedFlag));

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
    // auto& bundleState = mutations.ChangedStates["default-bundle"];
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));

    // Check Setting node attributes
    {
        EXPECT_EQ(3, std::ssize(mutations.ChangeNodeAnnotations));
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

    EXPECT_EQ(3, std::ssize(mutations.ChangedEnableBundleBalancerFlag));
    for (auto& [nodeName, enableBundleBalancer] : mutations.ChangedEnableBundleBalancerFlag) {
        GetOrCrash(input.TabletNodes, nodeName)->EnableBundleBalancer = enableBundleBalancer;
        EXPECT_TRUE(enableBundleBalancer);
    }

    // Finally!
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedEnableBundleBalancerFlag));
    EXPECT_EQ(3, std::ssize(mutations.ChangedDecommissionedFlag));
    for (auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        EXPECT_FALSE(decommissioned);
        EXPECT_TRUE(nodesToRemove.count(nodeName));
    }

    EXPECT_EQ(3, std::ssize(mutations.ChangedNodeUserTags));
    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_TRUE(nodesToRemove.count(nodeName));
        EXPECT_EQ(0, std::ssize(tags));
    }

    EXPECT_EQ(1, std::ssize(mutations.ChangedStates));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->NodeDeallocations));
}

TEST(TBundleSchedulerTest, DontRemoveTabletNodeCypressNodesFromBB)
{
    auto input = GenerateSimpleInputContext(DefaultNodeCount, DefaultCellCount, 10);
    auto nodeNames = GenerateNodesForBundle(input, "default-bundle", 13);

    const auto DateInThePast = TInstant::Now() - TDuration::Days(30);

    auto nodesToRemove = GetRandomElements(nodeNames, 3);
    for (auto& nodeName : nodesToRemove) {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        nodeInfo->Annotations->Allocated = false;
        nodeInfo->Annotations->DeallocationStrategy = DeallocationStrategyReturnToBB;
        nodeInfo->Annotations->DeallocatedAt = DateInThePast;
    }

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.ProxiesToCleanup));
    EXPECT_EQ(0, std::ssize(mutations.NodesToCleanup));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // NYT::NCellBalancer
