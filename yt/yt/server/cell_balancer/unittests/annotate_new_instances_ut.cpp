#include "helpers.h"

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/cypress_bindings.h>
#include <yt/yt/server/cell_balancer/input_state.h>
#include <yt/yt/server/cell_balancer/mutations.h>
#include <yt/yt/server/cell_balancer/orchid_bindings.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellBalancer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TAllocationYPClusterTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>>
{ };

TEST_P(TAllocationYPClusterTest, AllocationAndDeallocation)
{
    const auto [isProxy, hasActualCluster] = GetParam();
    const std::string bundleName = "bigd";
    const std::string dataCenterName = "default";
    const std::string expectedCluster = hasActualCluster ? "sam" : "vla";
    auto input = GenerateSimpleInputContext(isProxy ? 0 : 1, 0, isProxy ? 1 : 0);
    input.Config->DecommissionReleasedNodes = false;

    std::string instanceName;
    TInstanceInfoBasePtr instance;
    if (isProxy) {
        instanceName = *GenerateProxiesForBundle(input, bundleName, 1).begin();
        instance = input.RpcProxies.at(instanceName);
        GenerateProxyAllocationsForBundle(input, bundleName, 1);
    } else {
        instanceName = *GenerateNodesForBundle(input, bundleName, 1).begin();
        instance = input.TabletNodes.at(instanceName);
        GenerateNodeAllocationsForBundle(input, bundleName, 1);
    }
    instance->BundleControllerAnnotations = New<TBundleControllerInstanceAnnotations>();

    const auto allocationId = input.AllocationRequests.begin()->first;
    auto request = input.AllocationRequests.at(allocationId);
    request->Spec->YPCluster = "vla";
    request->Status->YPCluster = hasActualCluster ? "sam" : "";
    request->Status->PodId = GetPodIdForInstance(instanceName);
    request->Status->State = "COMPLETED";
    // BC deserializes the request updated by Hulk from Cypress.
    input.AllocationRequests[allocationId] = CloneYsonStruct(request);

    TSchedulerMutations allocationMutations;
    ScheduleBundles(input, &allocationMutations);
    CheckEmptyAlerts(allocationMutations);
    const auto& changedAnnotations = isProxy
        ? allocationMutations.ChangedProxyAnnotations
        : allocationMutations.ChangedNodeAnnotations;
    ASSERT_TRUE(changedAnnotations.contains(instanceName));
    const auto& annotations = changedAnnotations.at(instanceName).Mutation;
    EXPECT_EQ(expectedCluster, annotations->YPCluster);
    EXPECT_EQ(dataCenterName, annotations->DataCenter);
    EXPECT_EQ(request->Spec->NannyService, annotations->NannyService);
    EXPECT_EQ("vla", request->Spec->YPCluster);

    auto orchid = NOrchid::GetBundlesInfo(input, allocationMutations).at(bundleName);
    const auto& allocatingInstances = isProxy ? orchid->AllocatingRpcProxies : orchid->AllocatingTabletNodes;
    ASSERT_TRUE(allocatingInstances.contains(allocationId));
    ASSERT_TRUE(allocatingInstances.at(allocationId)->InstanceInfo);
    EXPECT_EQ(expectedCluster, allocatingInstances.at(allocationId)->InstanceInfo->YPCluster);

    instance->BundleControllerAnnotations = annotations;
    ApplyChangedStates(&input, allocationMutations);
    auto& state = input.BundleStates.at(bundleName);
    state->NodeAllocations.clear();
    state->ProxyAllocations.clear();
    input.AllocationRequests.clear();

    auto deallocation = New<TDeallocationRequestState>();
    deallocation->CreationTime = TInstant::Now();
    deallocation->InstanceName = instanceName;
    deallocation->DataCenter = dataCenterName;
    deallocation->Strategy = DeallocationStrategyHulkRequest;
    auto& deallocations = isProxy ? state->ProxyDeallocations : state->NodeDeallocations;
    deallocations["deallocation"] = deallocation;

    TSchedulerMutations deallocationMutations;
    ScheduleBundles(input, &deallocationMutations);
    CheckEmptyAlerts(deallocationMutations);
    ASSERT_EQ(1u, deallocationMutations.NewDeallocations.size());
    const auto& spec = deallocationMutations.NewDeallocations.at("deallocation")->Spec;
    EXPECT_EQ(expectedCluster, spec->YPCluster);
    EXPECT_EQ(request->Status->PodId, spec->PodId);
}

INSTANTIATE_TEST_SUITE_P(
    InstanceTypesAndLegacyRequests,
    TAllocationYPClusterTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

class TAnnotateNewInstancesTest
    : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        NLogging::TLogManager::Get()->ConfigureFromEnv();
    }

    // Creates a minimal input state with one zone, one tablet node size, one rpc proxy size,
    // and the "bigd" bundle with EnableInstanceAllocation=false.
    TSchedulerInputState MakeInput()
    {
        auto input = GenerateSimpleInputContext(/*nodeCount*/ 0);

        // No instance allocator service — instances are provisioned externally.
        input.Config->HasInstanceAllocatorService = false;

        // Add an rpc proxy size to the zone.
        auto& zoneInfo = input.Zones.begin()->second;
        auto defaultProxySize = New<NBundleControllerClient::TInstanceSize>();
        defaultProxySize->ResourceGuarantee->Vcpu = 1111;
        defaultProxySize->ResourceGuarantee->Memory = 18_GB;
        zoneInfo->RpcProxySizes["default"] = defaultProxySize;

        // Disable instance allocation for the bundle.
        input.Bundles["bigd"]->EnableInstanceAllocation = false;

        return input;
    }

    // Adds an online tablet node without bundle_controller_annotations.
    std::string AddUnannotatedOnlineNode(TSchedulerInputState& input)
    {
        auto nodeName = Format("unannotated-node-%v.default.yandex.net", input.TabletNodes.size());
        auto nodeInfo = New<TTabletNodeInfo>();
        nodeInfo->State = InstanceStateOnline;
        nodeInfo->Banned = false;
        nodeInfo->Decommissioned = false;
        // BundleControllerAnnotations is DefaultNew() — Allocated=false, AllocatedForBundle=""
        input.TabletNodes[nodeName] = nodeInfo;
        return nodeName;
    }

    // Adds an online rpc proxy without bundle_controller_annotations.
    std::string AddUnannotatedOnlineProxy(TSchedulerInputState& input)
    {
        auto proxyName = Format("unannotated-proxy-%v.default.yandex.net", input.RpcProxies.size());
        auto proxyInfo = New<TRpcProxyInfo>();
        proxyInfo->Alive = New<TRpcProxyAlive>();
        proxyInfo->Banned = false;
        // BundleControllerAnnotations is DefaultNew() — Allocated=false, AllocatedForBundle=""
        input.RpcProxies[proxyName] = proxyInfo;
        return proxyName;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesHappyPath)
{
    auto input = MakeInput();
    input.Config->AnnotateNewNodes = true;

    auto nodeName = AddUnannotatedOnlineNode(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    ASSERT_TRUE(mutations.ChangedNodeAnnotations.contains(nodeName));
    const auto& annotation = mutations.ChangedNodeAnnotations[nodeName].Mutation;
    EXPECT_TRUE(annotation->Allocated);
    EXPECT_EQ(annotation->AllocatedForBundle, "spare");
    ASSERT_TRUE(static_cast<bool>(annotation->Resource));
    EXPECT_EQ(annotation->Resource->Vcpu, 9999);
    EXPECT_EQ(annotation->Resource->Memory, static_cast<i64>(88_GB));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesSkipsAlreadyAnnotated)
{
    auto input = MakeInput();
    input.Config->AnnotateNewNodes = true;

    // Add a node that already has annotations.
    auto nodeName = std::string("annotated-node.default.yandex.net");
    auto nodeInfo = New<TTabletNodeInfo>();
    nodeInfo->State = InstanceStateOnline;
    nodeInfo->BundleControllerAnnotations->Allocated = true;
    nodeInfo->BundleControllerAnnotations->AllocatedForBundle = "spare";
    input.TabletNodes[nodeName] = nodeInfo;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_FALSE(mutations.ChangedNodeAnnotations.contains(nodeName));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesSkipsOfflineNodes)
{
    auto input = MakeInput();
    input.Config->AnnotateNewNodes = true;

    auto nodeName = std::string("offline-node.default.yandex.net");
    auto nodeInfo = New<TTabletNodeInfo>();
    nodeInfo->State = InstanceStateOffline;
    input.TabletNodes[nodeName] = nodeInfo;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_FALSE(mutations.ChangedNodeAnnotations.contains(nodeName));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesFiresAlertForMultipleZones)
{
    auto input = MakeInput();
    input.Config->AnnotateNewNodes = true;

    // Add a second zone.
    auto secondZone = New<TZoneInfo>();
    secondZone->DefaultYPCluster = "yp-second";
    secondZone->RequiresMinusOneRackGuarantee = false;
    auto secondTabNode = New<NBundleControllerClient::TInstanceSize>();
    secondTabNode->ResourceGuarantee->Vcpu = 1000;
    secondTabNode->ResourceGuarantee->Memory = 10_GB;
    secondZone->TabletNodeSizes["default"] = secondTabNode;
    input.Zones["second-zone"] = secondZone;

    AddUnannotatedOnlineNode(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    bool foundAlert = false;
    for (const auto& alert : mutations.AlertsToFire) {
        if (alert.Id == "annotate_new_nodes_multiple_zones") {
            foundAlert = true;
            break;
        }
    }
    EXPECT_TRUE(foundAlert) << "Expected alert 'annotate_new_nodes_multiple_zones' to be fired";
    EXPECT_TRUE(mutations.ChangedNodeAnnotations.empty());
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesFiresAlertForMultipleSizes)
{
    auto input = MakeInput();
    input.Config->AnnotateNewNodes = true;

    // Add a second tablet node size to the zone.
    auto& zoneInfo = input.Zones.begin()->second;
    auto secondSize = New<NBundleControllerClient::TInstanceSize>();
    secondSize->ResourceGuarantee->Vcpu = 5000;
    secondSize->ResourceGuarantee->Memory = 50_GB;
    zoneInfo->TabletNodeSizes["large"] = secondSize;

    AddUnannotatedOnlineNode(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    bool foundAlert = false;
    for (const auto& alert : mutations.AlertsToFire) {
        if (alert.Id == "annotate_new_nodes_multiple_sizes") {
            foundAlert = true;
            break;
        }
    }
    EXPECT_TRUE(foundAlert) << "Expected alert 'annotate_new_nodes_multiple_sizes' to be fired";
    EXPECT_TRUE(mutations.ChangedNodeAnnotations.empty());
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewNodesDisabledByDefault)
{
    auto input = MakeInput();
    // AnnotateNewNodes defaults to false.

    AddUnannotatedOnlineNode(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_TRUE(mutations.ChangedNodeAnnotations.empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesHappyPath)
{
    auto input = MakeInput();
    input.Config->AnnotateNewProxies = true;

    auto proxyName = AddUnannotatedOnlineProxy(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    ASSERT_TRUE(mutations.ChangedProxyAnnotations.contains(proxyName));
    const auto& annotation = mutations.ChangedProxyAnnotations[proxyName].Mutation;
    EXPECT_TRUE(annotation->Allocated);
    EXPECT_EQ(annotation->AllocatedForBundle, "spare");
    ASSERT_TRUE(static_cast<bool>(annotation->Resource));
    EXPECT_EQ(annotation->Resource->Vcpu, 1111);
    EXPECT_EQ(annotation->Resource->Memory, static_cast<i64>(18_GB));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesSkipsAlreadyAnnotated)
{
    auto input = MakeInput();
    input.Config->AnnotateNewProxies = true;

    auto proxyName = std::string("annotated-proxy.default.yandex.net");
    auto proxyInfo = New<TRpcProxyInfo>();
    proxyInfo->Alive = New<TRpcProxyAlive>();
    proxyInfo->BundleControllerAnnotations->Allocated = true;
    proxyInfo->BundleControllerAnnotations->AllocatedForBundle = "spare";
    input.RpcProxies[proxyName] = proxyInfo;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_FALSE(mutations.ChangedProxyAnnotations.contains(proxyName));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesSkipsOfflineProxies)
{
    auto input = MakeInput();
    input.Config->AnnotateNewProxies = true;

    auto proxyName = std::string("offline-proxy.default.yandex.net");
    auto proxyInfo = New<TRpcProxyInfo>();
    // Alive is null => IsOnline() returns false.
    input.RpcProxies[proxyName] = proxyInfo;

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_FALSE(mutations.ChangedProxyAnnotations.contains(proxyName));
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesFiresAlertForMultipleZones)
{
    auto input = MakeInput();
    input.Config->AnnotateNewProxies = true;

    // Add a second zone.
    auto secondZone = New<TZoneInfo>();
    secondZone->DefaultYPCluster = "yp-second";
    secondZone->RequiresMinusOneRackGuarantee = false;
    auto secondProxySize = New<NBundleControllerClient::TInstanceSize>();
    secondProxySize->ResourceGuarantee->Vcpu = 500;
    secondProxySize->ResourceGuarantee->Memory = 5_GB;
    secondZone->RpcProxySizes["default"] = secondProxySize;
    input.Zones["second-zone"] = secondZone;

    AddUnannotatedOnlineProxy(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    bool foundAlert = false;
    for (const auto& alert : mutations.AlertsToFire) {
        if (alert.Id == "annotate_new_proxies_multiple_zones") {
            foundAlert = true;
            break;
        }
    }
    EXPECT_TRUE(foundAlert) << "Expected alert 'annotate_new_proxies_multiple_zones' to be fired";
    EXPECT_TRUE(mutations.ChangedProxyAnnotations.empty());
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesFiresAlertForMultipleSizes)
{
    auto input = MakeInput();
    input.Config->AnnotateNewProxies = true;

    // Add a second rpc proxy size to the zone.
    auto& zoneInfo = input.Zones.begin()->second;
    auto secondSize = New<NBundleControllerClient::TInstanceSize>();
    secondSize->ResourceGuarantee->Vcpu = 2222;
    secondSize->ResourceGuarantee->Memory = 36_GB;
    zoneInfo->RpcProxySizes["large"] = secondSize;

    AddUnannotatedOnlineProxy(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    bool foundAlert = false;
    for (const auto& alert : mutations.AlertsToFire) {
        if (alert.Id == "annotate_new_proxies_multiple_sizes") {
            foundAlert = true;
            break;
        }
    }
    EXPECT_TRUE(foundAlert) << "Expected alert 'annotate_new_proxies_multiple_sizes' to be fired";
    EXPECT_TRUE(mutations.ChangedProxyAnnotations.empty());
}

TEST_F(TAnnotateNewInstancesTest, AnnotateNewProxiesDisabledByDefault)
{
    auto input = MakeInput();
    // AnnotateNewProxies defaults to false.

    AddUnannotatedOnlineProxy(input);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_TRUE(mutations.ChangedProxyAnnotations.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellBalancer
