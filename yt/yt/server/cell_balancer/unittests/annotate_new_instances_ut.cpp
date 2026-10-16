#include "helpers.h"

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/cypress_bindings.h>
#include <yt/yt/server/cell_balancer/input_state.h>
#include <yt/yt/server/cell_balancer/mutations.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NCellBalancer {
namespace {

using namespace NYTree;

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
