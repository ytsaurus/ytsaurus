#include <yt/core/test_framework/framework.h>

#include "mock_objects.h"

#include <yp/server/scheduler/config.h>
#include <yp/server/scheduler/pod_node_score.h>

#include <yp/server/lib/cluster/resource_capacities.h>

#include <yt/core/ytree/fluent.h>

namespace NYP::NServer::NScheduler::NTests {
namespace {

using namespace NCluster;
using namespace NCluster::NTests;

////////////////////////////////////////////////////////////////////////////////

TEST(TPodNodeScoreTest, NodeRandomHash)
{
    auto configWithCustomSeed = New<TPodNodeScoreConfig>();
    configWithCustomSeed->Type = EPodNodeScoreType::NodeRandomHash;
    configWithCustomSeed->Parameters = NYT::NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("seed")
            .Value(42)
        .EndMap()
    ->AsMap();

    auto configWithDefaultSeed = New<TPodNodeScoreConfig>();
    configWithDefaultSeed->Type = EPodNodeScoreType::NodeRandomHash;

    for (const auto& config : {configWithCustomSeed, configWithDefaultSeed}) {
        auto score = CreatePodNodeScore(config);

        auto node1 = CreateMockNode();
        auto node2 = CreateMockNode();
        auto pod1 = CreateMockPod();
        auto pod2 = CreateMockPod();

        auto value = score->Compute(node1.get(), pod1.get());
        EXPECT_EQ(value, score->Compute(node1.get(), pod1.get()));
        EXPECT_EQ(value, score->Compute(node1.get(), pod2.get()));
        EXPECT_NE(value, score->Compute(node2.get(), pod1.get()));
    }
}

TEST(TPodNodeScoreTest, FreeCpuMemoryShareVariance)
{
    auto config = New<TPodNodeScoreConfig>();
    config->Type = EPodNodeScoreType::FreeCpuMemoryShareVariance;

    auto score = CreatePodNodeScore(config);

    constexpr ui64 nodeCpuCapacity = 1000;
    constexpr ui64 nodeMemoryCapacity = 1024 * 1024;

    // Default check.
    auto node1 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(nodeCpuCapacity),
            /* allocated */ MakeCpuCapacities(0)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(0)));

    auto pod1 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity / 2);
    auto pod2 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity);
    auto pod3 = CreateMockPod(2 * nodeCpuCapacity + 1, 0);

    EXPECT_EQ(0.0, score->Compute(node1.get(), pod1.get()));
    EXPECT_EQ(0.0625, score->Compute(node1.get(), pod2.get()));
    EXPECT_THROW(score->Compute(node1.get(), pod3.get()), TErrorException);

    // Check for partially allocated node.
    auto node2 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(2 * nodeCpuCapacity),
            /* allocated */ MakeCpuCapacities(nodeCpuCapacity)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(2 * nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(nodeMemoryCapacity)));

    EXPECT_EQ(0.0, score->Compute(node2.get(), pod1.get()));
    EXPECT_EQ(0.015625, score->Compute(node2.get(), pod2.get()));
    EXPECT_THROW(score->Compute(node2.get(), pod3.get()), TErrorException);
}

TEST(TPodNodeScoreTest, FreeCpuMemoryShareSquaredMinDelta)
{
    auto config = New<TPodNodeScoreConfig>();
    config->Type = EPodNodeScoreType::FreeCpuMemoryShareSquaredMinDelta;

    auto score = CreatePodNodeScore(config);

    constexpr ui64 nodeCpuCapacity = 1000;
    constexpr ui64 nodeMemoryCapacity = 1024 * 1024;

    // Default check.
    auto node1 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(nodeCpuCapacity),
            /* allocated */ MakeCpuCapacities(0)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(0)));

    auto pod11 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity / 2);
    auto pod12 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity);
    auto pod13 = CreateMockPod(0, 2 * nodeMemoryCapacity + 1);

    EXPECT_EQ(0.75, score->Compute(node1.get(), pod11.get()));
    EXPECT_EQ(1, score->Compute(node1.get(), pod12.get()));
    EXPECT_THROW(score->Compute(node1.get(), pod13.get()), TErrorException);

    // Check for node with zero total cpu.
    auto node2 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(0),
            /* allocated */ MakeCpuCapacities(0)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(0)));

    auto pod21 = CreateMockPod(0, nodeMemoryCapacity / 4);

    EXPECT_EQ(0.4375, score->Compute(node2.get(), pod21.get()));
}

TEST(TPodNodeScoreTest, FreeCpuMemorySharePowSum)
{
    auto config = New<TPodNodeScoreConfig>();
    config->Type = EPodNodeScoreType::FreeCpuMemorySharePowSum;

    auto score = CreatePodNodeScore(config);

    constexpr ui64 nodeCpuCapacity = 1000;
    constexpr ui64 nodeMemoryCapacity = 1024 * 1024;

    // Default check.
    auto node1 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(nodeCpuCapacity),
            /* allocated */ MakeCpuCapacities(0)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(0)));

    auto pod1 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity / 2);
    auto pod2 = CreateMockPod(nodeCpuCapacity / 2, nodeMemoryCapacity);
    auto pod3 = CreateMockPod(2 * nodeCpuCapacity + 1, 0);

    EXPECT_NEAR(6.3245, score->Compute(node1.get(), pod1.get()), 0.001);
    EXPECT_NEAR(4.1622, score->Compute(node1.get(), pod2.get()), 0.001);
    EXPECT_THROW(score->Compute(node1.get(), pod3.get()), TErrorException);

    // Check for partially allocated node.
    auto node2 = CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(2 * nodeCpuCapacity),
            /* allocated */ MakeCpuCapacities(nodeCpuCapacity)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(2 * nodeMemoryCapacity),
            /* allocated */ MakeMemoryCapacities(nodeMemoryCapacity)));
    auto pod4 = CreateMockPod(nodeCpuCapacity, nodeMemoryCapacity);

    EXPECT_NEAR(2.0, score->Compute(node2.get(), pod4.get()), 0.001);
    EXPECT_THROW(score->Compute(node2.get(), pod3.get()), TErrorException);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NScheduler::NTests
