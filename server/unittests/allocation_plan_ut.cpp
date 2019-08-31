#include <yt/core/test_framework/framework.h>

#include "mock_objects.h"

#include <yp/server/scheduler/allocation_plan.h>

namespace NYP::NServer::NScheduler::NTests {
namespace {

using namespace NCluster::NTests;

////////////////////////////////////////////////////////////////////////////////

TEST(TAllocationPlanTest, GetNodeCount)
{
    TAllocationPlan plan;

    auto pod1 = CreateMockPod();
    auto pod2 = CreateMockPod();
    auto pod3 = CreateMockPod();

    auto node1 = CreateMockNode();
    auto node2 = CreateMockNode();

    plan.AssignPodToNode(pod1.get(), node1.get());
    EXPECT_EQ(1, plan.GetNodeCount());

    plan.AssignPodToNode(pod2.get(), node1.get());
    EXPECT_EQ(1, plan.GetNodeCount());

    plan.AssignPodToNode(pod3.get(), node2.get());
    EXPECT_EQ(2, plan.GetNodeCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NScheduler::NTests
