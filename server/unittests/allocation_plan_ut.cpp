#include <yt/core/test_framework/framework.h>

#include <yp/server/scheduler/allocation_plan.h>
#include <yp/server/scheduler/node.h>
#include <yp/server/scheduler/pod.h>

#include <yp/server/objects/proto/objects.pb.h>

#include <memory>

namespace NYP {
namespace NServer {
namespace NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUniqueId()
{
    static int lastObjectIndex = 0;
    return "mock_object_" + ToString(lastObjectIndex++);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPod> CreateMockPod()
{
    return std::make_unique<TPod>(
        GenerateUniqueId(),
        /* podSet */ nullptr,
        /* labels */ NYT::NYson::TYsonString(),
        /* node */ nullptr,
        NObjects::NProto::TPodSpecOther(),
        NObjects::NProto::TPodStatusOther());
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TNode> CreateMockNode()
{
    return std::make_unique<TNode>(
        GenerateUniqueId(),
        /* labels */ NYT::NYson::TYsonString(),
        std::vector<TTopologyZone*>(),
        NObjects::EHfsmState::Unknown,
        NObjects::ENodeMaintenanceState::None,
        NClient::NApi::NProto::TNodeSpec());
}

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
} // namespace NScheduler
} // namespace NServer
} // namespace NYP
