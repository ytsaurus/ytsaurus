#include <yt/core/test_framework/framework.h>

#include "control_thread.h"

#include <yt/core/yson/null_consumer.h>

#include <contrib/libs/gmock/gmock/gmock.h>
#include <contrib/libs/gmock/gmock/gmock-matchers.h>
#include <contrib/libs/gmock/gmock/gmock-actions.h>


namespace NYT::NSchedulerSimulator {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NNodeTrackerClient;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

namespace {

TExecNodePtr CreateExecNode(TJobResources resourceLimits)
{
    auto node = New<TExecNode>(/* nodeId */ 1, TNodeDescriptor(ToString("node1")));
    node->Tags().insert("internal");
    node->SetResourceLimits(resourceLimits);

    NNodeTrackerClient::NProto::TDiskResources diskResources;
    auto* diskReport = diskResources.add_disk_reports();
    diskReport->set_limit(100_GB);
    diskReport->set_usage(0);
    node->SetDiskInfo(diskResources);

    return node;
}

TExecNodePtr CreateExecNode(i64 cpu, i64 memory)
{
    TJobResources resourceLimits;

    resourceLimits.SetCpu(TCpuResource(cpu));
    resourceLimits.SetMemory(memory);

    resourceLimits.SetUserSlots(10'0000);
    resourceLimits.SetNetwork(100'000);

    return CreateExecNode(resourceLimits);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TControlThreadTest
    : public testing::Test
{
protected:

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControlThreadTest, TestAssertTrue)
{
    EXPECT_TRUE(true);
}

TEST_F(TControlThreadTest, TestSimpleAllocation)
{


}

} // namespace
} // namespace NYT::NSchedulerSimulator
