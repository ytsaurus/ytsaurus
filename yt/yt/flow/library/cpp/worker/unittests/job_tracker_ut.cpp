#include <yt/yt/flow/library/cpp/worker/job_tracker.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NWorker {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobThreadPoolSizeTest, ComputesFromVcpu)
{
    auto dynamicSpec = New<TDynamicPipelineSpec>();

    auto nodeInfo = New<TNodeInfo>();
    nodeInfo->VcpuLimit = 30000;
    nodeInfo->VcpuFactor = 1.5;

    // 30000 / 1000.0 / 1.5 == 20.
    EXPECT_EQ(GetJobThreadPoolSize(dynamicSpec, nodeInfo), 20);
}

TEST(TJobThreadPoolSizeTest, ExplicitJobThreadsTakesPrecedence)
{
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobTracker->JobThreads = 7;

    auto nodeInfo = New<TNodeInfo>();
    nodeInfo->VcpuLimit = 30000;
    nodeInfo->VcpuFactor = 1.5;

    EXPECT_EQ(GetJobThreadPoolSize(dynamicSpec, nodeInfo), 7);
}

TEST(TJobThreadPoolSizeTest, FallsBackToDefaultWithoutVcpuInfo)
{
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    auto nodeInfo = New<TNodeInfo>();

    EXPECT_EQ(GetJobThreadPoolSize(dynamicSpec, nodeInfo), TDynamicJobTrackerSpec::DefaultJobThreads);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
