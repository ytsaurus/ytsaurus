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

TEST(TPerformanceCountersSteadyTest, MarksSteadyAfterTheFirstIterationWithInput)
{
    // The initialization iteration is skipped, the second one is measured.
    EXPECT_FALSE(ShouldMarkPerformanceCountersSteady(/*nonEmptyIterations*/ 0, /*inputMessages*/ 100, TDuration::Seconds(5)));
    EXPECT_TRUE(ShouldMarkPerformanceCountersSteady(/*nonEmptyIterations*/ 1, /*inputMessages*/ 100, TDuration::Seconds(5)));
}

TEST(TPerformanceCountersSteadyTest, MarksAnIdleJobSteadyAfterAMinute)
{
    EXPECT_FALSE(ShouldMarkPerformanceCountersSteady(/*nonEmptyIterations*/ 0, /*inputMessages*/ 0, IdleMetricsSteadyDelay - TDuration::Seconds(1)));
    EXPECT_TRUE(ShouldMarkPerformanceCountersSteady(/*nonEmptyIterations*/ 0, /*inputMessages*/ 0, IdleMetricsSteadyDelay));
}

TEST(TPerformanceCountersSteadyTest, WaitsForALongFirstIterationWithInput)
{
    // A first iteration still consuming its input (an index build) is initialization, not idleness.
    EXPECT_FALSE(ShouldMarkPerformanceCountersSteady(/*nonEmptyIterations*/ 0, /*inputMessages*/ 100, TDuration::Minutes(10)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
