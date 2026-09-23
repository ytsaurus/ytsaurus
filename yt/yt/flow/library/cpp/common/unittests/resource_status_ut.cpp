#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/resource_status.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TResourceStatusTest, QueueSizeFollowsDrainWithoutFurtherUpdates)
{
    // A job keeps 65 requests of the resource in flight for 20 minutes.
    auto t0 = TInstant::Seconds(1'000'000);
    TResourceStatus status;
    status.Update(65, 0, t0);
    for (int second = 1; second <= 1200; ++second) {
        status.Update(40, 40, t0 + TDuration::Seconds(second));
    }

    auto steady = status.Collect(t0 + TDuration::Seconds(1200));
    ASSERT_TRUE(steady->QueueSize30s);
    ASSERT_TRUE(steady->QueueSize10m);
    ASSERT_TRUE(steady->QueueGrowthRate10m);
    EXPECT_NEAR(*steady->QueueSize30s, 65., 1e-6);
    EXPECT_NEAR(*steady->QueueSize10m, 65., 1e-6);
    EXPECT_NEAR(*steady->QueueGrowthRate10m, 0., 1e-6);

    // The job is stopped: its last requests complete within a second and nothing feeds the
    // resource afterwards.
    auto drainTime = t0 + TDuration::Seconds(1201);
    status.Update(0, 65, drainTime);

    // An hour later the queue is still empty and the report must say so.
    auto report = status.Collect(drainTime + TDuration::Hours(1));
    ASSERT_TRUE(report->QueueSize30s);
    ASSERT_TRUE(report->QueueSize10m);
    ASSERT_TRUE(report->QueueGrowthRate30s);
    ASSERT_TRUE(report->QueueGrowthRate10m);
    ASSERT_TRUE(report->QueuePushRate10m);
    ASSERT_TRUE(report->QueueFetchRate10m);
    EXPECT_NEAR(*report->QueueSize30s, 0., 1e-3);
    EXPECT_NEAR(*report->QueueSize10m, 0., 1e-3);
    EXPECT_NEAR(*report->QueueGrowthRate30s, 0., 1e-3);
    EXPECT_NEAR(*report->QueueGrowthRate10m, 0., 1e-3);
    EXPECT_NEAR(*report->QueuePushRate10m, 0., 1e-3);
    EXPECT_NEAR(*report->QueueFetchRate10m, 0., 1e-3);
}

TEST(TResourceStatusTest, QueueSizeFollowsSameInstantDrain)
{
    // 65 requests in flight for 20 minutes, then the consumer dies and every one of them
    // completes within the same instant, one FeedStatus each.
    auto t0 = TInstant::Seconds(1'000'000);
    TResourceStatus status;
    status.Update(65, 0, t0);
    for (int second = 1; second <= 1200; ++second) {
        status.Update(40, 40, t0 + TDuration::Seconds(second));
    }
    auto drainTime = t0 + TDuration::Seconds(1201);
    for (int i = 0; i < 65; ++i) {
        status.Update(0, 1, drainTime);
    }

    auto report = status.Collect(drainTime + TDuration::Hours(1));
    ASSERT_TRUE(report->QueueSize30s);
    ASSERT_TRUE(report->QueueSize10m);
    EXPECT_NEAR(*report->QueueSize30s, 0., 1e-3);
    EXPECT_NEAR(*report->QueueSize10m, 0., 1e-3);
}

TEST(TResourceStatusTest, CollectBetweenUpdatesKeepsLiveAverage)
{
    // A live queue of 65 updated every second and collected between the updates, as the worker
    // heartbeat does: the extra samples must not move the averages.
    auto t0 = TInstant::Seconds(1'000'000);
    TResourceStatus status;
    status.Update(65, 0, t0);
    TWorkerResourceStatusPtr report;
    for (int second = 1; second <= 1200; ++second) {
        status.Update(40, 40, t0 + TDuration::Seconds(second));
        report = status.Collect(t0 + TDuration::Seconds(second) + TDuration::MilliSeconds(500));
    }

    ASSERT_TRUE(report->QueueSize30s);
    ASSERT_TRUE(report->QueueSize10m);
    ASSERT_TRUE(report->QueueGrowthRate30s);
    ASSERT_TRUE(report->QueueGrowthRate10m);
    EXPECT_NEAR(*report->QueueSize30s, 65., 1e-6);
    EXPECT_NEAR(*report->QueueSize10m, 65., 1e-6);
    EXPECT_NEAR(*report->QueueGrowthRate30s, 0., 1e-6);
    EXPECT_NEAR(*report->QueueGrowthRate10m, 0., 1e-6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
