#include <yt/core/test_framework/framework.h>

#include <yt/server/lib/scheduler/job_metrics.h>

namespace NYT::NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobMetricsTest, TestIsEmpty)
{
    TJobMetrics metrics;
    EXPECT_TRUE(metrics.IsEmpty());

    metrics.Values()[EJobMetricName::TotalTime] = 117;
    EXPECT_TRUE(!metrics.IsEmpty());

    TCustomJobMetricDescription customMetricDescription{
        "/custom_statistic",
        "/custom_metric",
    };
    metrics.CustomValues()[customMetricDescription] = 117;
    EXPECT_TRUE(!metrics.IsEmpty());

    metrics.Values()[EJobMetricName::TotalTime] = 0;
    EXPECT_TRUE(!metrics.IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler
