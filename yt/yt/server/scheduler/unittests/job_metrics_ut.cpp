#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/core/misc/statistic_path.h>

namespace NYT::NScheduler {

using namespace NStatisticPath;

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TJobMetricsTest, TestIsEmpty)
{
    TJobMetrics metrics;
    EXPECT_TRUE(metrics.IsEmpty());

    metrics.Values()[EJobMetricName::TotalTime] = 117;
    EXPECT_TRUE(!metrics.IsEmpty());

    TCustomJobMetricDescription customMetricDescription{
        "/custom_statistic"_SP,
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
