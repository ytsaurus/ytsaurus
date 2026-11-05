#include <yt/yt/flow/library/cpp/buffers/max_rate_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMaxRateEstimator, EmptyReturnsNothing)
{
    TMaxRateEstimator estimator;
    EXPECT_FALSE(estimator.GetMaxRate().has_value());
    estimator.Update(0, TInstant::Seconds(1), TDuration::Seconds(1));
    EXPECT_FALSE(estimator.GetMaxRate().has_value());
}

TEST(TMaxRateEstimator, SteadyRate)
{
    TMaxRateEstimator estimator;
    for (int i = 0; i <= 20; ++i) {
        estimator.Update(i * 100.0, TInstant::Seconds(i), TDuration::Seconds(1));
    }
    EXPECT_NEAR(*estimator.GetMaxRate(), 100.0, 1e-9);
}

TEST(TMaxRateEstimator, HoldsPeakThroughIdleGap)
{
    TMaxRateEstimator estimator;
    // Burst: 1000 bytes/sec for 3 seconds.
    for (int i = 0; i <= 3; ++i) {
        estimator.Update(i * 1000.0, TInstant::Seconds(i), TDuration::Seconds(1));
    }
    EXPECT_NEAR(*estimator.GetMaxRate(), 1000.0, 1e-9);
    // Idle for a few buckets: the peak must survive within the window.
    for (int i = 4; i <= 8; ++i) {
        estimator.Update(3000.0, TInstant::Seconds(i), TDuration::Seconds(1));
    }
    EXPECT_NEAR(*estimator.GetMaxRate(), 1000.0, 1e-9);
    // After the whole window of idleness the peak falls out.
    for (int i = 9; i <= 12; ++i) {
        estimator.Update(3000.0, TInstant::Seconds(i), TDuration::Seconds(1));
    }
    EXPECT_NEAR(*estimator.GetMaxRate(), 0.0, 1e-9);
}

TEST(TMaxRateEstimator, BucketDurationScalesWithCycle)
{
    TMaxRateEstimator estimator;
    // Step-drain consumer: 12 MB drained instantly every 120 seconds; bucket
    // duration matches the cycle, so the bucket rate equals the mean rate.
    double cumulative = 0;
    for (int cycle = 0; cycle < 3; ++cycle) {
        estimator.Update(cumulative, TInstant::Seconds(cycle * 120), TDuration::Seconds(120));
        cumulative += 12e6;
    }
    EXPECT_NEAR(*estimator.GetMaxRate(), 1e5, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
