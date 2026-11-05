#include <yt/yt/flow/library/cpp/buffers/offered_rate_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MB = 1'000'000;
constexpr i64 WindowSeconds = 60;

//! Steady producer: |ratePerSecond| bytes accepted each second of alignment time.
TOfferedRateEstimatorPtr MakeWarmEstimator(i64 ratePerSecond, i64 fromTs, i64 toTs)
{
    auto estimator = New<TOfferedRateEstimator>(TDuration::Seconds(WindowSeconds));
    for (i64 ts = fromTs; ts <= toTs; ++ts) {
        estimator->RecordAccepted(ratePerSecond, ts);
    }
    return estimator;
}

TEST(TOfferedRateEstimatorTest, ColdBacklogFallsBackToSpanEstimate)
{
    auto estimator = New<TOfferedRateEstimator>();
    EXPECT_DOUBLE_EQ(estimator->EstimateRate({{{990, 10 * MB}, {995, 10 * MB}, {998, 10 * MB}}}), 30.0 * MB / 8);
    // The degenerate single-timestamp dump keeps the noisy cold-start peak.
    EXPECT_DOUBLE_EQ(estimator->EstimateRate({{{1000, 500 * MB}}}), 500.0 * MB);
}

TEST(TOfferedRateEstimatorTest, EmptyBacklogHasNoEstimate)
{
    auto estimator = MakeWarmEstimator(10 * MB, 0, 300);
    EXPECT_DOUBLE_EQ(estimator->EstimateRate({}), 0.0);
}

TEST(TOfferedRateEstimatorTest, SteadyStreamMatchesRate)
{
    auto estimator = MakeWarmEstimator(10 * MB, 0, 600);
    // A fresh one-second backlog of the same rate: the honest instant estimate wins.
    EXPECT_NEAR(estimator->EstimateRate({{{601, 10 * MB}}}), 10.0 * MB, 0.01 * MB);
}

TEST(TOfferedRateEstimatorTest, WarmSingleTimestampDumpIsCappedByHistory)
{
    auto estimator = MakeWarmEstimator(10 * MB, 0, 300);
    const double rate = estimator->EstimateRate({{{300, 500 * MB}}});
    // The instant estimate claims 500 MB/s; the cap allows at most
    // MaxHistoryGain × (history + backlog spread over the window).
    const double cap = TOfferedRateEstimator::MaxHistoryGain * (10 * MB + std::exp(1.0) * 500.0 * MB / WindowSeconds);
    EXPECT_GE(rate, 10.0 * MB);
    EXPECT_LE(rate, cap);
    EXPECT_LE(rate, 100.0 * MB);
}

TEST(TOfferedRateEstimatorTest, WarmRampPassesInstantlyUpToGainFactor)
{
    auto estimator = MakeWarmEstimator(10 * MB, 0, 300);
    // The producer ramps 4×: a fresh backlog announcing exactly that passes uncapped.
    EXPECT_NEAR(estimator->EstimateRate({{{301, 40 * MB}}}), 40.0 * MB, 4.0 * MB);
}

TEST(TOfferedRateEstimatorTest, FutureOutlierDentsHistoryByAtMostE)
{
    auto estimator = MakeWarmEstimator(10 * MB, 0, 300);
    const double before = estimator->EstimateRate({{{300, 500 * MB}}});
    // A lone far-future timestamp advances the clock by at most one window.
    estimator->RecordAccepted(1, 300 + 100 * WindowSeconds);
    const double after = estimator->EstimateRate({{{300, 500 * MB}}});
    EXPECT_LE(after, before);
    EXPECT_GE(after, before / std::exp(1.0) * 0.9);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
