#include <yt/yt/flow/library/cpp/throughput_autoscaler/steady_demand_history.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <type_traits>

namespace NYT::NFlow {
namespace {

static_assert(!std::is_copy_constructible_v<TSteadyDemandHistory>);

TEST(TSteadyDemandHistoryTest, ExpirationAndRestorePreserveTheRemainingPeaks)
{
    const auto start = TInstant::Days(20);
    const auto window = TDuration::Days(9);
    const auto period = TDuration::Minutes(3);
    TSteadyDemandHistory history;
    history.Observe(start, 100, period);
    history.Observe(start + TDuration::Days(2), 80, period);
    history.Observe(start + TDuration::Days(4), 90, period);
    EXPECT_EQ(history.GetPeak(start + TDuration::Days(8), window), 100);
    const auto saved = history.Save();
    TSteadyDemandHistory restored(saved);
    EXPECT_EQ(restored.GetPeak(start + TDuration::Days(9), window), 90);
    EXPECT_EQ(restored.GetPeak(start + TDuration::Days(13), window), 0);
    // Expiring the mutable history must not mutate an already prepared snapshot.
    TSteadyDemandHistory original(saved);
    EXPECT_EQ(original.GetPeak(start + TDuration::Days(8), window), 100);
}

TEST(TSteadyDemandHistoryTest, RepeatedPollsReuseSnapshotAndDoNotRefreshThePeak)
{
    const auto start = TInstant::Days(20);
    const auto period = TDuration::Minutes(3);
    const auto window = TDuration::Days(9);
    TSteadyDemandHistory history;
    history.Observe(start, 100, period);
    const auto saved = history.Save();
    for (int index = 0; index < 1000; ++index) {
        history.Observe(start, 100, period);
        EXPECT_EQ(history.GetPeak(start + TDuration::Days(8), window), 100);
        EXPECT_EQ(history.Save().AsStringBuf().data(), saved.AsStringBuf().data());
    }
    EXPECT_EQ(history.GetPeak(start + window, window), 0);
    history.Observe(start, 100, period);
    EXPECT_EQ(history.GetPeak(start + window, window), 0);
}

} // namespace
} // namespace NYT::NFlow
