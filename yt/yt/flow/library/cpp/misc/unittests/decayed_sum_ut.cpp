#include <yt/yt/flow/library/cpp/misc/decayed_sum.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <array>
#include <cmath>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDecayedSumTest, EmptyAndFirstObservation)
{
    TDecayedSum sum(TDuration::Minutes(5));
    auto now = TInstant::Seconds(1000);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), 0);
    EXPECT_DOUBLE_EQ(sum.GetDecayedValue(now), 0);
    sum.Add(12, now);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), 12);
    EXPECT_DOUBLE_EQ(sum.GetDecayedValue(now), 12);
}

TEST(TDecayedSumTest, AddsAtTheSameTimestamp)
{
    TDecayedSum batch(TDuration::Minutes(5));
    TDecayedSum rows(TDuration::Minutes(5));
    auto now = TInstant::Zero();
    batch.Add(7, now);
    rows.Add(3, now);
    rows.Add(4, now);
    EXPECT_DOUBLE_EQ(batch.GetLastValue(), rows.GetLastValue());
    EXPECT_DOUBLE_EQ(batch.GetDecayedValue(now + TDuration::Minutes(5)), rows.GetDecayedValue(now + TDuration::Minutes(5)));
}

TEST(TDecayedSumTest, ReadsDoNotAdvanceObservationTime)
{
    auto decayTime = TDuration::Seconds(10);
    auto now = TInstant::Seconds(1000);
    TDecayedSum read(decayTime);
    TDecayedSum unread(decayTime);
    read.Add(10, now);
    unread.Add(10, now);
    EXPECT_DOUBLE_EQ(read.GetDecayedValue(now + decayTime), 10 * std::exp(-1.0));
    EXPECT_DOUBLE_EQ(read.GetLastValue(), 10);
    EXPECT_DOUBLE_EQ(read.GetDecayedValue(now - decayTime), 10);
    read.Add(5, now + decayTime * 2);
    unread.Add(5, now + decayTime * 2);
    EXPECT_DOUBLE_EQ(read.GetLastValue(), unread.GetLastValue());
    EXPECT_DOUBLE_EQ(read.GetLastValue(), 10 * std::exp(-2.0) + 5);
}

TEST(TDecayedSumTest, OlderObservationsDecayToLatestTime)
{
    auto now = TInstant::Seconds(1000);
    auto decayTime = TDuration::Seconds(10);
    TDecayedSum sum(decayTime);
    sum.Add(10, now);
    sum.Add(3, now - decayTime);
    sum.Add(4, now - decayTime * 2);
    auto expected = 10 + 3 * std::exp(-1.0) + 4 * std::exp(-2.0);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), expected);
    EXPECT_DOUBLE_EQ(sum.GetDecayedValue(now - decayTime), expected);
    EXPECT_DOUBLE_EQ(sum.GetDecayedValue(now + decayTime), expected * std::exp(-1.0));
    sum.Add(5, now + decayTime);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), expected * std::exp(-1.0) + 5);
}

TEST(TDecayedSumTest, DeliveryOrderDoesNotChangeTheSum)
{
    auto now = TInstant::Seconds(1000);
    auto decayTime = TDuration::Seconds(10);
    std::array<double, 3> values{3, 4, 10};
    std::array<int, 3> order{0, 1, 2};
    auto expected = 3 * std::exp(-2.0) + 4 * std::exp(-1.0) + 10;
    do {
        TDecayedSum sum(decayTime);
        for (auto index : order) {
            sum.Add(values[index], now + decayTime * index);
        }
        EXPECT_NEAR(sum.GetLastValue(), expected, 1e-12);
        EXPECT_NEAR(sum.GetDecayedValue(now + decayTime * 3), expected * std::exp(-1.0), 1e-12);
        sum.Add(5, now + decayTime * 3);
        EXPECT_NEAR(sum.GetLastValue(), expected * std::exp(-1.0) + 5, 1e-12);
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(TDecayedSumTest, ZeroObservationAdvancesDecay)
{
    auto decayTime = TDuration::Seconds(10);
    auto now = TInstant::Seconds(1000);
    TDecayedSum sum(decayTime);
    sum.Add(10, now);
    sum.Add(0, now + decayTime);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), 10 * std::exp(-1.0));
    EXPECT_NEAR(sum.GetDecayedValue(now + decayTime * 2), 10 * std::exp(-2.0), 1e-12);
}

TEST(TDecayedSumTest, IrregularBatchesHaveIndependentWeights)
{
    auto now = TInstant::Seconds(1000);
    TDecayedSum sum(TDuration::Seconds(10));
    sum.Add(10, now);
    sum.Add(100, now + TDuration::Seconds(3));
    sum.Add(1, now + TDuration::Seconds(17));
    EXPECT_NEAR(sum.GetLastValue(), 10 * std::exp(-1.7) + 100 * std::exp(-1.4) + 1, 1e-12);
}

TEST(TDecayedSumTest, LongIdleDoesNotLoseTheNextObservation)
{
    auto now = TInstant::Seconds(1000);
    TDecayedSum sum(TDuration::Seconds(1));
    sum.Add(10, now);
    auto later = now + TDuration::Days(100);
    EXPECT_DOUBLE_EQ(sum.GetDecayedValue(later), 0);
    sum.Add(3, later);
    EXPECT_DOUBLE_EQ(sum.GetLastValue(), 3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
