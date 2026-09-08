#include <yt/yt/flow/library/cpp/misc/counter.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSimpleEmaCounterTest, KeepsLatestObservedRate)
{
    const auto start = TInstant::Seconds(1000);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    for (int i = 1; i <= 20; ++i) {
        counter.Update(300000 * 15.0 * i, start + TDuration::Seconds(15 * i));
    }
    ASSERT_TRUE(counter.GetLastRate());
    EXPECT_NEAR(*counter.GetLastRate(), 300000, 1);
    EXPECT_DOUBLE_EQ(counter.GetTotal(), 90000000);
}

TEST(TSimpleEmaCounterTest, ReadingDoesNotProvideObservationHistory)
{
    TSimpleEmaCounter counter;
    EXPECT_FALSE(counter.GetLastRate());
    counter.Update(0, TInstant::Seconds(1000));
    counter.Inc(100, TInstant::Seconds(1001));
    EXPECT_FALSE(counter.GetLastRate());
}

TEST(TSimpleEmaCounterTest, ExplicitZeroObservationReducesRate)
{
    const auto start = TInstant::Seconds(1000);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    counter.Inc(30000, start + TDuration::Seconds(30));
    ASSERT_TRUE(counter.GetLastRate());
    const auto before = *counter.GetLastRate();
    ASSERT_GT(before, 800);
    counter.Inc(0, start + TDuration::Seconds(30 * 2));
    EXPECT_LT(*counter.GetLastRate(), before / 7);
    EXPECT_GT(*counter.GetLastRate(), before / 8);
    EXPECT_DOUBLE_EQ(counter.GetTotal(), 30000);
}

TEST(TSimpleEmaCounterTest, UnchangedTotalIsAZeroObservation)
{
    const auto start = TInstant::Seconds(1000);
    const auto period = TDuration::Seconds(30);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    counter.Update(30000, start + period);
    ASSERT_TRUE(counter.GetLastRate());
    const auto before = *counter.GetLastRate();
    counter.Update(30000, start + period * 2);
    EXPECT_LT(*counter.GetLastRate(), before / 7);
    EXPECT_GT(*counter.GetLastRate(), before / 8);
    EXPECT_EQ(counter.GetLastRate(), counter.GetDecayedRate(start + period * 2));
    EXPECT_DOUBLE_EQ(counter.GetTotal(), 30000);
}

TEST(TSimpleEmaCounterTest, DecayedReadsDoNotChangeObservedRateOrLaterUpdates)
{
    const auto start = TInstant::Seconds(1000);
    const auto period = TDuration::Seconds(30);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    counter.Inc(30000, start + period);
    const auto before = *counter.GetLastRate();
    auto unread = counter;
    EXPECT_LT(*counter.GetDecayedRate(start + period * 2), before / 7);
    EXPECT_DOUBLE_EQ(*counter.GetLastRate(), before);
    EXPECT_DOUBLE_EQ(counter.GetTotal(), unread.GetTotal());
    counter.Inc(30000, start + period * 2);
    unread.Inc(30000, start + period * 2);
    EXPECT_DOUBLE_EQ(*counter.GetLastRate(), *unread.GetLastRate());
}

TEST(TSimpleEmaCounterTest, IrregularObservationsUseTheirWholeIntervals)
{
    const auto start = TInstant::Seconds(1000);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    int elapsed = 0;
    for (int i = 0; i < 100; ++i) {
        elapsed += i % 2 ? 18 : 12;
        counter.Update(300000.0 * elapsed, start + TDuration::Seconds(elapsed));
        if (i >= 20) {
            ASSERT_TRUE(counter.GetLastRate());
            EXPECT_NEAR(*counter.GetLastRate(), 300000, 1);
        }
    }
}

TEST(TSimpleEmaCounterTest, ResumedWorkDoesNotEraseObservedIdle)
{
    const auto start = TInstant::Seconds(1000);
    TSimpleEmaCounter counter;
    counter.Update(0, start);
    counter.Inc(30000, start + TDuration::Seconds(30));
    counter.Inc(0, start + TDuration::Seconds(60));
    counter.Inc(30000, start + TDuration::Seconds(90));
    ASSERT_TRUE(counter.GetLastRate());
    EXPECT_GT(*counter.GetLastRate(), 870);
    EXPECT_LT(*counter.GetLastRate(), 890);
    EXPECT_DOUBLE_EQ(counter.GetTotal(), 60000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
