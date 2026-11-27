#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timestamp_statistics.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The documented layout: bit 63 is set for a fresh message, bits 56..62 hold 15 - priority.
TSystemTimestamp MakeKey(bool stale, int priority, ui64 low)
{
    return TSystemTimestamp((static_cast<ui64>(!stale) << 63) | (static_cast<ui64>(15 - priority) << 56) | low);
}

TSystemTimestamp MakeFreshKey(int priority, ui64 low)
{
    return MakeKey(/*stale*/ false, priority, low);
}

TSystemTimestamp MakeStaleKey(int priority, ui64 low)
{
    return MakeKey(/*stale*/ true, priority, low);
}

// A message is stale when its SystemTimestamp is below this bound.
constexpr auto TestStaleBound = TSystemTimestamp(1'000);

TSystemTimestamp MakeTestOrderingKey(TSystemTimestamp alignmentTimestamp, double bias, int priority, bool stale)
{
    auto systemTimestamp = stale ? TSystemTimestamp(TestStaleBound.Underlying() - 1) : TestStaleBound;
    return MakeOrderingKey(alignmentTimestamp, systemTimestamp, {.Bias = bias, .Priority = priority}, TestStaleBound);
}

TEST(TOrderingKeyTest, StaleBound)
{
    const auto alignment = TSystemTimestamp(500);
    // Stale only strictly below the bound.
    EXPECT_EQ(MakeOrderingKey(alignment, TestStaleBound, /*params*/ {}, TestStaleBound), MakeFreshKey(0, 500));
    EXPECT_EQ(MakeOrderingKey(alignment, TSystemTimestamp(TestStaleBound.Underlying() - 1), /*params*/ {}, TestStaleBound), MakeStaleKey(0, 500));
    // A zero bound disables the boost.
    EXPECT_EQ(MakeOrderingKey(alignment, TSystemTimestamp(1), /*params*/ {}, ZeroSystemTimestamp), MakeFreshKey(0, 500));
}

TEST(TOrderingKeyTest, Layout)
{
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 0, /*priority*/ 0, /*stale*/ false), TSystemTimestamp((1ULL << 63) | (15ULL << 56) | 1000));
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 0, /*priority*/ 0, /*stale*/ true), TSystemTimestamp((15ULL << 56) | 1000));
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ -10.5, /*priority*/ 1, /*stale*/ false), MakeFreshKey(1, 989));
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 20, /*priority*/ -1, /*stale*/ true), MakeStaleKey(-1, 1020));
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 0, /*priority*/ MaxStreamPriority, /*stale*/ false), TSystemTimestamp((1ULL << 63) | 1000));
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 0, /*priority*/ -MaxStreamPriority, /*stale*/ false), TSystemTimestamp((1ULL << 63) | (30ULL << 56) | 1000));
}

TEST(TOrderingKeyTest, Bounds)
{
    // The largest alignment, the largest average bias plus the maximum stream delay.
    const auto maxAlignment = MaxAdequateTimestamp;
    const double maxBias = static_cast<double>(MaxAdequateTimestamp.Underlying()) + MaxStreamDelay.SecondsFloat();
    const auto largest = MakeTestOrderingKey(maxAlignment, /*bias*/ maxBias, /*priority*/ -MaxStreamPriority, /*stale*/ false);
    EXPECT_EQ(largest.Underlying() >> 56, 128u + 30u);
    EXPECT_LT(largest, InfinitySystemTimestamp);
    // The largest key of a priority class is below the smallest key of the next class.
    EXPECT_LT(
        MakeTestOrderingKey(maxAlignment, /*bias*/ maxBias, /*priority*/ -MaxStreamPriority + 1, /*stale*/ false),
        MakeTestOrderingKey(TSystemTimestamp(1), /*bias*/ -1, /*priority*/ -MaxStreamPriority, /*stale*/ false));
    // Any stale key is below any fresh key.
    EXPECT_LT(
        MakeTestOrderingKey(maxAlignment, /*bias*/ maxBias, /*priority*/ -MaxStreamPriority, /*stale*/ true),
        MakeTestOrderingKey(TSystemTimestamp(1), /*bias*/ -1, /*priority*/ MaxStreamPriority, /*stale*/ false));
}

TEST(TOrderingKeyTest, PriorityAppliesAmongStaleKeys)
{
    EXPECT_LT(
        MakeTestOrderingKey(TSystemTimestamp(1000), /*bias*/ 0, /*priority*/ 0, /*stale*/ true),
        MakeTestOrderingKey(TSystemTimestamp(1), /*bias*/ 0, /*priority*/ -1, /*stale*/ true));
}

TEST(TOrderingKeyTest, NegativeStabilizedTimestampIsClamped)
{
    EXPECT_EQ(MakeTestOrderingKey(TSystemTimestamp(100), /*bias*/ -1000, /*priority*/ 0, /*stale*/ false), MakeFreshKey(0, 0));
    EXPECT_LT(
        MakeTestOrderingKey(TSystemTimestamp(100), /*bias*/ -1000, /*priority*/ 0, /*stale*/ false),
        MakeTestOrderingKey(TSystemTimestamp(1), /*bias*/ 0, /*priority*/ -1, /*stale*/ false));
}

TEST(TOrderingKeyTest, StaleInputBoostBound)
{
    const auto now = TInstant::Seconds(10'000);
    EXPECT_EQ(GetStaleInputBoostBound(now, TDuration::Seconds(1'000)), TSystemTimestamp(9'000));
    // Zero disables the boost: no SystemTimestamp is below the bound.
    EXPECT_EQ(GetStaleInputBoostBound(now, TDuration::Zero()), ZeroSystemTimestamp);
    // An age beyond the clock saturates and also disables the boost.
    EXPECT_EQ(GetStaleInputBoostBound(now, TDuration::Days(365)), ZeroSystemTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTimestampStatisticsTest, RegistersMaxAdequateTimestamp)
{
    TMessageMeta meta;
    meta.SystemTimestamp = MaxAdequateTimestamp;
    meta.AlignmentTimestamp = MaxAdequateTimestamp;
    meta.EventTimestamp = TSystemTimestamp(1);

    TTimestampStatistics statistics;
    statistics.RegisterMessage(TTimestampStatistics::ComputeRegistrationInfo(meta));
    meta.EventTimestamp = MaxAdequateTimestamp;
    statistics.RegisterMessage(TTimestampStatistics::ComputeRegistrationInfo(meta));

    EXPECT_EQ(statistics.MessageCount, 2);
    EXPECT_DOUBLE_EQ(
        statistics.EstimateAlignmentToEventTimestampBias(),
        (1.0 - static_cast<double>(MaxAdequateTimestamp.Underlying())) / 2);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TThrottlerPriorityTest, NoInformation)
{
    EXPECT_EQ(ComputeThrottlerPriority(InfinitySystemTimestamp, InfinitySystemTimestamp), ZeroSystemTimestamp);
}

TEST(TThrottlerPriorityTest, SourceOnlyIsNeverStale)
{
    EXPECT_EQ(ComputeThrottlerPriority(InfinitySystemTimestamp, TSystemTimestamp(500)), MakeFreshKey(0, 500));
    // However old the read timestamp is, the source key stays in the fresh class.
    EXPECT_EQ(ComputeThrottlerPriority(InfinitySystemTimestamp, TSystemTimestamp(1)), MakeFreshKey(0, 1));
}

TEST(TThrottlerPriorityTest, MinOfInputAndEncodedSource)
{
    const auto source = TSystemTimestamp(500);
    // A default-priority input key with a larger timestamp, and a demoted one with a smaller timestamp.
    EXPECT_EQ(ComputeThrottlerPriority(MakeFreshKey(0, 600), source), MakeFreshKey(0, 500));
    EXPECT_EQ(ComputeThrottlerPriority(MakeFreshKey(-1, 10), source), MakeFreshKey(0, 500));
    // An input key below the encoded source: a smaller timestamp, a promoted stream, or a stale front.
    EXPECT_EQ(ComputeThrottlerPriority(MakeFreshKey(0, 400), source), MakeFreshKey(0, 400));
    EXPECT_EQ(ComputeThrottlerPriority(MakeFreshKey(1, 900), source), MakeFreshKey(1, 900));
    EXPECT_EQ(ComputeThrottlerPriority(MakeStaleKey(-1, 900), source), MakeStaleKey(-1, 900));
}

TEST(TThrottlerPriorityTest, SourceWithoutDataIsPassedRaw)
{
    EXPECT_EQ(ComputeThrottlerPriority(InfinitySystemTimestamp, ZeroSystemTimestamp), ZeroSystemTimestamp);
    EXPECT_EQ(ComputeThrottlerPriority(MakeStaleKey(15, 1), ZeroSystemTimestamp), ZeroSystemTimestamp);
    EXPECT_EQ(ComputeThrottlerPriority(MakeFreshKey(0, 600), ZeroSystemTimestamp), ZeroSystemTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
