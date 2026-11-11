#include <yt/yt/flow/library/cpp/source_read_delay/estimator.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TInstant At(int seconds)
{
    return TInstant::Seconds(seconds);
}

TSourceReadDelayObservation Observe(int now = 10000, int gate = 6400)
{
    return {
        .ObservationTime = At(now),
        .ReadDelayEventThreshold = At(gate),
        .NewRate = TSourceWork{1600, 20000},
        .Unread = TSourceWork{7200000, 72000000},
    };
}

class TSourceReadDelayEstimatorTest
    : public ::testing::Test
{
protected:
    TSourceReadDelayEstimatorSpecPtr Config = New<TSourceReadDelayEstimatorSpec>();
    TSourceReadDelayEstimator Estimator;

    void Train(int frontier = 2800, int offset = 0)
    {
        Estimator.RegisterInput("00", At(frontier - 60), At(frontier - 60 - offset), {60000, 600000}, At(9999), Config);
        Estimator.RegisterInput("01", At(frontier), At(frontier - offset), {60000, 600000}, At(10000), Config);
        Estimator.Estimate(Observe(9999, 6399), Config);
    }
};

TEST_F(TSourceReadDelayEstimatorTest, InterpolatesCountAndBytesIndependently)
{
    Train();
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.Offered->Count, 1300);
    EXPECT_DOUBLE_EQ(result.Offered->Bytes, 15000);
    EXPECT_DOUBLE_EQ(result.NewRateWeight, 0.5);
    ASSERT_TRUE(result.Ready);
    EXPECT_DOUBLE_EQ(result.Ready->Count, 3600000);
}

TEST_F(TSourceReadDelayEstimatorTest, FollowsThresholdWithoutNewBias)
{
    Train(6400);
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.Offered->Count, 1000);
    EXPECT_DOUBLE_EQ(result.Offered->Bytes, 10000);
    EXPECT_DOUBLE_EQ(result.Ready->Count, 0);
}

TEST_F(TSourceReadDelayEstimatorTest, ReadAheadUsesLocalDensityAndHasNoUnreadReady)
{
    Train(6460);
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.Offered->Count, 1000);
    EXPECT_DOUBLE_EQ(result.Ready->Count, 0);
}

TEST_F(TSourceReadDelayEstimatorTest, ConvertsTransformedEventGateToSystemTime)
{
    Train(2800, 1000);
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.SystemEventOffsetSeconds, 1000);
    EXPECT_NEAR(result.NewRateWeight, 4600.0 / 7200, 1e-12);
}

TEST_F(TSourceReadDelayEstimatorTest, CatchupReadSpeedDoesNotChangeProducerDensity)
{
    Train();
    TSourceReadDelayEstimator slow;
    slow.RegisterInput("00", At(2740), At(2740), {60000, 600000}, At(9900), Config);
    slow.RegisterInput("01", At(2800), At(2800), {60000, 600000}, At(10000), Config);
    slow.Estimate(Observe(9999, 6399), Config);
    const auto expected = Estimator.Estimate(Observe(), Config);
    const auto actual = slow.Estimate(Observe(), Config);
    ASSERT_TRUE(actual.Offered);
    EXPECT_DOUBLE_EQ(actual.Offered->Count, expected.Offered->Count);
    EXPECT_DOUBLE_EQ(actual.Offered->Bytes, expected.Offered->Bytes);
}

TEST_F(TSourceReadDelayEstimatorTest, TimestampGroupsSurviveSplitAndRestart)
{
    Estimator.RegisterInput("00", At(2740), At(2740), {20000, 200000}, At(9999), Config);
    auto restored = NYTree::ConvertTo<TSourceReadDelayEstimator>(NYson::ConvertToYsonString(Estimator));
    restored.RegisterInput("01", At(2740), At(2740), {40000, 400000}, At(9999), Config);
    restored.RegisterInput("02", At(2800), At(2800), {7, 8}, At(10000), Config);
    restored.Estimate(Observe(9999, 6399), Config);
    const auto result = restored.Estimate(Observe(), Config);
    EXPECT_DOUBLE_EQ(result.HistoricalDensity.Count, 1000);
    EXPECT_DOUBLE_EQ(result.HistoricalDensity.Bytes, 10000);
}

TEST_F(TSourceReadDelayEstimatorTest, RegressingTimestampDoesNotLoseInput)
{
    Estimator.RegisterInput("00", At(2740), At(2740), {20000, 200000}, At(9999), Config);
    Estimator.RegisterInput("01", At(2730), At(2730), {40000, 400000}, At(9999), Config);
    Estimator.RegisterInput("02", At(2800), At(2800), {7, 8}, At(10000), Config);
    Estimator.Estimate(Observe(9999, 6399), Config);
    EXPECT_DOUBLE_EQ(Estimator.Estimate(Observe(), Config).HistoricalDensity.Count, 1000);
}

TEST_F(TSourceReadDelayEstimatorTest, RestoredLearningSurvivesWhileReadyWaitsForReplay)
{
    Train();
    Estimator.Estimate(Observe(), Config);
    auto restored = NYTree::ConvertTo<TSourceReadDelayEstimator>(NYson::ConvertToYsonString(Estimator));
    const auto initial = restored.Estimate(Observe(10001, 6401), Config);
    ASSERT_TRUE(initial.Offered);
    EXPECT_NEAR(initial.Offered->Count, 1300, 1);
    EXPECT_FALSE(initial.Ready);
    restored.RegisterInput("00", At(2740), At(2740), {1e9, 1e10}, At(10001), Config);
    const auto replay = restored.Estimate(Observe(10002, 6402), Config);
    ASSERT_TRUE(replay.Ready);
    EXPECT_GT(replay.Ready->Count, 0);
    EXPECT_DOUBLE_EQ(replay.HistoricalDensity.Count, 1000);
}

TEST_F(TSourceReadDelayEstimatorTest, ReplayBoundaryIsIndependentOfSavedReadAhead)
{
    Estimator.RegisterInput("00", At(40), At(40), {2000, 20000}, At(199), Config);
    Estimator.RegisterInput("01", At(60), At(60), {4000, 40000}, At(199), Config);
    Estimator.RegisterInput("02", At(100), At(100), {1000, 10000}, At(199), Config);
    Estimator.Estimate(Observe(199, 79), Config);
    auto restored = NYTree::ConvertTo<TSourceReadDelayEstimator>(NYson::ConvertToYsonString(Estimator));
    EXPECT_FALSE(restored.Estimate(Observe(200, 80), Config).Ready);
    restored.RegisterInput("01", At(60), At(60), {4000, 40000}, At(200), Config);
    const auto partial = restored.Estimate(Observe(201, 81), Config);
    ASSERT_TRUE(partial.Ready);
    EXPECT_GT(partial.Ready->Count, 0);
    restored.RegisterInput("02", At(100), At(100), {1000, 10000}, At(201), Config);
    const auto complete = restored.Estimate(Observe(202, 82), Config);
    EXPECT_DOUBLE_EQ(complete.Ready->Count, 0);
    EXPECT_DOUBLE_EQ(complete.HistoricalDensity.Count, 100);
}

TEST_F(TSourceReadDelayEstimatorTest, FilteredInputTrainsDensityWithoutInventingOffset)
{
    Estimator.RegisterInput("00", At(2740), std::nullopt, {60000, 600000}, At(9999), Config);
    Estimator.RegisterInput("01", At(2800), std::nullopt, {60000, 600000}, At(10000), Config);
    Estimator.Estimate(Observe(9999, 6399), Config);
    EXPECT_FALSE(Estimator.Estimate(Observe(), Config).Offered);
    Estimator.RegisterInput("02", At(2860), At(2760), {60000, 600000}, At(10001), Config);
    const auto result = Estimator.Estimate(Observe(10001, 6401), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.SystemEventOffsetSeconds, 100);
    EXPECT_DOUBLE_EQ(result.HistoricalDensity.Count, 1000);
}

TEST_F(TSourceReadDelayEstimatorTest, StationaryGateStopsOfferedButRetainsReady)
{
    Config->SmoothingPeriod = TDuration::Seconds(1);
    Train();
    Estimator.Estimate(Observe(), Config);
    const auto result = Estimator.Estimate(Observe(10060, 6400), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_NEAR(result.Offered->Count, 0, 1e-9);
    EXPECT_GT(result.Ready->Count, 0);
}

TEST_F(TSourceReadDelayEstimatorTest, UnknownNewDoesNotHideKnownReady)
{
    Train();
    auto observation = Observe();
    observation.NewRate.reset();
    const auto result = Estimator.Estimate(observation, Config);
    EXPECT_FALSE(result.Offered);
    ASSERT_TRUE(result.Ready);
    EXPECT_GT(result.Ready->Count, 0);
}

TEST_F(TSourceReadDelayEstimatorTest, EmptyUnreadIsKnownBeforeLearning)
{
    auto observation = Observe();
    observation.Unread = TSourceWork{};
    const auto result = Estimator.Estimate(observation, Config);
    EXPECT_FALSE(result.Offered);
    ASSERT_TRUE(result.Ready);
    EXPECT_EQ(result.Ready->Count, 0);
}

TEST_F(TSourceReadDelayEstimatorTest, AtTailUsesNewAndAllUnread)
{
    Train(9940, 4000);
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.Offered->Count, 1600);
    EXPECT_DOUBLE_EQ(result.Ready->Count, Observe().Unread->Count);
}

TEST_F(TSourceReadDelayEstimatorTest, StaleDensityIsUnknownAndFreshInputReusesLearning)
{
    Train();
    Estimator.Estimate(Observe(), Config);
    EXPECT_FALSE(Estimator.Estimate(Observe(10600, 7000), Config).Offered);
    EXPECT_FALSE(Estimator.Estimate(Observe(10601, 7001), Config).Offered);
    Estimator.RegisterInput("02", At(2860), At(2860), {60000, 600000}, At(10602), Config);
    const auto result = Estimator.Estimate(Observe(10602, 7002), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.HistoricalDensity.Count, 1000);
}

TEST_F(TSourceReadDelayEstimatorTest, DynamicSmoothingChangesResponseWithoutResettingDensity)
{
    Train();
    Estimator.Estimate(Observe(), Config);
    auto observation = Observe(10010, 6410);
    observation.NewRate = TSourceWork{4000, 40000};
    Config->SmoothingPeriod = TDuration::Seconds(1);
    const auto result = Estimator.Estimate(observation, Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_NEAR(result.Offered->Count, 2502, 1);
    EXPECT_DOUBLE_EQ(result.HistoricalDensity.Count, 1000);
}

TEST_F(TSourceReadDelayEstimatorTest, SameTimestampRetainsOfferedAndRecomputesReady)
{
    Train();
    const auto first = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(first.Offered);
    auto observation = Observe();
    observation.Unread->Count /= 2;
    observation.Unread->Bytes /= 4;
    const auto repeated = Estimator.Estimate(observation, Config);
    ASSERT_TRUE(repeated.Offered);
    EXPECT_DOUBLE_EQ(repeated.Offered->Count, first.Offered->Count);
    EXPECT_DOUBLE_EQ(repeated.Offered->Bytes, first.Offered->Bytes);
    ASSERT_TRUE(repeated.Ready);
    EXPECT_DOUBLE_EQ(repeated.Ready->Count, first.Ready->Count / 2);
    EXPECT_DOUBLE_EQ(repeated.Ready->Bytes, first.Ready->Bytes / 4);
}

TEST_F(TSourceReadDelayEstimatorTest, SameTimestampDoesNotInventInitialGateSpeed)
{
    Train();
    EXPECT_FALSE(Estimator.Estimate(Observe(9999, 6399), Config).Offered);
    EXPECT_TRUE(Estimator.Estimate(Observe(), Config).Offered);
}

TEST_F(TSourceReadDelayEstimatorTest, FirstKnownNewDoesNotStartSmoothingFromZero)
{
    Estimator.RegisterInput("00", At(2740), At(2740), {60000, 600000}, At(9999), Config);
    Estimator.RegisterInput("01", At(2800), At(2800), {60000, 600000}, At(10000), Config);
    auto observation = Observe(9999, 6399);
    observation.NewRate.reset();
    EXPECT_FALSE(Estimator.Estimate(observation, Config).Offered);
    const auto result = Estimator.Estimate(Observe(), Config);
    ASSERT_TRUE(result.Offered);
    EXPECT_DOUBLE_EQ(result.Offered->Count, 1300);
    EXPECT_DOUBLE_EQ(result.Offered->Bytes, 15000);
}

TEST_F(TSourceReadDelayEstimatorTest, SameTimestampAfterGapWaitsForNewGateInterval)
{
    Config->MaxDensityAge = TDuration::Hours(1);
    Train();
    ASSERT_TRUE(Estimator.Estimate(Observe(), Config).Offered);
    EXPECT_FALSE(Estimator.Estimate(Observe(10600, 7000), Config).Offered);
    EXPECT_FALSE(Estimator.Estimate(Observe(10600, 7000), Config).Offered);
    EXPECT_TRUE(Estimator.Estimate(Observe(10601, 7001), Config).Offered);
}

TEST_F(TSourceReadDelayEstimatorTest, DensityMismatchKeepsNumbersAndSeparatesCountFromBytes)
{
    Train();
    auto observation = Observe();
    observation.NewRate = TSourceWork{10000, 10000};
    Config->SmoothingPeriod = TDuration::MilliSeconds(1);
    auto result = Estimator.Estimate(observation, Config);
    EXPECT_TRUE(result.CountDensityMismatch);
    EXPECT_FALSE(result.BytesDensityMismatch);
    ASSERT_TRUE(result.Offered);
    ASSERT_TRUE(result.Ready);
    EXPECT_NEAR(result.Offered->Count, 5500, 1e-6);
    EXPECT_DOUBLE_EQ(result.Offered->Bytes, 10000);
    observation.ObservationTime += TDuration::Seconds(1);
    observation.NewRate = TSourceWork{1000, 10000};
    result = Estimator.Estimate(observation, Config);
    EXPECT_FALSE(result.CountDensityMismatch);
    EXPECT_FALSE(result.BytesDensityMismatch);
    EXPECT_TRUE(result.Offered);
}

TEST_F(TSourceReadDelayEstimatorTest, TimestampWarningSurvivesRestartAndExpiresWithoutSuppressingNumbers)
{
    Train();
    Estimator.RegisterInput("02", At(2801), At(2201), {1000, 10000}, At(10000), Config);
    auto restored = NYTree::ConvertTo<TSourceReadDelayEstimator>(NYson::ConvertToYsonString(Estimator));
    auto result = restored.Estimate(Observe(), Config);
    ASSERT_TRUE(result.EventOffsetDeviationSeconds);
    EXPECT_DOUBLE_EQ(*result.EventOffsetDeviationSeconds, 600);
    ASSERT_TRUE(result.Offered);
    Estimator.RegisterInput("03", At(2802), At(2802), {1000, 10000}, At(10001), Config);
    EXPECT_TRUE(Estimator.Estimate(Observe(10001, 6401), Config).Ready);
    result = restored.Estimate(Observe(10301, 6701), Config);
    EXPECT_FALSE(result.EventOffsetDeviationSeconds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
