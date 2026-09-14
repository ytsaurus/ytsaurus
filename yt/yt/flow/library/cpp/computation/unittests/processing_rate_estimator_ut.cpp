#include <yt/yt/flow/library/cpp/computation/processing_rate_estimator.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto StartTime = TInstant::Seconds(1000);

TEST(TProcessingRateEstimatorTest, CommittedWindowsHaveTheSameUtilizationAsTiming)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    for (int seconds = 10; seconds <= 600; seconds += 10) {
        estimator.StartEpoch(totals);
        estimator.AddInputs(1000, 8000);
        // Diagnostic EMA uses an independent decay period and must not affect these rates.
        totals[EEpochPartKind::Processing].WallTimeEma = TDuration::Seconds(seconds * 100);
        totals[EEpochPartKind::Waiting].WallTimeEma = TDuration::MicroSeconds(seconds);
        totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(2);
        totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(8);
        auto rates = estimator.Commit(totals, StartTime + TDuration::Seconds(seconds));
        EXPECT_EQ(rates->Rate1m.has_value(), seconds >= 60);
        EXPECT_EQ(rates->Rate10m.has_value(), seconds >= 600);
        for (const auto& rate : {rates->Rate1m, rates->Rate10m}) {
            if (!rate) {
                continue;
            }
            ASSERT_TRUE(rate->Capacity);
            EXPECT_NEAR(rate->Processed.ProcessedMessagesPerSecond, 100, 1e-10);
            EXPECT_NEAR(rate->Capacity->ProcessedMessagesPerSecond, 500, 1e-10);
            EXPECT_NEAR(rate->Processed.ProcessedBytesPerSecond, 800, 1e-10);
            EXPECT_NEAR(rate->Processed.ProcessedMessagesPerSecond / rate->Capacity->ProcessedMessagesPerSecond, 0.2, 1e-10);
            EXPECT_NEAR(rate->Processed.ProcessedBytesPerSecond / rate->Capacity->ProcessedBytesPerSecond, 0.2, 1e-10);
        }
    }
}

TEST(TProcessingRateEstimatorTest, FailedEpochDiscardsInputsAndTiming)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    auto now = StartTime;
    for (int attempt = 0; attempt < 2; ++attempt) {
        estimator.StartEpoch(totals);
        estimator.AddInputs(900000, 9000000);
        totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(200);
        totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(500);
        now += TDuration::Seconds(700);

        estimator.StartEpoch(totals);
        estimator.AddInputs(1000, 8000);
        totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(10);
        totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(50);
        now += TDuration::Minutes(1);
        auto rates = estimator.Commit(totals, now);
        ASSERT_TRUE(rates->Rate1m);
        ASSERT_TRUE(rates->Rate1m->Capacity);
        EXPECT_NEAR(rates->Rate1m->Capacity->ProcessedMessagesPerSecond, 100, 1e-10);
        EXPECT_NEAR(rates->Rate1m->Capacity->ProcessedBytesPerSecond, 800, 1e-10);
        EXPECT_NEAR(rates->Rate1m->Processed.ProcessedMessagesPerSecond, 1000. / 60, 1e-10);
    }
}

TEST(TProcessingRateEstimatorTest, ExcludesInitializationAndPreservesCommittedTail)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(100);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(200);
    estimator.StartEpoch(totals);
    estimator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(50);
    auto first = estimator.Commit(totals, StartTime + TDuration::Minutes(1));
    ASSERT_TRUE(first->Rate1m);
    ASSERT_TRUE(first->Rate1m->Capacity);
    EXPECT_NEAR(first->Rate1m->Capacity->ProcessedMessagesPerSecond, 100, 1e-10);
    EXPECT_NEAR(first->Rate1m->Processed.ProcessedMessagesPerSecond, 1000. / 60, 1e-10);

    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(2);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(8);
    estimator.StartEpoch(totals);
    estimator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(8);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(42);
    auto second = estimator.Commit(totals, StartTime + TDuration::Minutes(2));
    ASSERT_TRUE(second->Rate1m);
    ASSERT_TRUE(second->Rate1m->Capacity);
    EXPECT_NEAR(second->Rate1m->Capacity->ProcessedMessagesPerSecond, 100, 1e-10);
    EXPECT_NEAR(second->Rate1m->Capacity->ProcessedBytesPerSecond, 800, 1e-10);
    EXPECT_NEAR(second->Rate1m->Processed.ProcessedMessagesPerSecond, 1000. / 60, 1e-10);
}

TEST(TProcessingRateEstimatorTest, PublishOnlyEpochChargesWorkWithoutRecountingInputs)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    estimator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(50);
    auto first = estimator.Commit(totals, StartTime + TDuration::Minutes(1));
    ASSERT_TRUE(first->Rate1m);
    ASSERT_TRUE(first->Rate1m->Capacity);
    EXPECT_NEAR(first->Rate1m->Capacity->ProcessedMessagesPerSecond, 100, 1e-10);

    estimator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(30);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(30);
    auto published = estimator.Commit(totals, StartTime + TDuration::Minutes(2));
    ASSERT_TRUE(published->Rate1m);
    ASSERT_TRUE(published->Rate1m->Capacity);
    EXPECT_GT(published->Rate1m->Capacity->ProcessedMessagesPerSecond, 0);
    EXPECT_LT(published->Rate1m->Capacity->ProcessedMessagesPerSecond, 100);
    EXPECT_NEAR(published->Rate1m->Capacity->ProcessedBytesPerSecond / published->Rate1m->Capacity->ProcessedMessagesPerSecond, 8, 1e-10);
}

TEST(TProcessingRateEstimatorTest, EmptyPollReportsZeroProcessedWithoutCapacity)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(49);
    auto cold = estimator.Commit(totals, StartTime + TDuration::Seconds(59));
    EXPECT_FALSE(cold->Rate1m);
    estimator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(1);
    auto zero = estimator.Commit(totals, StartTime + TDuration::Minutes(1));
    ASSERT_TRUE(zero->Rate1m);
    EXPECT_EQ(zero->Rate1m->Processed.ProcessedMessagesPerSecond, 0);
    EXPECT_FALSE(zero->Rate1m->Capacity);
    EXPECT_FALSE(zero->Rate10m);
}

TEST(TProcessingRateEstimatorTest, EmptyEpochTimeRemainsProcessing)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Minutes(10);
    auto empty = estimator.Commit(totals, StartTime + TDuration::Minutes(10));
    for (const auto& rate : {empty->Rate1m, empty->Rate10m}) {
        ASSERT_TRUE(rate);
        EXPECT_EQ(rate->Processed.ProcessedMessagesPerSecond, 0);
        EXPECT_EQ(rate->Processed.ProcessedBytesPerSecond, 0);
        EXPECT_FALSE(rate->Capacity);
    }

    estimator.StartEpoch(totals);
    estimator.AddInputs(600, 4800);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Minutes(10);
    auto active = estimator.Commit(totals, StartTime + TDuration::Minutes(20));
    for (const auto& rate : {active->Rate1m, active->Rate10m}) {
        ASSERT_TRUE(rate);
        ASSERT_TRUE(rate->Capacity);
        const auto capacity = rate->Capacity->ProcessedMessagesPerSecond;
        EXPECT_GT(capacity, 0);
        EXPECT_LT(capacity, 1);
        EXPECT_NEAR(rate->Processed.ProcessedMessagesPerSecond, capacity, 1e-10);
        EXPECT_NEAR(rate->Processed.ProcessedBytesPerSecond, capacity * 8, 1e-10);
    }
}

TEST(TProcessingRateEstimatorTest, ZeroProcessingTimeHasNoCapacity)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    estimator.AddInputs(600, 0);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Minutes(10);
    auto rates = estimator.Commit(totals, StartTime + TDuration::Minutes(10));
    for (const auto& rate : {rates->Rate1m, rates->Rate10m}) {
        ASSERT_TRUE(rate);
        EXPECT_NEAR(rate->Processed.ProcessedMessagesPerSecond, 1, 1e-10);
        EXPECT_EQ(rate->Processed.ProcessedBytesPerSecond, 0);
        EXPECT_FALSE(rate->Capacity);
    }
}

TEST(TProcessingRateEstimatorTest, IrregularBatchSizesDoNotChangeCapacity)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    auto now = StartTime;
    for (int epoch = 1; epoch <= 100; ++epoch) {
        estimator.StartEpoch(totals);
        auto count = (epoch % 7 + 1) * 900;
        auto work = TDuration::Seconds(count / 100);
        auto wait = TDuration::Seconds(epoch % 53);
        estimator.AddInputs(count, count * 16);
        now += work + wait;
        totals[EEpochPartKind::Processing].TotalDuration += work;
        totals[EEpochPartKind::Waiting].TotalDuration += wait;
        auto rates = estimator.Commit(totals, now);
        if (rates->Rate1m) {
            ASSERT_TRUE(rates->Rate1m->Capacity);
            EXPECT_NEAR(rates->Rate1m->Capacity->ProcessedMessagesPerSecond, 100, 1e-10);
            EXPECT_NEAR(rates->Rate1m->Capacity->ProcessedBytesPerSecond, 1600, 1e-10);
        }
    }
}

TEST(TProcessingRateEstimatorTest, ShortWindowReactsFaster)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    TComputationProcessingRatesPtr rates;
    for (int seconds = 10; seconds <= 700; seconds += 10) {
        estimator.StartEpoch(totals);
        auto count = seconds <= 600 ? 1000 : 2000;
        estimator.AddInputs(count, count * 8);
        totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(1);
        totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(9);
        rates = estimator.Commit(totals, StartTime + TDuration::Seconds(seconds));
    }
    ASSERT_TRUE(rates->Rate1m);
    ASSERT_TRUE(rates->Rate10m);
    ASSERT_TRUE(rates->Rate1m->Capacity);
    ASSERT_TRUE(rates->Rate10m->Capacity);
    EXPECT_GT(rates->Rate1m->Capacity->ProcessedMessagesPerSecond, rates->Rate10m->Capacity->ProcessedMessagesPerSecond);
    for (const auto& rate : {rates->Rate1m, rates->Rate10m}) {
        EXPECT_NEAR(rate->Processed.ProcessedMessagesPerSecond / rate->Capacity->ProcessedMessagesPerSecond, 0.1, 1e-10);
    }
}

TEST(TProcessingRateEstimatorTest, IdleDecayDoesNotProduceInfiniteCapacity)
{
    TProcessingRateEstimator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    estimator.AddInputs(1000, 0);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(50);
    auto first = estimator.Commit(totals, StartTime + TDuration::Minutes(1));
    ASSERT_TRUE(first->Rate1m);
    ASSERT_TRUE(first->Rate1m->Capacity);
    EXPECT_EQ(first->Rate1m->Capacity->ProcessedBytesPerSecond, 0);
    estimator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(1);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Days(10) - TDuration::Seconds(1);
    auto idle = estimator.Commit(totals, StartTime + TDuration::Days(10));
    ASSERT_TRUE(idle->Rate1m);
    EXPECT_FALSE(idle->Rate1m->Capacity);
    EXPECT_EQ(idle->Rate1m->Processed.ProcessedMessagesPerSecond, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
