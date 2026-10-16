#include <yt/yt/flow/library/cpp/computation/processing_observation_accumulator.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto StartTime = TInstant::Seconds(1000);

TEST(TProcessingObservationAccumulatorTest, PublicationIsCoherentAndDoesNotChangeDuringNextEpoch)
{
    TProcessingObservationAccumulator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    estimator.AddInputs(600, 4800);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(5);
    totals[EEpochPartKind::WaitingForInput].TotalDuration = TDuration::Seconds(30);
    totals[EEpochPartKind::WaitingForOutput].TotalDuration = TDuration::Seconds(15);
    auto first = estimator.Commit(totals, StartTime + TDuration::Minutes(1));
    EXPECT_EQ(first->Sequence, 1);
    EXPECT_EQ(first->CapturedAt, StartTime + TDuration::Minutes(1));
    EXPECT_EQ(first->ObservationDuration, TDuration::Minutes(1));
    EXPECT_EQ(first->ProcessedCount, 600);
    EXPECT_EQ(first->ProcessedByteSize, 4800);
    EXPECT_EQ(first->ProcessingTime, TDuration::Seconds(10));
    EXPECT_EQ(first->OtherWaitingTime, TDuration::Seconds(5));
    EXPECT_EQ(first->InputWaitingTime, TDuration::Seconds(30));
    EXPECT_EQ(first->OutputWaitingTime, TDuration::Seconds(15));

    estimator.StartEpoch(totals);
    estimator.AddInputs(90, 720);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(3);
    EXPECT_EQ(first->ProcessedCount, 600);
    auto second = estimator.Commit(totals, StartTime + TDuration::Seconds(63));
    EXPECT_EQ(second->Sequence, 2);
    EXPECT_EQ(second->ProcessedCount, 690);
    EXPECT_EQ(second->ProcessingTime, TDuration::Seconds(13));
    EXPECT_EQ(second->ObservationDuration, TDuration::Seconds(3));
    EXPECT_EQ(first->Sequence, 1);
    EXPECT_EQ(first->ProcessedCount, 600);
    EXPECT_EQ(first->ProcessingTime, TDuration::Seconds(10));
}

TEST(TProcessingObservationAccumulatorTest, FailedAttemptDoesNotAdvanceCommittedCounters)
{
    TProcessingObservationAccumulator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    estimator.AddInputs(9000, 72000);
    for (auto kind : {EEpochPartKind::Processing, EEpochPartKind::Waiting, EEpochPartKind::WaitingForInput, EEpochPartKind::WaitingForOutput})
    {
        totals[kind].TotalDuration = TDuration::Seconds(100);
    }
    estimator.StartEpoch(totals);
    estimator.AddInputs(12, 96);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(2);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(1);
    totals[EEpochPartKind::WaitingForInput].TotalDuration += TDuration::Seconds(3);
    totals[EEpochPartKind::WaitingForOutput].TotalDuration += TDuration::Seconds(4);
    auto committed = estimator.Commit(totals, StartTime + TDuration::Seconds(410));
    EXPECT_EQ(committed->Sequence, 1);
    EXPECT_EQ(committed->ProcessedCount, 12);
    EXPECT_EQ(committed->ProcessedByteSize, 96);
    EXPECT_EQ(committed->ObservationDuration, TDuration::Seconds(10));
    EXPECT_EQ(committed->ProcessingTime, TDuration::Seconds(2));
    EXPECT_EQ(committed->OtherWaitingTime, TDuration::Seconds(1));
    EXPECT_EQ(committed->InputWaitingTime, TDuration::Seconds(3));
    EXPECT_EQ(committed->OutputWaitingTime, TDuration::Seconds(4));
}

TEST(TProcessingObservationAccumulatorTest, EqualTimestampsStillHaveDistinctPublicationSequences)
{
    TProcessingObservationAccumulator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    auto now = StartTime + TDuration::Minutes(1);
    estimator.StartEpoch(totals);
    estimator.AddInputs(100, 800);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Minutes(1);
    auto first = estimator.Commit(totals, now);

    estimator.StartEpoch(totals);
    estimator.AddInputs(20, 160);
    auto second = estimator.Commit(totals, now);
    EXPECT_EQ(first->CapturedAt, second->CapturedAt);
    EXPECT_EQ(second->Sequence, first->Sequence + 1);
    EXPECT_EQ(second->ProcessedCount, 120);
    EXPECT_EQ(second->ProcessedByteSize, 960);
    EXPECT_EQ(second->ObservationDuration, TDuration::Zero());
}

TEST(TProcessingObservationAccumulatorTest, EmptyPublicationHasKnownWorkAndTime)
{
    TProcessingObservationAccumulator estimator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    estimator.StartEpoch(totals);
    totals[EEpochPartKind::WaitingForInput].TotalDuration = TDuration::Seconds(5);
    auto observation = estimator.Commit(totals, StartTime + TDuration::Seconds(5));
    EXPECT_EQ(observation->Sequence, 1);
    EXPECT_EQ(observation->ProcessedCount, 0);
    EXPECT_EQ(observation->ProcessedByteSize, 0);
    EXPECT_EQ(observation->InputWaitingTime, TDuration::Seconds(5));
}

TEST(TProcessingObservationAccumulatorTest, ExcludesInitializationAndPreservesCommittedTail)
{
    TProcessingObservationAccumulator accumulator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(100);
    totals[EEpochPartKind::Waiting].TotalDuration = TDuration::Seconds(200);
    accumulator.StartEpoch(totals);
    accumulator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(10);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(50);
    auto first = accumulator.Commit(totals, StartTime + TDuration::Minutes(1));
    EXPECT_EQ(first->ProcessingTime, TDuration::Seconds(10));
    EXPECT_EQ(first->OtherWaitingTime, TDuration::Seconds(50));
    EXPECT_EQ(first->ProcessedCount, 1000);

    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(2);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(8);
    accumulator.StartEpoch(totals);
    accumulator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(8);
    totals[EEpochPartKind::Waiting].TotalDuration += TDuration::Seconds(42);
    auto second = accumulator.Commit(totals, StartTime + TDuration::Minutes(2));
    EXPECT_EQ(second->ProcessingTime, TDuration::Seconds(20));
    EXPECT_EQ(second->OtherWaitingTime, TDuration::Seconds(100));
    EXPECT_EQ(second->ObservationDuration, TDuration::Minutes(1));
    EXPECT_EQ(second->ProcessedCount, 2000);
    EXPECT_EQ(second->ProcessedByteSize, 16000);
}

TEST(TProcessingObservationAccumulatorTest, PublishOnlyEpochAddsTimeWithoutRecountingInputs)
{
    TProcessingObservationAccumulator accumulator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    accumulator.StartEpoch(totals);
    accumulator.AddInputs(1000, 8000);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    auto first = accumulator.Commit(totals, StartTime + TDuration::Seconds(10));

    accumulator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(30);
    auto second = accumulator.Commit(totals, StartTime + TDuration::Seconds(40));
    EXPECT_EQ(second->ProcessedCount, first->ProcessedCount);
    EXPECT_EQ(second->ProcessedByteSize, first->ProcessedByteSize);
    EXPECT_EQ(second->ProcessingTime - first->ProcessingTime, TDuration::Seconds(30));
    EXPECT_EQ(second->ObservationDuration, TDuration::Seconds(30));
}

TEST(TProcessingObservationAccumulatorTest, CountsFirstBatchWithoutWarmupOrDiagnosticEma)
{
    TProcessingObservationAccumulator accumulator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    accumulator.StartEpoch(totals);
    accumulator.AddInputs(1, 64);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::MilliSeconds(1);
    totals[EEpochPartKind::Processing].WallTimeEma = TDuration::Hours(1);
    auto first = accumulator.Commit(totals, StartTime + TDuration::MilliSeconds(1));
    EXPECT_EQ(first->ProcessedCount, 1);
    EXPECT_EQ(first->ProcessedByteSize, 64);
    EXPECT_EQ(first->ProcessingTime, TDuration::MilliSeconds(1));
    EXPECT_EQ(first->ObservationDuration, TDuration::MilliSeconds(1));
}

TEST(TProcessingObservationAccumulatorTest, IdleEpochKeepsWorkAndIncludesActualOverhead)
{
    TProcessingObservationAccumulator accumulator(StartTime);
    THashMap<EEpochPartKind, IComputationTracer::TPartState> totals;
    accumulator.StartEpoch(totals);
    accumulator.AddInputs(100, 800);
    totals[EEpochPartKind::Processing].TotalDuration = TDuration::Seconds(10);
    auto first = accumulator.Commit(totals, StartTime + TDuration::Seconds(10));

    accumulator.StartEpoch(totals);
    totals[EEpochPartKind::Processing].TotalDuration += TDuration::Seconds(1);
    totals[EEpochPartKind::WaitingForInput].TotalDuration += TDuration::Days(10);
    auto idle = accumulator.Commit(totals, StartTime + TDuration::Days(10) + TDuration::Seconds(11));
    EXPECT_EQ(idle->ProcessedCount, first->ProcessedCount);
    EXPECT_EQ(idle->ProcessedByteSize, first->ProcessedByteSize);
    EXPECT_EQ(idle->ProcessingTime, TDuration::Seconds(11));
    EXPECT_EQ(idle->InputWaitingTime, TDuration::Days(10));
    EXPECT_EQ(idle->ObservationDuration, TDuration::Days(10) + TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
