#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_observation_window.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto StartTime = TInstant::Seconds(1000);
using EAddResult = EProcessingObservationAddResult;

class TProcessingObservationWindowTest
    : public ::testing::Test
{
protected:
    TJobId JobId_ = TJobId(TGuid::Create());
    TProcessingObservationWindow Window_{JobId_};

    TJobStatusPtr MakeStatus(int seconds, i64 sequence, i64 count)
    {
        auto result = New<TJobStatus>();
        result->JobId = JobId_;
        result->Epoch = 7;
        result->InitedTime = StartTime;
        result->UpdateTime = StartTime + TDuration::Seconds(seconds);
        auto& observation = result->ProcessingObservation;
        observation = New<TProcessingObservation>();
        observation->SpecGeneration = 7;
        observation->Sequence = sequence;
        observation->CapturedAt = result->UpdateTime;
        observation->ProcessedCount = count;
        observation->ProcessedByteSize = count * 10;
        observation->ProcessingTime = TDuration::MilliSeconds(count);
        observation->InputWaitingTime = TDuration::MilliSeconds(2 * count);
        observation->OutputWaitingTime = TDuration::MilliSeconds(3 * count);
        observation->OtherWaitingTime = TDuration::MilliSeconds(4 * count);
        return result;
    }

    void Add(int seconds, i64 sequence, i64 count)
    {
        auto status = MakeStatus(seconds, sequence, count);
        ASSERT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
    }
};

TEST_F(TProcessingObservationWindowTest, RejectsMissingFailedAndFutureObservations)
{
    auto status = MakeStatus(10, 1, 100);
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    status->UpdateTime = StartTime;
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    status->ProcessingObservation->CapturedAt = StartTime;
    status->Error = TError("Failed epoch");
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    status->Error = TError();
    status->IsFinished = true;
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    status->IsFinished = false;
    status->RetryableErrors.emplace("source", TError("Retrying input"));
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    status->RetryableErrors.clear();
    status->ProcessingObservation.Reset();
    EXPECT_EQ(Window_.Add(*status, StartTime), EProcessingObservationAddResult::Rejected);
    EXPECT_FALSE(Window_.GetCoverage());
}

TEST_F(TProcessingObservationWindowTest, DelayedReportDoesNotReplaceNewerCounters)
{
    Add(10, 2, 100);
    auto delayed = MakeStatus(0, 1, 0);
    EXPECT_EQ(Window_.Add(*delayed, StartTime + TDuration::Seconds(20)), EProcessingObservationAddResult::Rejected);
    Add(20, 3, 200);
    ASSERT_FALSE(Window_.GetIntervals().empty());
    EXPECT_EQ(Window_.GetIntervals().back().Statistics.ProcessedCount, 100);
}

TEST_F(TProcessingObservationWindowTest, AccumulatesDistinctPublicationsAtTheSameTimestamp)
{
    Add(0, 1, 0);
    Add(10, 2, 100);
    Add(10, 3, 150);
    Add(20, 4, 250);
    ASSERT_EQ(Window_.GetIntervals().size(), 2u);
    EXPECT_EQ(Window_.GetIntervals().front().Statistics.ProcessedCount, 150);
    ASSERT_FALSE(Window_.GetIntervals().empty());
    EXPECT_EQ(Window_.GetIntervals().back().Statistics.ProcessedCount, 100);
}

TEST_F(TProcessingObservationWindowTest, DuplicateReportsDoNotCreateIntervals)
{
    Add(0, 1, 100);
    EXPECT_TRUE(Window_.GetIntervals().empty());
    auto status = MakeStatus(10, 2, 200);
    ASSERT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
    ASSERT_EQ(Window_.GetIntervals().size(), 1u);
    EXPECT_EQ(Window_.GetIntervals().front().Statistics.ProcessedCount, 100);
    status->UpdateTime += TDuration::Seconds(30);
    EXPECT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Repeated);
    EXPECT_EQ(Window_.GetIntervals().size(), 1u);
    EXPECT_EQ(Window_.GetCoverage()->second, StartTime + TDuration::Seconds(10));
}

TEST_F(TProcessingObservationWindowTest, SkippedReportsUseCumulativeDeltasRatherThanLatestEpochDuration)
{
    Add(0, 1, 0);
    auto status = MakeStatus(60, 7, 6000);
    status->ProcessingObservation->ObservationDuration = TDuration::Seconds(10);
    ASSERT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
    ASSERT_EQ(Window_.GetIntervals().size(), 1u);
    const auto& interval = Window_.GetIntervals().front();
    EXPECT_EQ(interval.End - interval.Begin, TDuration::Seconds(60));
    EXPECT_EQ(interval.Statistics.ProcessedCount, 6000);
    EXPECT_EQ(interval.Statistics.ProcessingTime, TDuration::Seconds(6));
}

TEST_F(TProcessingObservationWindowTest, RetentionKeepsTheBoundaryIntervalWithoutExponentialWeights)
{
    Add(0, 1, 0);
    Add(10, 2, 100);
    auto status = MakeStatus(70, 3, 700);
    ASSERT_EQ(Window_.Add(*status, status->UpdateTime, TDuration::Seconds(60)), EProcessingObservationAddResult::Accepted);
    ASSERT_EQ(Window_.GetIntervals().size(), 1u);
    EXPECT_EQ(Window_.GetCoverage()->first, StartTime + TDuration::Seconds(10));
    const auto& statistics = Window_.GetIntervals().front().Statistics;
    EXPECT_EQ(statistics.ProcessedCount, 600);
    EXPECT_EQ(statistics.ProcessingTime, TDuration::MilliSeconds(600));
    EXPECT_EQ(statistics.InputWaitingTime, TDuration::MilliSeconds(1200));
    EXPECT_EQ(statistics.OutputWaitingTime, TDuration::MilliSeconds(1800));
}

TEST_F(TProcessingObservationWindowTest, LongEpochAfterEmptyEpochPreservesHistoryWithoutHeartbeats)
{
    auto empty = MakeStatus(1, 1, 0);
    empty->ProcessingObservation->ObservationDuration = TDuration::MilliSeconds(100);
    ASSERT_EQ(Window_.Add(*empty, empty->UpdateTime), EProcessingObservationAddResult::Accepted);
    EXPECT_EQ(Window_.Add(*empty, StartTime + TDuration::Minutes(4)), EProcessingObservationAddResult::Repeated);
    auto full = MakeStatus(241, 2, 24'000);
    full->ProcessingObservation->ObservationDuration = TDuration::Minutes(4);
    ASSERT_EQ(Window_.Add(*full, full->UpdateTime), EProcessingObservationAddResult::Accepted);
    ASSERT_EQ(Window_.GetIntervals().size(), 1u);
    EXPECT_EQ(Window_.GetCoverage()->first, empty->ProcessingObservation->CapturedAt);
    EXPECT_EQ(Window_.GetIntervals().front().Statistics.ProcessedCount, 24'000);
}

TEST_F(TProcessingObservationWindowTest, SpecPublicationDoesNotChangeTheCounterSeries)
{
    Add(0, 1, 0);
    Add(60, 2, 600);
    auto status = MakeStatus(60, 2, 600);
    status->Epoch = 8;
    status->UpdateTime = StartTime + TDuration::Minutes(5);
    EXPECT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Repeated);
    EXPECT_EQ(Window_.GetCoverage()->second, StartTime + TDuration::Seconds(60));
    status = MakeStatus(301, 3, 1000);
    status->Epoch = status->ProcessingObservation->SpecGeneration = 8;
    EXPECT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
    EXPECT_EQ(Window_.GetIntervals().back().Statistics.ProcessedCount, 400);
    status = MakeStatus(310, 4, 100);
    EXPECT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Rejected);
    status = MakeStatus(320, 5, 2000);
    status->JobId = TJobId(TGuid::Create());
    EXPECT_EQ(Window_.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Rejected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
