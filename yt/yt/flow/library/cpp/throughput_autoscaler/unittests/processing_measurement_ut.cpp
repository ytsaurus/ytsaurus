#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_measurement.h>
#include <yt/yt/flow/library/cpp/throughput_autoscaler/processing_observation_window.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <limits>
#include <random>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto StartTime = TInstant::Seconds(10'000);

class TProcessingMeasurementTest
    : public ::testing::Test
{
protected:
    std::array<TJobId, 2> JobIds_{TJobId(TGuid::Create()), TJobId(TGuid::Create())};
    std::array<TProcessingObservationWindow, 2> Windows_{
        TProcessingObservationWindow(JobIds_[0]),
        TProcessingObservationWindow(JobIds_[1]),
    };
    std::array<const TProcessingObservationWindow*, 2> Cohort_{&Windows_[0], &Windows_[1]};

    TJobStatusPtr MakeStatus(int job, int second, int rate = 10, int epochSeconds = 10, int busyMilliseconds = 800)
    {
        auto status = New<TJobStatus>();
        status->JobId = JobIds_[job];
        status->Epoch = 7;
        status->InitedTime = StartTime;
        status->UpdateTime = At(second);
        auto& observation = status->ProcessingObservation;
        observation = New<TProcessingObservation>();
        observation->SpecGeneration = 7;
        observation->Sequence = second + 1;
        observation->CapturedAt = status->UpdateTime;
        observation->ObservationDuration = TDuration::Seconds(epochSeconds);
        observation->ProcessedCount = rate * second;
        observation->ProcessedByteSize = 10 * rate * second;
        observation->ProcessingTime = TDuration::MilliSeconds(busyMilliseconds * second);
        observation->InputWaitingTime = TDuration::MilliSeconds((900 - busyMilliseconds) * second);
        observation->OutputWaitingTime = TDuration::MilliSeconds(100 * second);
        return status;
    }

    static TInstant At(int second)
    {
        return StartTime + TDuration::Seconds(second);
    }

    void Add(int job, const TJobStatusPtr& status)
    {
        ASSERT_EQ(
            Windows_[job].Add(*status, status->UpdateTime),
            EProcessingObservationAddResult::Accepted);
    }

    void Fill(int job, int offset = 0, int last = 180, int step = 10)
    {
        for (int second = offset; second <= last; second += step) {
            Add(job, MakeStatus(job, second, 10 * (job + 1), step));
        }
    }

    auto Measure(int now = 180)
    {
        return MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(now), TDuration::Minutes(10));
    }
};

TEST_F(TProcessingMeasurementTest, MeasuresCommonIntervalUsingBusiestJob)
{
    for (int second = 0; second <= 100; second += 10) {
        Add(0, MakeStatus(/*job*/ 0, second, /*rate*/ 10, /*epochSeconds*/ 10, /*busyMilliseconds*/ 100));
        Add(1, MakeStatus(/*job*/ 1, second + 5, /*rate*/ 20, /*epochSeconds*/ 10, /*busyMilliseconds*/ 800));
    }
    auto result = Measure(105);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Begin, At(5));
    EXPECT_EQ(result->End, At(100));
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_DOUBLE_EQ(*result->Processed, 30);
    EXPECT_DOUBLE_EQ(*result->Capacity, 37.5);
    EXPECT_DOUBLE_EQ(result->InputWaitingShare, 0.45);
    EXPECT_DOUBLE_EQ(result->OutputWaitingShare, 0.1);
}

TEST_F(TProcessingMeasurementTest, EmptyWorkDoesNotEraseBusyTime)
{
    for (int second = 0; second <= 180; second += 10) {
        Add(0, MakeStatus(/*job*/ 0, second, /*rate*/ 0, /*epochSeconds*/ 10, /*busyMilliseconds*/ 800));
        Add(1, MakeStatus(/*job*/ 1, second, /*rate*/ 10, /*epochSeconds*/ 10, /*busyMilliseconds*/ 100));
    }
    auto result = Measure();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_DOUBLE_EQ(*result->Processed, 10);
    EXPECT_DOUBLE_EQ(*result->Capacity, 12.5);
}

TEST_F(TProcessingMeasurementTest, IdleJobDoesNotInvalidateBusyPeer)
{
    for (int second = 0; second <= 180; second += 10) {
        Add(0, MakeStatus(/*job*/ 0, second, /*rate*/ 0, /*epochSeconds*/ 10, /*busyMilliseconds*/ 0));
        Add(1, MakeStatus(/*job*/ 1, second, /*rate*/ 10));
    }
    auto result = Measure();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_DOUBLE_EQ(*result->Processed, 10);
    EXPECT_DOUBLE_EQ(*result->Capacity, 12.5);
}

TEST_F(TProcessingMeasurementTest, BusyPartitionShareUsesClippedTimeAndIncludesIdlePartitions)
{
    std::vector<TProcessingObservationWindow> windows;
    for (int job = 0; job < 4; ++job) {
        auto id = TJobId(TGuid::Create());
        auto& window = windows.emplace_back(id);
        auto add = [&] (int second, int busyMilliseconds) {
            auto status = MakeStatus(/*job*/ 0, second, /*rate*/ 10, /*epochSeconds*/ 10, busyMilliseconds);
            status->JobId = id;
            ASSERT_EQ(window.Add(*status, At(second)), EProcessingObservationAddResult::Accepted);
        };
        if (job == 0) {
            add(0, 900);
            add(100, 900);
            add(200, 750);
        } else {
            add(50, job == 1 ? 749 : 0);
            add(150, job == 1 ? 749 : 0);
        }
    }
    std::vector<const TProcessingObservationWindow*> cohort;
    for (const auto& window : windows) {
        cohort.push_back(&window);
    }
    auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), cohort, At(200), TDuration::Seconds(100));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Begin, At(50));
    EXPECT_EQ(result->End, At(150));
    EXPECT_DOUBLE_EQ(result->BusyPartitionShare, 0.25);
    result = MeasureThroughputProcessing(New<TThroughputSpec>(), cohort, At(200), TDuration::Seconds(50));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Begin, At(100));
    EXPECT_DOUBLE_EQ(result->BusyPartitionShare, 0);
}

TEST_F(TProcessingMeasurementTest, CapacityTracksTheBusiestJobAsItChanges)
{
    const std::array<std::array<int, 3>, 3> busySeconds{{{8, 1, 1}, {2, 7, 1}, {1, 1, 6}}};
    std::vector<TProcessingObservationWindow> windows;
    for (const auto& jobBusySeconds : busySeconds) {
        auto jobId = TJobId(TGuid::Create());
        auto& window = windows.emplace_back(jobId);
        int processingSeconds = 0;
        for (int step = 0; step <= 3; ++step) {
            if (step > 0) {
                processingSeconds += jobBusySeconds[step - 1];
            }
            auto status = MakeStatus(/*job*/ 0, 10 * step);
            status->JobId = jobId;
            status->ProcessingObservation->ProcessingTime = TDuration::Seconds(processingSeconds);
            status->ProcessingObservation->InputWaitingTime = TDuration::Seconds(9 * step - processingSeconds);
            ASSERT_EQ(window.Add(*status, status->UpdateTime), EProcessingObservationAddResult::Accepted);
        }
    }
    std::vector<const TProcessingObservationWindow*> cohort;
    for (const auto& window : windows) {
        cohort.push_back(&window);
    }
    auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), cohort, At(30), TDuration::Minutes(3));
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_DOUBLE_EQ(*result->Processed, 30);
    EXPECT_DOUBLE_EQ(*result->Capacity, 30 / 0.7);
}

TEST_F(TProcessingMeasurementTest, IdleCohortDoesNotInventCapacity)
{
    for (int second = 0; second <= 180; second += 10) {
        Add(0, MakeStatus(/*job*/ 0, second, /*rate*/ 0, /*epochSeconds*/ 10, /*busyMilliseconds*/ 0));
        Add(1, MakeStatus(/*job*/ 1, second, /*rate*/ 0, /*epochSeconds*/ 10, /*busyMilliseconds*/ 0));
    }
    auto result = Measure();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    EXPECT_DOUBLE_EQ(*result->Processed, 0);
    EXPECT_FALSE(result->Capacity);
}

TEST_F(TProcessingMeasurementTest, MissingMemberCannotBecomeZero)
{
    Fill(/*job*/ 0);
    EXPECT_FALSE(Measure());
    Fill(/*job*/ 1, /*offset*/ 70);
    EXPECT_TRUE(Measure());
    Cohort_[1] = nullptr;
    EXPECT_FALSE(Measure());
    EXPECT_FALSE(MeasureThroughputProcessing(New<TThroughputSpec>(), {}, At(180), TDuration::Minutes(10)));
}

TEST_F(TProcessingMeasurementTest, HeartbeatSilenceDoesNotExpireTheCommittedWindow)
{
    Fill(/*job*/ 0);
    Fill(/*job*/ 1);
    auto status = MakeStatus(/*job*/ 0, /*second*/ 180);
    status->UpdateTime = At(300);
    EXPECT_EQ(Windows_[0].Add(*status, At(300)), EProcessingObservationAddResult::Repeated);
    auto result = Measure(300);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->End, At(180));
    ASSERT_TRUE(Measure(3600));
    EXPECT_EQ(Measure(3600)->End, result->End);
    EXPECT_EQ(Measure(3600)->Capacity, result->Capacity);
}

TEST_F(TProcessingMeasurementTest, MissingProcessingTimeDoesNotInventCapacity)
{
    for (int job = 0; job < 2; ++job) {
        for (int second = 0; second <= 180; second += 10) {
            auto status = MakeStatus(job, second);
            status->ProcessingObservation->ProcessingTime = TDuration::Zero();
            status->ProcessingObservation->InputWaitingTime = TDuration::Seconds(second);
            status->ProcessingObservation->OutputWaitingTime = TDuration::Zero();
            Add(job, status);
        }
    }
    auto result = Measure();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    EXPECT_DOUBLE_EQ(*result->Processed, 20);
    EXPECT_FALSE(result->Capacity);
}

TEST_F(TProcessingMeasurementTest, AbsentTimingDoesNotProduceCapacity)
{
    for (int job = 0; job < 2; ++job) {
        for (int second = 0; second <= 180; second += 10) {
            auto status = MakeStatus(job, second, /*rate*/ 0);
            status->ProcessingObservation->ProcessingTime = TDuration::Zero();
            status->ProcessingObservation->InputWaitingTime = TDuration::Zero();
            status->ProcessingObservation->OutputWaitingTime = TDuration::Zero();
            Add(job, status);
        }
    }
    auto result = Measure();
    EXPECT_FALSE(result);
}

TEST_F(TProcessingMeasurementTest, IndependentReportSchedulesKeepRatesAndWaitShares)
{
    std::mt19937 generator(775);
    for (int run = 0; run < 50; ++run) {
        for (int job = 0; job < 2; ++job) {
            Windows_[job] = TProcessingObservationWindow(JobIds_[job]);
            for (int second = 0; second <= 360; ++second) {
                if (second == 0 || second == 360 || generator() % 7 == 0) {
                    Add(job, MakeStatus(job, second, 10 * (job + 1), 7));
                }
            }
        }
        auto result = Measure(360);
        ASSERT_TRUE(result);
        ASSERT_TRUE(result->Processed);
        EXPECT_NEAR(*result->Processed, 30, 1e-9);
        ASSERT_TRUE(result->Capacity);
        EXPECT_NEAR(*result->Capacity, 37.5, 1e-9);
        EXPECT_NEAR(result->InputWaitingShare, 0.1, 1e-12);
        EXPECT_NEAR(result->OutputWaitingShare, 0.1, 1e-12);
    }
}

TEST_F(TProcessingMeasurementTest, HeartbeatsKeepStaggeredLongEpochsMeasurable)
{
    std::vector<TProcessingObservationWindow> windows;
    std::vector<TJobStatusPtr> statuses;
    for (int job = 0; job < 4; ++job) {
        auto id = TJobId(TGuid::Create());
        windows.emplace_back(id);
        auto status = MakeStatus(0, 45 * (job + 1), 1, 180);
        status->JobId = id;
        statuses.push_back(std::move(status));
    }
    std::vector<const TProcessingObservationWindow*> cohort;
    for (const auto& window : windows) {
        cohort.push_back(&window);
    }
    int measurements = 0;
    for (int second = 0; second <= 1800; second += 15) {
        auto commonEnd = At(second);
        for (int job = 0; job < 4; ++job) {
            int first = 45 * (job + 1);
            if (second < first) {
                continue;
            }
            if ((second - first) % 180 == 0) {
                auto status = MakeStatus(/*job*/ 0, second, /*rate*/ 1, /*epochSeconds*/ 180);
                status->JobId = statuses[job]->JobId;
                statuses[job] = std::move(status);
            }
            auto& status = statuses[job];
            status->UpdateTime = At(second);
            ASSERT_NE(windows[job].Add(*status, At(second)), EProcessingObservationAddResult::Rejected);
            commonEnd = std::min(commonEnd, status->ProcessingObservation->CapturedAt);
        }
        if (second < 1080) {
            continue;
        }
        auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), cohort, At(second), TDuration::Minutes(10));
        ASSERT_TRUE(result) << second;
        ++measurements;
        EXPECT_EQ(result->End, commonEnd);
        EXPECT_EQ(result->Begin, commonEnd - TDuration::Minutes(10));
    }
    EXPECT_EQ(measurements, 49);
}

TEST_F(TProcessingMeasurementTest, AlternatingBusiestJobsUseMeanOfMaximumNotMaximumOfMeans)
{
    auto add = [&] (int job, int second, i64 count, int processing) {
        auto status = MakeStatus(job, second);
        auto& observation = status->ProcessingObservation;
        observation->ProcessedCount = count;
        observation->ProcessingTime = TDuration::Seconds(processing);
        observation->InputWaitingTime = TDuration::Seconds(second - processing);
        observation->OutputWaitingTime = TDuration::Zero();
        Add(job, status);
    };
    add(0, 0, 0, 0);
    add(1, 0, 0, 0);
    add(0, 100, 10'000, 100);
    add(1, 100, 0, 0);
    add(0, 200, 10'000, 100);
    add(1, 200, 10'000, 100);
    auto result = Measure(200);
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_DOUBLE_EQ(*result->Processed, 100);
    EXPECT_DOUBLE_EQ(*result->Capacity, 100);
}

TEST_F(TProcessingMeasurementTest, WindowCropsAsynchronousReportsAndIncludesIdleIntervals)
{
    auto add = [&] (int job, int second, i64 count, int processing) {
        auto status = MakeStatus(job, second);
        auto& observation = status->ProcessingObservation;
        observation->ProcessedCount = count;
        observation->ProcessingTime = TDuration::Seconds(processing);
        observation->InputWaitingTime = TDuration::Seconds(second - processing);
        observation->OutputWaitingTime = TDuration::Zero();
        Add(job, status);
    };
    add(0, 0, 0, 0);
    add(0, 100, 10'000, 100);
    add(0, 200, 10'000, 100);
    add(1, 50, 0, 0);
    add(1, 150, 0, 0);
    add(1, 250, 0, 0);
    auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(250), TDuration::Seconds(120));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Begin, At(80));
    EXPECT_EQ(result->End, At(200));
    ASSERT_TRUE(result->Processed);
    ASSERT_TRUE(result->Capacity);
    EXPECT_NEAR(*result->Processed, 2000.0 / 120, 1e-9);
    EXPECT_DOUBLE_EQ(*result->Capacity, 100);
}

TEST_F(TProcessingMeasurementTest, LongBatchesEarnCreditOverWholeReceivedIntervals)
{
    Fill(/*job*/ 0, /*offset*/ 0, /*last*/ 600, /*step*/ 600);
    Fill(/*job*/ 1, /*offset*/ 0, /*last*/ 900, /*step*/ 900);
    auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(900), TDuration::Minutes(3), StartTime);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Processed, 30);
    EXPECT_EQ(result->Begin, At(420));
    EXPECT_EQ(result->End, At(600));
    ASSERT_TRUE(result->ProcessedCredit);
    EXPECT_DOUBLE_EQ(*result->ProcessedCredit, 24'000.0 / 900);
    EXPECT_EQ(result->ProcessedCreditBegin, At(0));
    EXPECT_EQ(result->ProcessedCreditEnd, At(900));
}

TEST_F(TProcessingMeasurementTest, LongSmallBatchesDoNotDiluteCompletedFastWorkCredit)
{
    for (int slowCount : {0, 1}) {
        SCOPED_TRACE(slowCount);
        for (int job = 0; job < 2; ++job) {
            Windows_[job] = TProcessingObservationWindow(JobIds_[job]);
            Add(job, MakeStatus(job, /*second*/ 0, /*rate*/ 0));
        }
        Add(0, MakeStatus(/*job*/ 0, /*second*/ 3420, /*rate*/ 0));
        auto fast = MakeStatus(/*job*/ 0, /*second*/ 3600, /*rate*/ 0);
        fast->ProcessingObservation->ProcessedCount = 72'000;
        Add(0, fast);
        auto slow = MakeStatus(/*job*/ 1, /*second*/ 3600, /*rate*/ 0);
        slow->ProcessingObservation->ProcessedCount = slowCount;
        Add(1, slow);
        auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(3600), TDuration::Minutes(3), StartTime);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->ProcessedCredit, 400);
        EXPECT_EQ(result->ProcessedCreditBegin, At(3420));
        EXPECT_EQ(result->ProcessedCreditEnd, At(3600));
    }
}

TEST_F(TProcessingMeasurementTest, CreditExcludesWholeIntervalsCrossingTheCurrentLayoutBoundary)
{
    Fill(/*job*/ 0, /*offset*/ 0, /*last*/ 180, /*step*/ 60);
    Fill(/*job*/ 1, /*offset*/ 0, /*last*/ 180, /*step*/ 60);
    auto result = MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(180), TDuration::Minutes(3), At(90));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Processed, 30);
    EXPECT_EQ(result->ProcessedCredit, 10);
    result = MeasureThroughputProcessing(New<TThroughputSpec>(), Cohort_, At(180), TDuration::Minutes(3), At(150));
    ASSERT_TRUE(result);
    EXPECT_EQ(result->Processed, 30);
    EXPECT_EQ(result->ProcessedCredit, 0);
}

TEST_F(TProcessingMeasurementTest, BatchMeanUsesAllNonemptyEpochsAcrossJobsAndSkippedReports)
{
    auto add = [&] (int job, int second, i64 epochs, int duration) {
        auto status = MakeStatus(job, second, /*rate*/ 10, /*epochSeconds*/ 1);
        status->ProcessingObservation->NonemptyEpochCount = epochs;
        status->ProcessingObservation->NonemptyEpochDuration = TDuration::Seconds(duration);
        Add(job, status);
    };
    add(0, 0, 0, 0);
    add(1, 0, 0, 0);
    add(0, 10, 2, 10);
    add(0, 20, 4, 20);
    add(1, 540, 1, 540);
    // Reporting only the last of many empty epochs must preserve the long batch.
    add(0, 600, 4, 20);
    add(1, 600, 1, 540);
    EXPECT_EQ(ComputeThroughputBatchDuration(Cohort_), TDuration::Seconds(112));

    auto legacy = MakeStatus(/*job*/ 1, /*second*/ 610, /*rate*/ 10, /*epochSeconds*/ 7);
    Add(1, legacy);
    EXPECT_EQ(ComputeThroughputBatchDuration(Cohort_), TDuration::Seconds(7));
}

TEST_F(TProcessingMeasurementTest, OldNonemptyEpochsLeaveTheBoundedHistory)
{
    auto add = [&] (int second, i64 epochs, int duration) {
        for (int job = 0; job < 2; ++job) {
            auto status = MakeStatus(job, second, /*rate*/ 10, /*epochSeconds*/ 1);
            status->ProcessingObservation->NonemptyEpochCount = epochs;
            status->ProcessingObservation->NonemptyEpochDuration = TDuration::Seconds(duration);
            ASSERT_EQ(
                Windows_[job].Add(*status, At(second), TDuration::Minutes(10)),
                EProcessingObservationAddResult::Accepted);
        }
    };
    add(0, 0, 0);
    add(540, 1, 540);
    add(600, 3, 560);
    EXPECT_EQ(ComputeThroughputBatchDuration(Cohort_), TDuration::MicroSeconds(560'000'000 / 3));
    add(1140, 3, 560);
    EXPECT_EQ(ComputeThroughputBatchDuration(Cohort_), TDuration::Seconds(10));
    add(1740, 3, 560);
    EXPECT_EQ(ComputeThroughputBatchDuration(Cohort_), TDuration::Zero());
}

TEST_F(TProcessingMeasurementTest, BatchMeanRecoversAfterIncompleteIntervalsExpire)
{
    auto add = [&] (int second, i64 sequence, std::optional<i64> epochs, int duration) {
        auto status = MakeStatus(/*job*/ 0, second);
        status->ProcessingObservation->Sequence = sequence;
        status->ProcessingObservation->NonemptyEpochCount = epochs;
        status->ProcessingObservation->NonemptyEpochDuration = epochs
            ? std::optional(TDuration::Seconds(duration))
            : std::nullopt;
        ASSERT_EQ(
            Windows_[0].Add(*status, At(second), TDuration::Seconds(20)),
            EProcessingObservationAddResult::Accepted);
    };
    const std::array<const TProcessingObservationWindow*, 1> cohort{&Windows_[0]};
    add(0, 1, 0, 0);
    add(10, 2, 1, 4);
    add(10, 3, 2, 6);
    EXPECT_EQ(ComputeThroughputBatchDuration(cohort), TDuration::Seconds(3));
    add(20, 4, std::nullopt, 0);
    add(30, 5, 5, 30);
    add(40, 6, 6, 38);
    EXPECT_EQ(ComputeThroughputBatchDuration(cohort), TDuration::Seconds(10));
    add(50, 7, 8, 42);
    EXPECT_EQ(ComputeThroughputBatchDuration(cohort), TDuration::Seconds(4));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
