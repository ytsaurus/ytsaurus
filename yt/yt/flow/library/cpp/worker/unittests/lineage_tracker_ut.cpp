#include <yt/yt/flow/library/cpp/worker/lineage_tracker.h>

#include <yt/yt/flow/library/cpp/common/job_lineage_tracker.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NWorker {
namespace {

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr MakeComputationSpec(
    const TStreamId& inputStreamId,
    const TStreamId& outputStreamId)
{
    auto spec = New<TComputationSpec>();
    spec->InputStreamIds.insert(inputStreamId);
    spec->OutputStreamIds.insert(outputStreamId);
    spec->StreamsDependency[outputStreamId] = {inputStreamId};
    return spec;
}

TLineageDeltaValue MakeDelta(double count, double byteSize)
{
    return {
        .Count = count,
        .ByteSize = byteSize,
    };
}

TEST(TLineageTrackerTest, PairedConversionSurvivesLoadChangesAndIdleDecay)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto spec = MakeComputationSpec(input, output);
    const auto start = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();
    tracker->Add(computationId, spec, {}, start);
    for (int minute = 1; minute <= 60; ++minute) {
        const double inputCount = minute <= 20 || minute > 40 ? 300'000 : 30'000;
        TLineageDelta delta;
        delta[output][input] = {
            .Count = 2 * inputCount,
            .ByteSize = 30 * inputCount,
            .InputCount = inputCount,
            .InputByteSize = 10 * inputCount,
        };
        const auto now = start + TDuration::Minutes(minute);
        tracker->Add(computationId, spec, delta, now);
        if (now >= start + LineageRateDecayTime) {
            const auto rate = tracker->GetRates(now).at(output).at(input);
            ASSERT_TRUE(rate.CountPerSecond);
            ASSERT_TRUE(rate.InputCountPerSecond);
            ASSERT_TRUE(rate.BytesPerSecond);
            ASSERT_TRUE(rate.InputBytesPerSecond);
            EXPECT_NEAR(*rate.CountPerSecond / *rate.InputCountPerSecond, 2, 1e-12);
            EXPECT_NEAR(*rate.BytesPerSecond / *rate.InputBytesPerSecond, 3, 1e-12);
        }
    }
    const auto idleRate = tracker->GetRates(start + TDuration::Minutes(65)).at(output).at(input);
    EXPECT_NEAR(*idleRate.CountPerSecond / *idleRate.InputCountPerSecond, 2, 1e-12);
    EXPECT_NEAR(*idleRate.BytesPerSecond / *idleRate.InputBytesPerSecond, 3, 1e-12);
}

TEST(TLineageTrackerTest, RecordsPairedJobDeltasImmediately)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto spec = MakeComputationSpec(input, output);
    auto tracker = New<TLineageTracker>();
    tracker->Add(computationId, spec, {}, TInstant::Now() - LineageRateDecayTime - TDuration::Seconds(1));
    auto job = CreateJobLineageTracker(tracker, computationId, spec);
    for (int index = 1; index <= 2; ++index) {
        TLineageDelta delta;
        delta[output][input] = {.Count = 100.0 * index, .ByteSize = 400.0 * index, .InputCount = 50.0 * index, .InputByteSize = 100.0 * index};
        job->Add(std::move(delta));
        const auto rate = tracker->GetRates(TInstant::Now()).at(output).at(input);
        ASSERT_TRUE(rate.CountPerSecond);
        ASSERT_TRUE(rate.BytesPerSecond);
        ASSERT_TRUE(rate.InputCountPerSecond);
        ASSERT_TRUE(rate.InputBytesPerSecond);
        ASSERT_GT(*rate.InputCountPerSecond, 0);
        ASSERT_GT(*rate.InputBytesPerSecond, 0);
        EXPECT_NEAR(*rate.CountPerSecond / *rate.InputCountPerSecond, 2, 1e-12);
        EXPECT_NEAR(*rate.BytesPerSecond / *rate.InputBytesPerSecond, 4, 1e-12);
    }
}

TEST(TLineageTrackerTest, AggregatesObservationsAcrossJobs)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const TStreamId timer("timer");
    const TStreamId globalTimer("mapper/timer");
    const auto computationSpec = MakeComputationSpec(input, output);
    computationSpec->StreamsDependency[timer] = {timer};
    const auto startTime = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();

    TLineageDelta firstDelta;
    firstDelta[output][input] = MakeDelta(600, 6000);
    firstDelta[timer][timer] = MakeDelta(60, 600);
    tracker->Add(computationId, computationSpec, firstDelta, startTime);

    TLineageDelta secondDelta;
    secondDelta[output][input] = MakeDelta(600, 6000);
    secondDelta[timer][timer] = MakeDelta(60, 600);
    tracker->Add(computationId, computationSpec, secondDelta, startTime + TDuration::Minutes(1));

    const auto youngRates = tracker->GetRates(startTime + LineageRateDecayTime - TDuration::Seconds(1));
    EXPECT_FALSE(youngRates.at(output).at(input).CountPerSecond);
    EXPECT_FALSE(youngRates.at(output).at(input).BytesPerSecond);
    EXPECT_FALSE(youngRates.at(globalTimer).at(globalTimer).CountPerSecond);
    EXPECT_FALSE(youngRates.at(globalTimer).at(globalTimer).BytesPerSecond);

    const auto matureRates = tracker->GetRates(startTime + LineageRateDecayTime);
    ASSERT_TRUE(matureRates.at(output).at(input).CountPerSecond);
    ASSERT_TRUE(matureRates.at(output).at(input).BytesPerSecond);
    ASSERT_TRUE(matureRates.at(globalTimer).at(globalTimer).CountPerSecond);
    ASSERT_TRUE(matureRates.at(globalTimer).at(globalTimer).BytesPerSecond);
    EXPECT_GT(*matureRates.at(output).at(input).CountPerSecond, 0);
    EXPECT_GT(*matureRates.at(output).at(input).BytesPerSecond, 0);
    EXPECT_GT(*matureRates.at(globalTimer).at(globalTimer).CountPerSecond, 0);
    EXPECT_GT(*matureRates.at(globalTimer).at(globalTimer).BytesPerSecond, 0);
}

TEST(TLineageTrackerTest, KeepsObservationsAfterJobDestructionAndReplay)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto spec = MakeComputationSpec(input, output);
    auto tracker = New<TLineageTracker>();
    tracker->Add(computationId, spec, {}, TInstant::Now() - LineageRateDecayTime - TDuration::Seconds(1));

    double previousRate = 0;
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto job = CreateJobLineageTracker(tracker, computationId, spec);
        TLineageDelta delta;
        delta[output][input] = {.Count = 100, .ByteSize = 400, .InputCount = 50, .InputByteSize = 100};
        job->Add(std::move(delta));
        job.Reset();

        const auto rate = tracker->GetRates(TInstant::Now()).at(output).at(input);
        ASSERT_TRUE(rate.CountPerSecond);
        ASSERT_TRUE(rate.InputCountPerSecond);
        EXPECT_GT(*rate.CountPerSecond, previousRate);
        EXPECT_NEAR(*rate.CountPerSecond / *rate.InputCountPerSecond, 2, 1e-12);
        previousRate = *rate.CountPerSecond;
    }
}

TEST(TLineageTrackerTest, DecaysIdleEdgesAndEventuallyDropsThem)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto computationSpec = MakeComputationSpec(input, output);
    const auto startTime = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();

    TLineageDelta delta;
    delta[output][input] = MakeDelta(600, 6000);
    tracker->Add(computationId, computationSpec, delta, startTime);
    tracker->Add(computationId, computationSpec, delta, startTime + TDuration::Minutes(1));

    const auto matureRate = tracker->GetRates(startTime + LineageRateDecayTime).at(output).at(input);
    const auto decayedRate = tracker->GetRates(startTime + 2 * LineageRateDecayTime).at(output).at(input);
    ASSERT_TRUE(matureRate.CountPerSecond);
    ASSERT_TRUE(matureRate.BytesPerSecond);
    ASSERT_TRUE(decayedRate.CountPerSecond);
    ASSERT_TRUE(decayedRate.BytesPerSecond);
    EXPECT_LT(*decayedRate.CountPerSecond, *matureRate.CountPerSecond);
    EXPECT_LT(*decayedRate.BytesPerSecond, *matureRate.BytesPerSecond);

    EXPECT_TRUE(tracker->GetRates(startTime + TDuration::Minutes(1) + LineageRateRetentionTime).empty());
}

TEST(TLineageTrackerTest, ReportsMatureZeroForDeclaredEdgeWithoutOutput)
{
    const TComputationId computationId("filter");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto computationSpec = MakeComputationSpec(input, output);
    computationSpec->StreamsDependency[output] = {input};
    const auto startTime = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();

    tracker->Add(computationId, computationSpec, {}, startTime);
    TLineageDelta delta;
    delta[output][input] = {.InputCount = 600, .InputByteSize = 6000};
    tracker->Add(computationId, computationSpec, delta, startTime + TDuration::Minutes(1));

    const auto youngRate = tracker->GetRates(startTime + LineageRateDecayTime - TDuration::Seconds(1)).at(output).at(input);
    EXPECT_FALSE(youngRate.CountPerSecond);
    EXPECT_FALSE(youngRate.BytesPerSecond);

    const auto matureRate = tracker->GetRates(startTime + LineageRateDecayTime).at(output).at(input);
    ASSERT_TRUE(matureRate.CountPerSecond);
    ASSERT_TRUE(matureRate.BytesPerSecond);
    EXPECT_DOUBLE_EQ(*matureRate.CountPerSecond, 0);
    EXPECT_DOUBLE_EQ(*matureRate.BytesPerSecond, 0);
    ASSERT_TRUE(matureRate.InputCountPerSecond);
    ASSERT_TRUE(matureRate.InputBytesPerSecond);
    EXPECT_GT(*matureRate.InputCountPerSecond, 0);
    EXPECT_GT(*matureRate.InputBytesPerSecond, 0);
}

TEST(TLineageTrackerTest, KeepsObservationTimestampsMonotonic)
{
    const TComputationId computationId("mapper");
    const TStreamId input("input");
    const TStreamId output("output");
    const auto computationSpec = MakeComputationSpec(input, output);
    const auto startTime = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();

    TLineageDelta delta;
    delta[output][input] = MakeDelta(600, 6000);
    tracker->Add(computationId, computationSpec, delta, startTime);
    tracker->Add(computationId, computationSpec, delta, startTime + TDuration::Minutes(1));
    tracker->Add(computationId, computationSpec, delta, startTime + TDuration::Seconds(30));

    const auto rates = tracker->GetRates(
        startTime + TDuration::Seconds(30) + LineageRateRetentionTime);
    EXPECT_FALSE(rates.empty());
}

TEST(TLineageTrackerTest, ScopesInternalStreamIdsByComputation)
{
    const TStreamId input("input");
    const TStreamId outputA("output_a");
    const TStreamId outputB("output_b");
    const TStreamId source("source");
    const auto computationSpecA = MakeComputationSpec(input, outputA);
    const auto computationSpecB = MakeComputationSpec(input, outputB);
    computationSpecA->StreamsDependency[outputA].insert(source);
    computationSpecB->StreamsDependency[outputB].insert(source);
    const auto startTime = TInstant::Seconds(100);
    auto tracker = New<TLineageTracker>();

    TLineageDelta deltaA;
    deltaA[outputA][source] = MakeDelta(600, 6000);
    tracker->Add(TComputationId("reader_a"), computationSpecA, deltaA, startTime);

    TLineageDelta deltaB;
    deltaB[outputB][source] = MakeDelta(600, 6000);
    tracker->Add(
        TComputationId("reader_b"),
        computationSpecB,
        deltaB,
        startTime + TDuration::Minutes(1));

    const auto rates = tracker->GetRates(startTime + LineageRateDecayTime);
    EXPECT_TRUE(rates.at(outputA).contains(TStreamId("reader_a/source")));
    EXPECT_TRUE(rates.at(outputB).contains(TStreamId("reader_b/source")));
    EXPECT_FALSE(rates.at(outputA).contains(source));
    EXPECT_FALSE(rates.at(outputB).contains(source));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
