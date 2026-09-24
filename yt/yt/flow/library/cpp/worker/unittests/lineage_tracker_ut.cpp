#include <yt/yt/flow/library/cpp/worker/lineage_tracker.h>

#include <yt/yt/flow/library/cpp/common/job_lineage_tracker.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <array>
#include <cmath>

namespace NYT::NFlow::NWorker {
namespace {

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr MakeComputationSpec()
{
    auto spec = New<TComputationSpec>();
    spec->InputStreamIds.insert(TStreamId("input"));
    spec->OutputStreamIds.insert(TStreamId("output"));
    spec->StreamsDependency[TStreamId("output")] = {TStreamId("input")};
    return spec;
}

TLineageDelta MakeDelta(double inputCount, double ratio = 2)
{
    TLineageDelta delta;
    delta[TStreamId("output")][TStreamId("input")] = {
        .Count = inputCount * ratio,
        .ByteSize = inputCount * ratio * 20,
        .InputCount = inputCount,
        .InputByteSize = inputCount * 10,
    };
    return delta;
}

TLineageRatio GetRatio(const TLineageTrackerPtr& tracker, TInstant now)
{
    return tracker->GetRatios(now).at(TStreamId("output")).at(TStreamId("input"));
}

TEST(TLineageTrackerTest, FirstBatchHasRatioAndWeight)
{
    auto tracker = New<TLineageTracker>();
    auto now = TInstant::Seconds(100);
    EXPECT_TRUE(tracker->GetRatios(now).empty());
    tracker->Add(TComputationId("mapper"), MakeComputationSpec(), MakeDelta(100), now);
    auto value = GetRatio(tracker, now);
    ASSERT_TRUE(value.Count);
    ASSERT_TRUE(value.ByteSize);
    EXPECT_DOUBLE_EQ(value.Count->Ratio, 2);
    EXPECT_DOUBLE_EQ(value.Count->Weight, 100);
    EXPECT_DOUBLE_EQ(value.ByteSize->Ratio, 4);
    EXPECT_DOUBLE_EQ(value.ByteSize->Weight, 1000);
}

TEST(TLineageTrackerTest, EmptyInputIsUnknownAndFilteredOutputIsKnownZero)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    auto now = TInstant::Seconds(100);
    tracker->Add(TComputationId("filter"), spec, {}, now);
    auto unknown = GetRatio(tracker, now);
    EXPECT_FALSE(unknown.Count);
    EXPECT_FALSE(unknown.ByteSize);
    tracker->Add(TComputationId("filter"), spec, MakeDelta(100, 0), now);
    auto zero = GetRatio(tracker, now);
    ASSERT_TRUE(zero.Count);
    ASSERT_TRUE(zero.ByteSize);
    EXPECT_DOUBLE_EQ(zero.Count->Ratio, 0);
    EXPECT_DOUBLE_EQ(zero.Count->Weight, 100);
    EXPECT_DOUBLE_EQ(zero.ByteSize->Ratio, 0);
    EXPECT_DOUBLE_EQ(zero.ByteSize->Weight, 1000);
}

TEST(TLineageTrackerTest, DecaysWeightWithoutChangingRatio)
{
    auto tracker = New<TLineageTracker>();
    auto now = TInstant::Seconds(100);
    tracker->Add(TComputationId("mapper"), MakeComputationSpec(), MakeDelta(100), now);
    auto value = GetRatio(tracker, now + LineageDecayTime);
    ASSERT_TRUE(value.Count);
    ASSERT_TRUE(value.ByteSize);
    EXPECT_DOUBLE_EQ(value.Count->Ratio, 2);
    EXPECT_DOUBLE_EQ(value.ByteSize->Ratio, 4);
    EXPECT_NEAR(value.Count->Weight, 100 * std::exp(-1.0), 1e-12);
    EXPECT_NEAR(value.ByteSize->Weight, 1000 * std::exp(-1.0), 1e-12);
    EXPECT_TRUE(tracker->GetRatios(now + LineageRetentionTime).empty());
    tracker->Add(TComputationId("mapper"), MakeComputationSpec(), MakeDelta(10, 3), now + LineageRetentionTime);
    auto fresh = GetRatio(tracker, now + LineageRetentionTime);
    ASSERT_TRUE(fresh.Count);
    EXPECT_DOUBLE_EQ(fresh.Count->Ratio, 3);
    EXPECT_DOUBLE_EQ(fresh.Count->Weight, 10);
}

TEST(TLineageTrackerTest, AggregatesActualInputWeightsInsteadOfBatchCounts)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    auto now = TInstant::Seconds(100);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100, 2), now);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(300, 0), now);
    auto value = GetRatio(tracker, now);
    ASSERT_TRUE(value.Count);
    ASSERT_TRUE(value.ByteSize);
    EXPECT_DOUBLE_EQ(value.Count->Ratio, 0.5);
    EXPECT_DOUBLE_EQ(value.Count->Weight, 400);
    EXPECT_DOUBLE_EQ(value.ByteSize->Ratio, 1);
    EXPECT_DOUBLE_EQ(value.ByteSize->Weight, 4000);
}

TEST(TLineageTrackerTest, RecentObservationsChangeTheRatio)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    auto now = TInstant::Seconds(100);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100, 2), now);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100, 4), now + LineageDecayTime);
    auto value = GetRatio(tracker, now + LineageDecayTime);
    ASSERT_TRUE(value.Count);
    EXPECT_NEAR(value.Count->Ratio, (2 * std::exp(-1.0) + 4) / (std::exp(-1.0) + 1), 1e-12);
    EXPECT_NEAR(value.Count->Weight, 100 * (std::exp(-1.0) + 1), 1e-12);
}

TEST(TLineageTrackerTest, JobRestartAndReplayDoNotResetSharedObservation)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    double previousWeight = 0;
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto job = CreateJobLineageTracker(tracker, TComputationId("mapper"), spec);
        job->Add(MakeDelta(100));
        job.Reset();
        auto value = GetRatio(tracker, TInstant::Now());
        ASSERT_TRUE(value.Count);
        EXPECT_DOUBLE_EQ(value.Count->Ratio, 2);
        EXPECT_GT(value.Count->Weight, previousWeight);
        previousWeight = value.Count->Weight;
    }
}

TEST(TLineageTrackerTest, ReorderedTimestampsDoNotRewindDecay)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    auto now = TInstant::Seconds(100);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100), now);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100), now + LineageDecayTime);
    tracker->Add(TComputationId("mapper"), spec, MakeDelta(100), now);
    auto value = GetRatio(tracker, now);
    ASSERT_TRUE(value.Count);
    EXPECT_DOUBLE_EQ(value.Count->Ratio, 2);
    EXPECT_NEAR(value.Count->Weight, 200 * std::exp(-1.0) + 100, 1e-12);
}

TEST(TLineageTrackerTest, ObservationDeliveryOrderPreservesRatioAndWeight)
{
    auto spec = MakeComputationSpec();
    auto start = TInstant::Seconds(100);
    std::array<int, 3> order = {0, 1, 2};
    do {
        auto tracker = New<TLineageTracker>();
        for (int index : order) {
            tracker->Add(TComputationId("mapper"), spec, MakeDelta(100, index + 1), start + LineageDecayTime * index);
        }
        auto value = GetRatio(tracker, start + LineageDecayTime * 2);
        ASSERT_TRUE(value.Count);
        ASSERT_TRUE(value.ByteSize);
        auto weight = 100 * (std::exp(-2.0) + std::exp(-1.0) + 1);
        auto ratio = (std::exp(-2.0) + 2 * std::exp(-1.0) + 3) * 100 / weight;
        EXPECT_NEAR(value.Count->Weight, weight, 1e-10);
        EXPECT_NEAR(value.Count->Ratio, ratio, 1e-12);
        EXPECT_NEAR(value.ByteSize->Weight, 10 * weight, 1e-9);
        EXPECT_NEAR(value.ByteSize->Ratio, 2 * ratio, 1e-12);
        EXPECT_TRUE(tracker->GetRatios(start + LineageDecayTime * 2 + LineageRetentionTime).empty());
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(TLineageTrackerTest, ScopesInternalStreamIdsByComputation)
{
    auto tracker = New<TLineageTracker>();
    auto spec = MakeComputationSpec();
    spec->StreamsDependency[TStreamId("output")].insert(TStreamId("source"));
    auto now = TInstant::Seconds(100);
    TLineageDelta delta;
    delta[TStreamId("output")][TStreamId("source")] = {.Count = 10, .InputCount = 20};
    tracker->Add(TComputationId("first"), spec, delta, now);
    tracker->Add(TComputationId("second"), spec, delta, now);
    auto values = tracker->GetRatios(now).at(TStreamId("output"));
    EXPECT_TRUE(values.contains(TStreamId("first/source")));
    EXPECT_TRUE(values.contains(TStreamId("second/source")));
    EXPECT_FALSE(values.contains(TStreamId("source")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
