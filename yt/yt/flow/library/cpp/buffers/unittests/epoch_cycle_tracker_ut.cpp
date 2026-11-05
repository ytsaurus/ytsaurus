#include <yt/yt/flow/library/cpp/buffers/epoch_cycle_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TEpochCycleTracker, EmptyReturnsNothing)
{
    auto tracker = New<TEpochCycleTracker>();
    EXPECT_FALSE(tracker->GetMedianCycle().has_value());
}

TEST(TEpochCycleTracker, MedianOfFewSamples)
{
    auto tracker = New<TEpochCycleTracker>();
    tracker->RecordCycle(TDuration::Seconds(1));
    EXPECT_EQ(*tracker->GetMedianCycle(), TDuration::Seconds(1));

    tracker->RecordCycle(TDuration::Seconds(100));
    tracker->RecordCycle(TDuration::Seconds(2));
    // Samples: {1, 2, 100} — the outlier does not shift the median.
    EXPECT_EQ(*tracker->GetMedianCycle(), TDuration::Seconds(2));
}

TEST(TEpochCycleTracker, OldSamplesFallOutOfWindow)
{
    auto tracker = New<TEpochCycleTracker>();
    for (size_t i = 0; i < TEpochCycleTracker::DefaultWindowSize; ++i) {
        tracker->RecordCycle(TDuration::Seconds(1));
    }
    EXPECT_EQ(*tracker->GetMedianCycle(), TDuration::Seconds(1));

    // Overwrite the whole ring with a new regime.
    for (size_t i = 0; i < TEpochCycleTracker::DefaultWindowSize; ++i) {
        tracker->RecordCycle(TDuration::Minutes(10));
    }
    EXPECT_EQ(*tracker->GetMedianCycle(), TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
