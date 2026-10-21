#include <yt/yt/flow/library/cpp/throughput_autoscaler/computation_warmup.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NFlow {
namespace {

const auto Start = TInstant::Seconds(1000);
const auto Period = TDuration::Minutes(3);

TEST(TThroughputComputationWarmupTest, RequiresMeasuredCoverageRatherThanElapsedControllerTime)
{
    TThroughputComputationWarmup warmup;
    EXPECT_FALSE(warmup.Observe({
        .Now = Start + Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start,
        .MeasurementEnd = Start,
        .Period = Period,
    }));
    EXPECT_FALSE(warmup.Observe({
        .Now = Start + 2 * Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start + Period,
        .MeasurementEnd = Start + Period + Period / 2,
        .Period = Period,
    }));
    EXPECT_TRUE(warmup.Observe({
        .Now = Start + 2 * Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start + Period,
        .MeasurementEnd = Start + 2 * Period,
        .Period = Period,
    }));
    EXPECT_FALSE(warmup.Observe({
        .Now = Start + 2 * Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start + Period,
        .MeasurementEnd = Start + 2 * Period,
        .Period = Period,
    }));
    EXPECT_TRUE(warmup.Observe({
        .Now = Start + 3 * Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start + 2 * Period,
        .MeasurementEnd = Start + 3 * Period,
        .Period = Period,
    }));
}

TEST(TThroughputComputationWarmupTest, ResetAndLayoutChangeExcludeEarlierMeasurements)
{
    TThroughputComputationWarmup warmup;
    const auto changed = Start + Period;
    warmup.Reset(changed);
    EXPECT_FALSE(warmup.Observe({
        .Now = changed,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start,
        .MeasurementEnd = changed,
        .Period = Period,
    }));
    EXPECT_FALSE(warmup.Observe({
        .Now = changed + Period / 2,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start,
        .MeasurementEnd = changed + Period / 2,
        .Period = Period,
    }));
    EXPECT_TRUE(warmup.Observe({
        .Now = changed + Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = changed,
        .MeasurementEnd = changed + Period,
        .Period = Period,
    }));
    EXPECT_FALSE(warmup.Observe({
        .Now = changed + 2 * Period,
        .CohortReadyAt = changed + 2 * Period,
        .MeasurementBegin = changed,
        .MeasurementEnd = changed + 2 * Period,
        .Period = Period,
    }));
    EXPECT_TRUE(warmup.Observe({
        .Now = changed + 3 * Period,
        .CohortReadyAt = changed + 2 * Period,
        .MeasurementBegin = changed + 2 * Period,
        .MeasurementEnd = changed + 3 * Period,
        .Period = Period,
    }));
}

TEST(TThroughputComputationWarmupTest, LongerRetryNeedsLongerFreshCoverage)
{
    TThroughputComputationWarmup warmup;
    EXPECT_FALSE(warmup.Observe({
        .Now = Start + Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start,
        .MeasurementEnd = Start + Period,
        .Period = 2 * Period,
    }));
    EXPECT_TRUE(warmup.Observe({
        .Now = Start + 2 * Period,
        .CohortReadyAt = Start,
        .MeasurementBegin = Start,
        .MeasurementEnd = Start + 2 * Period,
        .Period = 2 * Period,
    }));
}

} // namespace
} // namespace NYT::NFlow
