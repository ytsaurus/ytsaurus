#include <yt/yt/flow/library/cpp/throughput_autoscaler/demand.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <array>
#include <limits>

namespace NYT::NFlow {
namespace {

TEST(TThroughputDemandTest, QuarterHourDebtRequiresTwiceTheSteadyRate)
{
    const auto demand = BuildThroughputDemand(New<TThroughputSpec>(), 100, 90'000, TDuration::Zero());
    EXPECT_EQ(demand.SteadyDemand, 100);
    EXPECT_EQ(demand.CatchUpDemand, 200);
}

TEST(TThroughputDemandTest, BatchAllowanceAndCatchUpArithmetic)
{
    auto config = New<TThroughputSpec>();
    config->CatchUpPeriod = TDuration::Minutes(30);
    for (const auto [offered, backlog, seconds, expected] : {
            std::array<double, 4>{100, 100, 0, 100 + 100.0 / 1800},
            {10'000, 6'000'000, 300, 10'000},
            {10'000, 7'800'000, 300, 11'000},
            {0, 600'000, 300, 600'000.0 / 1800},
            {100, 360'200, 1, 300},
         })
    {
        SCOPED_TRACE(backlog);
        auto demand = BuildThroughputDemand(config, offered, backlog, TDuration::Seconds(seconds));
        EXPECT_EQ(demand.SteadyDemand, offered);
        EXPECT_EQ(demand.NormalBacklog, 2 * seconds * offered);
        ASSERT_TRUE(demand.CatchUpDemand);
        EXPECT_NEAR(*demand.CatchUpDemand, expected, 1e-9);
    }
}

TEST(TThroughputDemandTest, UpstreamForecastOverridesLocalFlowWithoutInflatingBatchAllowance)
{
    TThroughputDemand upstream{.SteadyDemand = 2, .CatchUpDemand = 100};
    auto demand = BuildThroughputDemand(New<TThroughputSpec>(), 450, 22, TDuration::Seconds(1), &upstream);
    EXPECT_EQ(demand.SteadyDemand, 2);
    EXPECT_EQ(demand.NormalBacklog, 4);
    EXPECT_EQ(demand.ExcessBacklog, 18);
    EXPECT_EQ(demand.CatchUpDemand, 100.02);
}

TEST(TThroughputDemandTest, CatchUpAndNormalBacklogUseConfiguredPeriods)
{
    auto config = New<TThroughputSpec>();
    config->CatchUpPeriod = TDuration::Seconds(60);
    config->NormalBacklogBatchCount = 1;
    const auto demand = BuildThroughputDemand(config, 100, 6200, TDuration::Seconds(2));
    EXPECT_EQ(demand.NormalBacklog, 200);
    EXPECT_EQ(demand.CatchUpDemand, 200);
}

TEST(TThroughputDemandTest, KnownZeroMissingAndInvalidInputsRemainDifferent)
{
    auto zero = BuildThroughputDemand(New<TThroughputSpec>(), 0, 0, TDuration::Seconds(1));
    EXPECT_EQ(zero.CatchUpDemand, 0);
    EXPECT_EQ(zero.ExcessBacklog, 0);
    TThroughputDemand upstream{.SteadyDemand = 2};
    EXPECT_FALSE(BuildThroughputDemand(New<TThroughputSpec>(), 100, 0, TDuration::Seconds(1), &upstream).CatchUpDemand);
    EXPECT_FALSE(BuildThroughputDemand(New<TThroughputSpec>(), std::nullopt, 0, TDuration::Seconds(1)).CatchUpDemand);
    EXPECT_FALSE(BuildThroughputDemand(New<TThroughputSpec>(), 2, std::nullopt, TDuration::Seconds(1)).CatchUpDemand);
    for (double invalid : {-1.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        auto demand = BuildThroughputDemand(New<TThroughputSpec>(), 2, invalid, TDuration::Seconds(1));
        EXPECT_EQ(demand.SteadyDemand, 2);
        EXPECT_FALSE(demand.CatchUpDemand);
        EXPECT_FALSE(demand.ExcessBacklog);
        EXPECT_FALSE(BuildThroughputDemand(New<TThroughputSpec>(), invalid, 100, TDuration::Seconds(1)).CatchUpDemand);
    }
    auto overflow = BuildThroughputDemand(New<TThroughputSpec>(), std::numeric_limits<double>::max(), 10, TDuration::Seconds(1));
    EXPECT_FALSE(overflow.NormalBacklog);
    EXPECT_FALSE(overflow.CatchUpDemand);
}

} // namespace
} // namespace NYT::NFlow
