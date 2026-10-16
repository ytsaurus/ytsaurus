#include <yt/yt/server/controller_agent/input_statistics_collector.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TInputStatisticsCollectorTest, EmptyInputHasNeutralRatios)
{
    auto statistics = TInputStatisticsCollector().Finish();

    ASSERT_FALSE(std::isnan(statistics.CompressionRatio));
    ASSERT_FALSE(std::isnan(statistics.DataWeightRatio));
    ASSERT_EQ(statistics.CompressionRatio, 1.0);
    ASSERT_EQ(statistics.DataWeightRatio, 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
