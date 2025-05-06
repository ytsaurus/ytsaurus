#include <yt/yt/server/lib/tablet_balancer/replica_balancing_helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTabletBalancer {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr double MetricError = 1e-10;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "BalancingHelpersUnittest");

////////////////////////////////////////////////////////////////////////////////

auto MakeSimpleKey(int first, const std::string& second)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(first, 0));
    builder.AddValue(MakeUnversionedStringValue(second, 1));
    return builder.FinishRow();
}

class TTestCommonPivotKeyIndices
    : public ::testing::Test
{ };

TEST_F(TTestCommonPivotKeyIndices, TestEqual)
{
    std::vector<TLegacyOwningKey> leftKeys;
    leftKeys.push_back(MakeSimpleKey(1000, "blabla"));
    leftKeys.push_back(MakeSimpleKey(1000, "sad test"));
    leftKeys.push_back(MakeSimpleKey(2000, "blabla"));
    auto rightKeys = leftKeys;

    auto commonIndices = GetCommonKeyIndices(leftKeys, rightKeys, Logger(), /*enableVerboseLogging*/ true);
    std::vector<std::pair<int, int>> expectedCommonIndices = {
        {0, 0},
        {1, 1},
        {2, 2},
    };
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

TEST_F(TTestCommonPivotKeyIndices, TestUnequal)
{
    std::vector<TLegacyOwningKey> leftKeys, rightKeys;
    leftKeys.push_back(MakeSimpleKey(1000, "blabla"));
    leftKeys.push_back(MakeSimpleKey(1000, "sad test"));
    leftKeys.push_back(MakeSimpleKey(2000, "blabla"));
    leftKeys.push_back(MakeSimpleKey(3000, "blabla"));

    rightKeys.push_back(leftKeys.front());
    rightKeys.push_back(MakeSimpleKey(1000, "blabla"));
    rightKeys.push_back(MakeSimpleKey(1500, "blabla"));
    rightKeys.push_back(MakeSimpleKey(2000, "sad test"));
    rightKeys.push_back(MakeSimpleKey(5000, "sad test"));

    auto commonIndices = GetCommonKeyIndices(leftKeys, rightKeys, Logger(), /*enableVerboseLogging*/ true);
    std::vector<std::pair<int, int>> expectedCommonIndices = {{0, 0}};
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

TEST_F(TTestCommonPivotKeyIndices, TestMixed)
{
    std::vector<TLegacyOwningKey> leftKeys, rightKeys;
    leftKeys.push_back(MakeSimpleKey(1000, "blabla"));
    leftKeys.push_back(MakeSimpleKey(1000, "sad test"));
    leftKeys.push_back(MakeSimpleKey(2000, "blabla"));
    leftKeys.push_back(MakeSimpleKey(3000, "blabla"));

    rightKeys.push_back(leftKeys.front());
    rightKeys.push_back(MakeSimpleKey(1000, "something else"));
    rightKeys.push_back(MakeSimpleKey(1500, "blabla"));
    rightKeys.push_back(MakeSimpleKey(2000, "blabla"));
    rightKeys.push_back(MakeSimpleKey(5000, "sad test"));

    auto commonIndices = GetCommonKeyIndices(leftKeys, rightKeys, Logger(), /*enableVerboseLogging*/ true);
    std::vector<std::pair<int, int>> expectedCommonIndices = {{0, 0}, {2, 3}};
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

////////////////////////////////////////////////////////////////////////////////

class TTestMetricDistribution
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*metrics*/ std::vector<i64>,
        /*distribution*/ std::vector<double>>>
{ };

TEST_P(TTestMetricDistribution, ViaMemorySize)
{
    const auto& params = GetParam();
    auto sizes = std::get<0>(params);
    ASSERT_EQ(
        GetCumulativeDistribution(TRange<i64>(sizes.begin(), sizes.end()), Logger(), /*enableVerboseLogging*/ true),
        std::get<1>(params));
}

INSTANTIATE_TEST_SUITE_P(
    TTestMetricDistribution,
    TTestMetricDistribution,
    ::testing::Values(
        std::tuple(
            std::vector<i64>{40, 20, 60, 40},
            std::vector<double>{0, 0.25, 0.375, 3./4, 1}),
        std::tuple(
            std::vector<i64>{0, 100, 0, 25, 0},
            std::vector<double>{0, 0, 0.8, 0.8, 1, 1}),
        std::tuple(
            std::vector<i64>{0, 0, 0, 0},
            std::vector<double>{0, 0.25, 0.5, 0.75, 1}),
        std::tuple(
            std::vector<i64>{40},
            std::vector<double>{0, 1}
        )));

////////////////////////////////////////////////////////////////////////////////

bool AreMetricsEqual(double lhs, double rhs)
{
    return lhs <= rhs + MetricError && rhs <= lhs + MetricError;
}

bool AreMetricsEqual(
    const std::vector<double>& leftMetrics,
    const std::vector<double>& rightMetrics)
{
    EXPECT_EQ(std::ssize(leftMetrics), std::ssize(rightMetrics));
    if (std::ssize(leftMetrics) != std::ssize(rightMetrics)) {
        return false;
    }

    for (int index = 0; index < std::ssize(leftMetrics); ++index) {
        EXPECT_TRUE(AreMetricsEqual(leftMetrics[index], rightMetrics[index]));
        if (!AreMetricsEqual(leftMetrics[index], rightMetrics[index])) {
            return false;
        }
    }
    return true;
}

auto GetTotalMettric(const std::vector<double>& metrics)
{
    return std::accumulate(
        metrics.begin(),
        metrics.end(),
        0.0,
        [] (double x, const auto& metric) {
            return x + metric;
        });
}

class TTestCalculateMajorMetricsBetweenSamePivots
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*rightMetrics*/ std::vector<double>,
        /*leftSizes*/ std::vector<i64>,
        /*rightSizes*/ std::vector<i64>,
        /*metrics*/ std::vector<double>>>
{ };

TEST_P(TTestCalculateMajorMetricsBetweenSamePivots, Simple)
{
    const auto& params = GetParam();
    auto rightMetrics = std::get<0>(params);
    auto leftSizes = std::get<1>(params);
    auto rightSizes = std::get<2>(params);
    auto expectedMetrics = std::get<3>(params);

    auto actualMetrics = CalculateMajorMetricsBetweenSamePivots(
        TRange<double>(rightMetrics.begin(), rightMetrics.end()),
        TRange<i64>(leftSizes.begin(), leftSizes.end()),
        TRange<i64>(rightSizes.begin(), rightSizes.end()),
        Logger(),
        /*enableVerboseLogging*/ true);

    EXPECT_TRUE(AreMetricsEqual(actualMetrics, expectedMetrics))
        << "actualMetrics: " << ToString(actualMetrics)
        << "; expectedMetrics: " << ToString(expectedMetrics);

    auto actualTotalMetric = GetTotalMettric(actualMetrics);
    auto expectedTotalMetric = GetTotalMettric(rightMetrics);
    EXPECT_TRUE(AreMetricsEqual(GetTotalMettric(actualMetrics), GetTotalMettric(rightMetrics)))
        << "actualTotalMetric: " << actualTotalMetric
        << "; expectedTotalMetric: " << expectedTotalMetric;
}

INSTANTIATE_TEST_SUITE_P(
    TTestCalculateMajorMetricsBetweenSamePivots,
    TTestCalculateMajorMetricsBetweenSamePivots,
    ::testing::Values(
        std::tuple(
            std::vector<double>{40, 20, 60, 40},
            std::vector<i64>{100, 20, 100, 20},
            std::vector<i64>{100, 20, 100, 20},
            std::vector<double>{40, 20, 60, 40}),
        std::tuple(
            std::vector<double>{60, 100},
            std::vector<i64>{0, 0, 0, 0},
            std::vector<i64>{10, 10},
            std::vector<double>{30, 30, 50, 50}),
        std::tuple(
            std::vector<double>{40, 20, 60, 40},
            std::vector<i64>{10, 10},
            std::vector<i64>{0, 0, 0, 0},
            std::vector<double>{60, 100}),
        std::tuple(
            std::vector<double>{100},
            std::vector<i64>{0, 100, 0, 25, 0},
            std::vector<i64>{20},
            std::vector<double>{0, 80, 0, 20, 0}),
        std::tuple(
            std::vector<double>{24, 6, 66, 105},
            std::vector<i64>{30, 30, 15, 10, 35},
            std::vector<i64>{30, 10, 15, 25},
            std::vector<double>{16, 14, 44, 29, 98}),
        std::tuple(
            std::vector<double>{44, 28, 98},
            std::vector<i64>{15, 25},
            std::vector<i64>{15, 10, 35},
            std::vector<double>{65, 105}),
            std::tuple(
            std::vector<double>{0},
            std::vector<i64>{40},
            std::vector<i64>{0},
            std::vector<double>{0}
        )));

////////////////////////////////////////////////////////////////////////////////

auto MakePivotKeys(const std::vector<int>& values)
{
    std::vector<TLegacyOwningKey> keys;
    for (auto value : values) {
        keys.push_back(MakeSimpleKey(value, "value"));
    }
    return keys;
}

class TTestCalculateMajorMetrics
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*rightMetrics*/ std::vector<double>,
        /*leftSizes*/ std::vector<i64>,
        /*rightSizes*/ std::vector<i64>,
        /*leftPivotKeys*/ std::vector<int>,
        /*rightPivotKeys*/ std::vector<int>,
        /*metrics*/ std::vector<double>>>
{ };

TEST_P(TTestCalculateMajorMetrics, Simple)
{
    const auto& params = GetParam();
    auto rightMetrics = std::get<0>(params);
    auto expectedMetrics = std::get<5>(params);

    auto actualMetrics = CalculateMajorMetrics(
        std::get<0>(params),
        std::get<1>(params),
        std::get<2>(params),
        MakePivotKeys(std::get<3>(params)),
        MakePivotKeys(std::get<4>(params)),
        Logger(),
        /*enableVerboseLogging*/ true);

    EXPECT_TRUE(AreMetricsEqual(actualMetrics, expectedMetrics))
        << "actualMetrics: " << ToString(actualMetrics)
        << "; expectedMetrics: " << ToString(expectedMetrics);

    auto actualTotalMetric = GetTotalMettric(actualMetrics);
    auto expectedTotalMetric = GetTotalMettric(rightMetrics);
    EXPECT_TRUE(AreMetricsEqual(GetTotalMettric(actualMetrics), GetTotalMettric(rightMetrics)))
        << "actualTotalMetric: " << actualTotalMetric
        << "; expectedTotalMetric: " << expectedTotalMetric;
}

INSTANTIATE_TEST_SUITE_P(
    TTestCalculateMajorMetrics,
    TTestCalculateMajorMetrics,
    ::testing::Values(
        std::tuple(
            std::vector<double>{40, 20, 60, 40},
            std::vector<i64>{100, 20, 100, 20},
            std::vector<i64>{15, 100, 15, 200},
            std::vector<int>{1, 2, 3, 4},
            std::vector<int>{1, 2, 3, 4},
            std::vector<double>{40, 20, 60, 40}),
        std::tuple(
            std::vector<double>{100},
            std::vector<i64>{0, 100, 0, 25, 0},
            std::vector<i64>{20},
            std::vector<int>{1, 2, 3, 4, 5},
            std::vector<int>{1},
            std::vector<double>{0, 80, 0, 20, 0}),
        std::tuple(
            std::vector<double>{40},
            std::vector<i64>{0},
            std::vector<i64>{0},
            std::vector<int>{1},
            std::vector<int>{1},
            std::vector<double>{40}),
        std::tuple(
            std::vector<double>{840, 420, 1260, 840, 2100},
            std::vector<i64>{200, 20, 95, 85, 20, 15},
            std::vector<i64>{20, 10, 30, 20, 50},
            std::vector<int>{1, 3, 5, 7, 9, 10},
            std::vector<int>{1, 4, 6, 8, 10},
            std::vector<double>{1600, 160, 760, 680, 160, 2100}
        )));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer
