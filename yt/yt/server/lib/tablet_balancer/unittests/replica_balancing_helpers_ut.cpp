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

class TCommonPivotKeyIndicesTest
    : public ::testing::Test
{ };

TEST_F(TCommonPivotKeyIndicesTest, TestEqual)
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
        {3, 3},
    };
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

TEST_F(TCommonPivotKeyIndicesTest, TestUnequal)
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
    std::vector<std::pair<int, int>> expectedCommonIndices = {{0, 0}, {4, 5}};
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

TEST_F(TCommonPivotKeyIndicesTest, TestMixed)
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
    std::vector<std::pair<int, int>> expectedCommonIndices = {{0, 0}, {2, 3}, {4, 5}};
    ASSERT_EQ(expectedCommonIndices, commonIndices);
}

////////////////////////////////////////////////////////////////////////////////

class TMetricDistributionTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*metrics*/ std::vector<i64>,
        /*distribution*/ std::vector<double>>>
{ };

TEST_P(TMetricDistributionTest, ViaMemorySize)
{
    const auto& params = GetParam();
    auto sizes = std::get<0>(params);
    ASSERT_EQ(
        GetCumulativeDistribution(TRange<i64>(sizes.begin(), sizes.end()), Logger(), /*enableVerboseLogging*/ true),
        std::get<1>(params));
}

INSTANTIATE_TEST_SUITE_P(
    TMetricDistributionTest,
    TMetricDistributionTest,
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
            std::vector<double>{0, 1})));

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

class TCalculateMajorMetricsBetweenSamePivotsTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*rightMetrics*/ std::vector<double>,
        /*leftSizes*/ std::vector<i64>,
        /*rightSizes*/ std::vector<i64>,
        /*metrics*/ std::vector<double>>>
{ };

TEST_P(TCalculateMajorMetricsBetweenSamePivotsTest, Simple)
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
    TCalculateMajorMetricsBetweenSamePivotsTest,
    TCalculateMajorMetricsBetweenSamePivotsTest,
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
                std::vector<double>{0})));

////////////////////////////////////////////////////////////////////////////////

auto MakePivotKeys(const std::vector<int>& values)
{
    std::vector<TLegacyOwningKey> keys;
    for (auto value : values) {
        keys.push_back(MakeSimpleKey(value, "value"));
    }
    return keys;
}

class TCalculateMajorMetricsTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*rightMetrics*/ std::vector<double>,
        /*leftSizes*/ std::vector<i64>,
        /*rightSizes*/ std::vector<i64>,
        /*leftPivotKeys*/ std::vector<int>,
        /*rightPivotKeys*/ std::vector<int>,
        /*metrics*/ std::vector<double>>>
{ };

TEST_P(TCalculateMajorMetricsTest, Simple)
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
    TCalculateMajorMetricsTest,
    TCalculateMajorMetricsTest,
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
            std::vector<double>{1600, 160, 760, 680, 160, 2100})));

////////////////////////////////////////////////////////////////////////////////

class TReshardByReferencePivotsTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        /*leftPivotKeys*/ std::vector<int>,
        /*rightPivotKeys*/ std::vector<int>,
        /*maxTabletCount*/ int,
        /*expectedActions*/ std::vector<std::pair<int, std::vector<int>>>>>
{ };

int VerifyActions(
    const std::vector<std::pair<int, std::vector<int>>>& expectedActions,
    const std::vector<std::pair<int, std::vector<TLegacyOwningKey>>>& actualActions)
{
    EXPECT_EQ(std::ssize(expectedActions), std::ssize(actualActions));
    int tabletCount = 0;
    for (int index = 0; index < std::ssize(expectedActions); ++index) {
        EXPECT_EQ(expectedActions[index].first, actualActions[index].first);
        EXPECT_EQ(expectedActions[index].second.size(), actualActions[index].second.size());
        tabletCount += actualActions[index].first;
        for (int pivotIndex = 0; pivotIndex < std::ssize(expectedActions[index].second); ++pivotIndex) {
            EXPECT_EQ(
                MakeSimpleKey(expectedActions[index].second[pivotIndex], "value"),
                actualActions[index].second[pivotIndex]);
        }
    }
    return tabletCount;
}

TEST_P(TReshardByReferencePivotsTest, Simple)
{
    const auto& params = GetParam();
    auto leftPivotKeys = MakePivotKeys(std::get<0>(params));
    auto rightPivotKeys = MakePivotKeys(std::get<1>(params));
    auto expectedActions = std::get<3>(params);

    auto actualActions = ReshardByReferencePivots(
        TRange(leftPivotKeys),
        TRange(rightPivotKeys),
        std::get<2>(params),
        Logger(),
        /*enableVerboseLogging*/ true);

    auto usedTabletCount = VerifyActions(expectedActions, actualActions);
    ASSERT_EQ(usedTabletCount, std::ssize(leftPivotKeys))
        << "actualActions: " << Format("%v", actualActions)
        << "; expectedActions: " << Format("%v", expectedActions)
        << "; usetTabletCount: " << usedTabletCount
        << "; tabletCount: " << leftPivotKeys.size();
}

INSTANTIATE_TEST_SUITE_P(
    TReshardByReferencePivotsTest,
    TReshardByReferencePivotsTest,
    ::testing::Values(
        std::tuple(
            std::vector<int>{0, 1},
            std::vector<int>{0},
            /*maxTabletCount*/ 3,
            std::vector{
                std::pair(2, std::vector{0}),        // [0, [1 -> [0
            }),
        std::tuple(
            std::vector<int>{0, 1, 2, 3, 6},
            std::vector<int>{0, 4},
            /*maxTabletCount*/ 3,
            std::vector{
                std::pair(3, std::vector{0}),        // [0, [1, [2 -> [0
                std::pair(2, std::vector{3, 4}),     // [3, [6 -> [3, [4
            }),
        std::tuple(
            std::vector<int>{0, 4, 7},
            std::vector<int>{0, 1, 2, 3, 8},
            /*maxTabletCount*/ 3,
            std::vector{
                std::pair(1, std::vector{0, 1, 2}),  // [0 -> [0, [1, [2
                std::pair(2, std::vector{4, 8}),     // [4, [7 -> [4, [8
            }),
        std::tuple(
            std::vector<int>{0, 1, 3, 5},
            std::vector<int>{0, 2, 4, 6},
            /*maxTabletCount*/ 3,
            std::vector{
                std::pair(3, std::vector{0, 2}),     // [0, [1, [3 -> [0, [2
                std::pair(1, std::vector{5, 6}),     // [5 -> [5, [6
            }),
        std::tuple(
            std::vector<int>{0, 1, 2, 3, 6, 9},
            std::vector<int>{0, 4, 8},
            /*maxTabletCount*/ 3,
            std::vector{
                std::pair(3, std::vector{0}),        // [0, [1, [2 -> [0
                std::pair(2, std::vector{3, 4, 8}),  // [3, [6 -> [3, [4, [8
                std::pair(1, std::vector{9}),        // [9 -> [9
            })));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer
