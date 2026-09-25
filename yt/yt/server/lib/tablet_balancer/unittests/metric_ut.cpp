#include <yt/yt/server/lib/tablet_balancer/metric.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>

namespace NYT::NTabletBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <int Size>
struct TMetricSizeTag
{
    static constexpr int Value = Size;
};

using TMetricSizes = ::testing::Types<
    TMetricSizeTag<1>,
    TMetricSizeTag<2>,
    TMetricSizeTag<3>,
    TMetricSizeTag<4>,
    TMetricSizeTag<5>,
    TMetricSizeTag<6>,
    TMetricSizeTag<7>,
    TMetricSizeTag<8>
>;

template <class T>
class TMetricTest
    : public ::testing::Test
{
public:
    static constexpr int Size = T::Value;
    using TMetric = TGenericMetric<Size>;
    using TValues = std::array<double, Size>;

    static TValues MakeArray(double start, double step = 1.0)
    {
        TValues values;
        for (int index = 0; index < Size; ++index) {
            values[index] = start + index * step;
        }
        return values;
    }

    static TValues MakeArrayWithMixedValues(
        int smallCount,
        int atThresholdCount,
        double normalStart = 10.0)
    {
        TValues values;
        int index = 0;

        for (int count = 0; count < smallCount && index < Size; ++count, ++index) {
            values[index] = MinimumAcceptableMetricValue * 0.5;
        }

        for (int count = 0; count < atThresholdCount && index < Size; ++count, ++index) {
            values[index] = MinimumAcceptableMetricValue;
        }

        for (; index < Size; ++index) {
            values[index] = normalStart + index;
        }

        return values;
    }

    static void ExpectValues(const TMetric& metric, const TValues& expected)
    {
        double expectedTotal = 0.0;
        for (int index = 0; index < Size; ++index) {
            SCOPED_TRACE(index);
            EXPECT_DOUBLE_EQ(expected[index], metric[index]);
            expectedTotal += expected[index];
        }
        EXPECT_DOUBLE_EQ(expectedTotal, metric.GetTotalValue());
    }
};

TYPED_TEST_SUITE(TMetricTest, TMetricSizes);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TMetricTest, NormalizationBoundaryValues)
{
    constexpr double infinity = std::numeric_limits<double>::infinity();
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    const std::array inputs = {
        -infinity, -1.0, -0.0, 0.0,
        std::nextafter(MinimumAcceptableMetricValue, 0.0),
        MinimumAcceptableMetricValue,
        std::nextafter(MinimumAcceptableMetricValue, infinity),
        1.0, infinity, nan,
    };
    const std::array scalars = {-infinity, -10.0, -0.0, 0.0, 10.0, infinity, nan};

    for (int offset = 0; offset < std::ssize(inputs); ++offset) {
        std::array<double, TestFixture::Size> values;
        for (int index = 0; index < TestFixture::Size; ++index) {
            values[index] = inputs[(offset + index) % std::ssize(inputs)];
        }

        for (double scalar : scalars) {
            auto normalized = TGenericMetric<TestFixture::Size>(values).AsNormalizationFactor(scalar);
            for (int index = 0; index < TestFixture::Size; ++index) {
                double expected = values[index] < MinimumAcceptableMetricValue ? 1.0 : scalar / values[index];
                double actual = normalized[index];
                if (std::isnan(expected)) {
                    EXPECT_TRUE(std::isnan(actual));
                } else {
                    EXPECT_EQ(expected, actual);
                    EXPECT_EQ(std::signbit(expected), std::signbit(actual));
                }
            }
        }
    }
}

TYPED_TEST(TMetricTest, Constructors)
{
    using TMetric = typename TestFixture::TMetric;

    auto values = TestFixture::MakeArray(1.0);
    TMetric metric(values);
    TMetric copy(metric);

    TestFixture::ExpectValues(metric, values);
    TestFixture::ExpectValues(TMetric(), {});
    TestFixture::ExpectValues(copy, values);

    metric += 1.0;
    TestFixture::ExpectValues(copy, values);
}

TYPED_TEST(TMetricTest, ZeroAndUnit)
{
    using TMetric = typename TestFixture::TMetric;

    TestFixture::ExpectValues(TMetric::Zero(), {});
    TestFixture::ExpectValues(TMetric::Unit(), TestFixture::MakeArray(1.0, 0.0));
}

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TMetricTest, Subscript)
{
    using TMetric = typename TestFixture::TMetric;

    auto values = TestFixture::MakeArray(1.0);
    TMetric metric(values);
    const auto& constMetric = metric;
    for (int index = 0; index < TestFixture::Size; ++index) {
        SCOPED_TRACE(index);
        EXPECT_DOUBLE_EQ(values[index], metric[index]);
        EXPECT_DOUBLE_EQ(values[index], constMetric[index]);
        metric[index] += 10.0;
        values[index] += 10.0;
        TestFixture::ExpectValues(constMetric, values);
    }
}

TYPED_TEST(TMetricTest, Formatting)
{
    using TMetric = typename TestFixture::TMetric;

    auto values = TestFixture::MakeArray(1.0);
    const TMetric metric(values);
    if constexpr (TestFixture::Size == 1) {
        EXPECT_EQ(ToString(metric), ToString(values[0]));
    } else {
        EXPECT_EQ(ToString(metric), Format("{%v, TotalValue: %v}", values, metric.GetTotalValue()));
    }
}

TYPED_TEST(TMetricTest, IsLessOrEqualComponentwise)
{
    using TMetric = typename TestFixture::TMetric;

    auto values = TestFixture::MakeArray(2.0);
    const TMetric metric(values);
    EXPECT_TRUE(metric.IsLessOrEqualComponentwise(metric));
    EXPECT_TRUE(TMetric::Zero().IsLessOrEqualComponentwise(TMetric::Zero()));
    EXPECT_TRUE((metric - 1.0).IsLessOrEqualComponentwise(metric));
    EXPECT_FALSE((metric + 1.0).IsLessOrEqualComponentwise(metric));

    for (int index = 0; index < TestFixture::Size; ++index) {
        SCOPED_TRACE(index);
        auto smallerValues = values;
        smallerValues[index] -= 1.0;
        const TMetric smallerMetric(smallerValues);
        EXPECT_TRUE(smallerMetric.IsLessOrEqualComponentwise(metric));
        EXPECT_FALSE(metric.IsLessOrEqualComponentwise(smallerMetric));

        if constexpr (TestFixture::Size > 1) {
            auto mixedValues = values;
            mixedValues[index] += 1.0;
            mixedValues[(index + 1) % TestFixture::Size] -= 1.0;
            const TMetric mixedMetric(mixedValues);
            // Equal totals do not imply componentwise comparability.
            EXPECT_FALSE(mixedMetric.IsLessOrEqualComponentwise(metric));
            EXPECT_FALSE(metric.IsLessOrEqualComponentwise(mixedMetric));
        }
    }
}

TYPED_TEST(TMetricTest, ComponentwiseComparisonWithNaN)
{
    using TMetric = typename TestFixture::TMetric;

    auto values = TestFixture::MakeArray(1.0);
    const TMetric metric(values);
    for (int index = 0; index < TestFixture::Size; ++index) {
        SCOPED_TRACE(index);
        auto nanValues = values;
        nanValues[index] = std::numeric_limits<double>::quiet_NaN();
        const TMetric nanMetric(nanValues);
        EXPECT_FALSE(nanMetric.IsLessOrEqualComponentwise(metric));
        EXPECT_FALSE(metric.IsLessOrEqualComponentwise(nanMetric));
        EXPECT_FALSE(nanMetric.IsLessOrEqualComponentwise(nanMetric));
    }
}

////////////////////////////////////////////////////////////////////////////////

#define CHECK_ARITHMETIC(lhsStart, rhsStart, scalar, operation) \
    do { \
        SCOPED_TRACE(#operation); \
        auto lhsValues = TestFixture::MakeArray(lhsStart); \
        auto rhsValues = TestFixture::MakeArray(rhsStart); \
        typename TestFixture::TValues expectedMetric; \
        typename TestFixture::TValues expectedScalar; \
        for (int index = 0; index < TestFixture::Size; ++index) { \
            expectedMetric[index] = lhsValues[index] operation rhsValues[index]; \
            expectedScalar[index] = lhsValues[index] operation scalar; \
        } \
        TMetric lhs(lhsValues); \
        TMetric rhs(rhsValues); \
        TestFixture::ExpectValues(lhs operation rhs, expectedMetric); \
        TestFixture::ExpectValues(lhs operation scalar, expectedScalar); \
        TestFixture::ExpectValues(lhs, lhsValues); \
        TestFixture::ExpectValues(rhs, rhsValues); \
        auto compoundMetric = lhs; \
        compoundMetric operation##= rhs; \
        TestFixture::ExpectValues(compoundMetric, expectedMetric); \
        auto compoundScalar = lhs; \
        compoundScalar operation##= scalar; \
        TestFixture::ExpectValues(compoundScalar, expectedScalar); \
    } while (false)

TYPED_TEST(TMetricTest, Arithmetics)
{
    using TMetric = typename TestFixture::TMetric;

    CHECK_ARITHMETIC(22.0, 10.0, 5.4, +);
    CHECK_ARITHMETIC(10.0, 32.0, 3.2, -);
    CHECK_ARITHMETIC(42.0, 322.0, 22.8, *);
    CHECK_ARITHMETIC(122.0, 75.0, 9.9, /);
}

#undef CHECK_ARITHMETIC

////////////////////////////////////////////////////////////////////////////////

#define CHECK_PRODUCT(lhsStart, rhsStart, multiplierStart, scalar, operation) \
    do { \
        SCOPED_TRACE(#operation); \
        auto lhsValues = TestFixture::MakeArray(lhsStart); \
        auto rhsValues = TestFixture::MakeArray(rhsStart); \
        auto multiplierValues = TestFixture::MakeArray(multiplierStart); \
        auto expectedMetric = lhsValues; \
        auto expectedScalar = lhsValues; \
        for (int index = 0; index < TestFixture::Size; ++index) { \
            double metricProduct = rhsValues[index] * multiplierValues[index]; \
            double scalarProduct = rhsValues[index] * scalar; \
            expectedMetric[index] operation metricProduct; \
            expectedScalar[index] operation scalarProduct; \
        } \
        TMetric metricResult(lhsValues); \
        metricResult operation TMetric(rhsValues) * TMetric(multiplierValues); \
        TestFixture::ExpectValues(metricResult, expectedMetric); \
        TMetric scalarResult(lhsValues); \
        scalarResult operation TMetric(rhsValues) * (scalar); \
        TestFixture::ExpectValues(scalarResult, expectedScalar); \
    } while (false)

TYPED_TEST(TMetricTest, ProductAccumulation)
{
    using TMetric = typename TestFixture::TMetric;

    CHECK_PRODUCT(12.0, 32.2, 42.0, 411.0, +=);
    CHECK_PRODUCT(99.0, 23.1, 23.0, 12.0, -=);
}

#undef CHECK_PRODUCT

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TMetricTest, NormalizedMetric)
{
    using TMetric = typename TestFixture::TMetric;
    using TValues = typename TestFixture::TValues;

    auto check = [] (const TValues& values, double scalar) {
        SCOPED_TRACE(scalar);
        TValues expected;
        for (int index = 0; index < TestFixture::Size; ++index) {
            expected[index] = values[index] < MinimumAcceptableMetricValue ? 1.0 : scalar / values[index];
        }
        TestFixture::ExpectValues(TMetric(values).AsNormalizationFactor(scalar), expected);
    };

    check(TestFixture::MakeArray(2.0), 10.0);
    check(
        TestFixture::MakeArrayWithMixedValues(
            /*smallCount*/ std::min(2, TestFixture::Size),
            /*atThresholdCount*/ 0),
        5.0);
    check(
        TestFixture::MakeArrayWithMixedValues(
            /*smallCount*/ std::min(1, TestFixture::Size),
            /*atThresholdCount*/ std::min(1, std::max(0, TestFixture::Size - 1))),
        100.0);

    TValues values;
    values.fill(MinimumAcceptableMetricValue * 0.1);
    check(values, 10.0);

    values.fill(MinimumAcceptableMetricValue);
    check(values, 10.0);

    check(TestFixture::MakeArray(1.0), 1e15);
    check(TestFixture::MakeArray(100.0), 1e-10);
}

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TMetricTest, ComplexChain)
{
    using TMetric = typename TestFixture::TMetric;

    auto values1 = TestFixture::MakeArray(1.0);
    auto values2 = TestFixture::MakeArray(4.0);
    auto values3 = TestFixture::MakeArray(2.0);

    auto result = (TMetric(values1) * 2.5 + TMetric(values2)) / TMetric(values3) - TMetric(values1) * 0.5;
    typename TestFixture::TValues expected;
    for (int index = 0; index < TestFixture::Size; ++index) {
        expected[index] = (values1[index] * 2.5 + values2[index]) / values3[index] - values1[index] * 0.5;
    }

    TestFixture::ExpectValues(result, expected);
}

TYPED_TEST(TMetricTest, ComplexChainWithNormalized)
{
    using TMetric = typename TestFixture::TMetric;

    auto values1 = TestFixture::MakeArray(1.0);
    auto values2 = TestFixture::MakeArray(4.0);

    double scalar = 234.0;
    auto result = TMetric(values1) * 2.2 - TMetric(values2) -
        TMetric(values1) * TMetric(values2).AsNormalizationFactor(scalar);
    typename TestFixture::TValues expected;
    for (int index = 0; index < TestFixture::Size; ++index) {
        double normalized = values2[index] < MinimumAcceptableMetricValue ? 1.0 : scalar / values2[index];
        expected[index] = values1[index] * 2.2 - values2[index] - values1[index] * normalized;
    }

    TestFixture::ExpectValues(result, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer
