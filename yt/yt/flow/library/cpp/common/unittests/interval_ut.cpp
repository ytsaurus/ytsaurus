#include <random>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/interval.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

//! Check whether values of the vector are sorted in ascending order (in contrast to std::sort which is for non-descending order).
template <class T>
bool IsStrictlySorted(const std::vector<T>& vec)
{
    if (std::begin(vec) == std::end(vec)) {
        return true;
    }
    for (auto it = std::begin(vec), jt = std::next(it); jt != std::end(vec); ++it, ++jt) {
        if (!(*it < *jt)) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TNoSpaceShipInt
{
    int Value;

    TNoSpaceShipInt(int value)
        : Value(value)
    {
    }

    bool operator<(const TNoSpaceShipInt& other) const
    {
        return Value < other.Value;
    }

    bool operator==(const TNoSpaceShipInt& other) const
    {
        return Value == other.Value;
    }

    bool operator>(const TNoSpaceShipInt& other) const
    {
        return Value > other.Value;
    }

    bool operator<=(const TNoSpaceShipInt& other) const
    {
        return Value <= other.Value;
    }

    bool operator>=(const TNoSpaceShipInt& other) const
    {
        return Value >= other.Value;
    }

    bool operator!=(const TNoSpaceShipInt& other) const
    {
        return Value != other.Value;
    }

    friend void FormatValue(TStringBuilderBase* builder, const TNoSpaceShipInt& value, TStringBuf)
    {
        builder->AppendFormat("%v", value.Value);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Template class for testing TEndpoint with int and TNoSpaceShipInt (as type without <=> support).
template <class T>
class TIntervalTest : public testing::Test
{
public:
    static T FromInt(int val)
    {
        if constexpr (std::is_same_v<T, int>) {
            return val;
        } else if constexpr (std::is_same_v<T, TNoSpaceShipInt>) {
            return TNoSpaceShipInt(val);
        } else {
            static_assert(false, "Unsupported type");
        }
        YT_UNREACHABLE();
    }
};

////////////////////////////////////////////////////////////////////////////////

using TTestTypes = ::testing::Types<int, TNoSpaceShipInt>;
TYPED_TEST_SUITE(TIntervalTest, TTestTypes);

////////////////////////////////////////////////////////////////////////////////

//! Test of TEndpoint methods.
TYPED_TEST(TIntervalTest, Boundary)
{
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenLeft()), TEndpoint<TypeParam>(TEndpointOpenLeft(), std::optional<TypeParam>()));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenLeft()), TEndpoint<TypeParam>(TEndpointOpenLeft(), std::nullopt));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenLeft(), TestFixture::FromInt(0)), TEndpoint<TypeParam>(TEndpointOpenLeft(), std::optional<TypeParam>(TestFixture::FromInt(0))));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenRight()), TEndpoint<TypeParam>(TEndpointOpenRight(), std::optional<TypeParam>()));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenRight()), TEndpoint<TypeParam>(TEndpointOpenRight(), std::nullopt));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointOpenRight(), TestFixture::FromInt(0)), TEndpoint<TypeParam>(TEndpointOpenRight(), std::optional<TypeParam>(TestFixture::FromInt(0))));
    EXPECT_EQ(TEndpoint<TypeParam>(TEndpointClosed(), TestFixture::FromInt(0)), TEndpoint<TypeParam>(TestFixture::FromInt(0)));

    EXPECT_EQ(ToString(TEndpoint<TypeParam>(TEndpointOpenLeft())), "-inf");
    EXPECT_EQ(ToString(TEndpoint<TypeParam>(TEndpointOpenLeft(), TestFixture::FromInt(1))), "1+");
    EXPECT_EQ(ToString(TEndpoint<TypeParam>(TestFixture::FromInt(1))), "1");
    EXPECT_EQ(ToString(TEndpoint<TypeParam>(TEndpointOpenRight(), TestFixture::FromInt(1))), "1-");
    EXPECT_EQ(ToString(TEndpoint<TypeParam>(TEndpointOpenRight())), "+inf");

    std::vector<TEndpoint<TypeParam>> test;
    test.emplace_back(TEndpointOpenLeft{});
    test.emplace_back(TEndpointOpenRight{}, TestFixture::FromInt(1));
    test.emplace_back(TEndpointClosed{}, TestFixture::FromInt(1));
    test.emplace_back(TEndpointOpenLeft{}, TestFixture::FromInt(1));
    test.emplace_back(TEndpointOpenRight{}, TestFixture::FromInt(2));
    test.emplace_back(TEndpointClosed{}, TestFixture::FromInt(2));
    test.emplace_back(TEndpointOpenLeft{}, TestFixture::FromInt(2));
    test.emplace_back(TEndpointOpenRight{});
    EXPECT_TRUE(IsStrictlySorted(test));

    for (ssize_t i = 0; i < std::ssize(test); i++) {
        EXPECT_EQ(test[i], test[i]);
        for (ssize_t j = i + 1; j < std::ssize(test); j++) {
            EXPECT_NE(test[i], test[j]);
            EXPECT_LT(test[i], test[j]);
            EXPECT_GT(test[j], test[i]);
        }
    }

    for (TypeParam key : {TestFixture::FromInt(1), TestFixture::FromInt(2)}) {
        // Index of a value in test that is equal to the key.
        ssize_t iEqual = key == TestFixture::FromInt(1) ? 2 : 5;
        for (ssize_t i = 0; i < iEqual; i++) {
            EXPECT_LT(test[i], key);
            EXPECT_GT(key, test[i]);
            EXPECT_NE(test[i], key);
            EXPECT_NE(key, test[i]);
        }
        EXPECT_EQ(test[iEqual], key);
        EXPECT_EQ(key, test[iEqual]);
        for (ssize_t i = iEqual + 1; i < std::ssize(test); i++) {
            EXPECT_GT(test[i], key);
            EXPECT_LT(key, test[i]);
            EXPECT_NE(test[i], key);
            EXPECT_NE(key, test[i]);
        }
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::ranges::shuffle(test, g);
    std::ranges::sort(test);
    EXPECT_TRUE(IsStrictlySorted(test));
}

////////////////////////////////////////////////////////////////////////////////

//! Test of TInterval methods.
TYPED_TEST(TIntervalTest, Interval)
{
    {
        // [1, 1]
        TInterval<TypeParam> point{TestFixture::FromInt(1)};
        EXPECT_EQ(point.EffectiveLeft(), TestFixture::FromInt(1));
        EXPECT_EQ(point.EffectiveRight(), TestFixture::FromInt(1));
        EXPECT_EQ(point.ExtractLeft(), TestFixture::FromInt(1));
        EXPECT_EQ(point.ExtractRight(), std::nullopt);
        EXPECT_EQ(ToString(point), "[1, 1]");
    }
    {
        // (11, 13)
        TInterval<TypeParam> interval{TestFixture::FromInt(11), TestFixture::FromInt(13)};
        EXPECT_GT(interval.EffectiveLeft(), TestFixture::FromInt(11));
        EXPECT_LT(interval.EffectiveLeft(), TestFixture::FromInt(12));
        EXPECT_LT(interval.EffectiveRight(), TestFixture::FromInt(13));
        EXPECT_GT(interval.EffectiveRight(), TestFixture::FromInt(12));
        EXPECT_EQ(interval.ExtractLeft(), TestFixture::FromInt(11));
        EXPECT_EQ(interval.ExtractRight(), TestFixture::FromInt(13));
        EXPECT_EQ(ToString(interval), "(11, 13)");
    }
    {
        // (-inf, 22)
        TInterval<TypeParam> interval{std::nullopt, TestFixture::FromInt(22)};
        EXPECT_LT(interval.EffectiveLeft(), TestFixture::FromInt(0));
        EXPECT_LT(interval.EffectiveRight(), TestFixture::FromInt(22));
        EXPECT_GT(interval.EffectiveRight(), TestFixture::FromInt(11));
        EXPECT_EQ(interval.ExtractLeft(), std::nullopt);
        EXPECT_EQ(interval.ExtractRight(), TestFixture::FromInt(22));
        EXPECT_EQ(ToString(interval), "(-inf, 22)");
    }
    {
        // (32, +inf)
        TInterval<TypeParam> interval{TestFixture::FromInt(32), std::nullopt};
        EXPECT_GT(interval.EffectiveLeft(), TestFixture::FromInt(32));
        EXPECT_LT(interval.EffectiveLeft(), TestFixture::FromInt(33));
        EXPECT_GT(interval.EffectiveRight(), TestFixture::FromInt(100));
        EXPECT_EQ(interval.ExtractLeft(), TestFixture::FromInt(32));
        EXPECT_EQ(interval.ExtractRight(), std::nullopt);
        EXPECT_EQ(ToString(interval), "(32, +inf)");
    }
    {
        // (-inf, +inf)
        TInterval<TypeParam> interval{std::nullopt, std::nullopt};
        EXPECT_LT(interval.EffectiveLeft(), TestFixture::FromInt(0));
        EXPECT_GT(interval.EffectiveRight(), TestFixture::FromInt(100));
        EXPECT_EQ(interval.ExtractLeft(), std::nullopt);
        EXPECT_EQ(interval.ExtractRight(), std::nullopt);
        EXPECT_EQ(ToString(interval), "(-inf, +inf)");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
