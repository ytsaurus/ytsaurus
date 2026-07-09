#include <yt/yt/core/test_framework/framework.h>

#include <util/generic/map.h>
#include <yt/yt/flow/library/cpp/misc/weighted_random.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using ::testing::Return;

class TMockRandomProvider : public IRandomDoubleProvider
{
public:
    MOCK_METHOD(double, Get, (), (override));
};

TEST(TWeightedRandomTest, BasicTest)
{
    auto provider = New<TMockRandomProvider>();
    TMap<std::string, double> container;
    container["A"] = 1;
    container["B"] = 2;
    container["C"] = 3;

    TWeightedRandom<std::string> random(container, provider);

    EXPECT_CALL(*provider, Get()).WillOnce(Return(0.4));
    ASSERT_EQ("B", random());
}

TEST(TWeightedRandomTest, BorderTest)
{
    auto provider = New<TMockRandomProvider>();
    TWeightedRandom<int> random(std::vector<std::pair<int, double>>{{1, 1}, {2, 2}, {3, 3}}, provider);

    EXPECT_CALL(*provider, Get()).WillOnce(Return(0.49999999999));
    ASSERT_EQ(2, random());

    EXPECT_CALL(*provider, Get()).WillOnce(Return(0.50000000001));
    ASSERT_EQ(3, random());
}

TEST(TWeightedRandomTest, EmptyTest)
{
    ASSERT_ANY_THROW(auto obj = TWeightedRandom<int>(std::vector<std::pair<int, double>>{}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
