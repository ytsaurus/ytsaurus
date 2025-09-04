#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/server/lib/misc/max_min_balancer.h>

#include <iostream>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestBalancer
    : public TDecayingMaxMinBalancer<int, double>
{
public:
    using TBase = TDecayingMaxMinBalancer<int, double>;

    using TBase::TBase;

    auto PeekContenders() -> decltype(ContenderToWeight_)&
    {
        return ContenderToWeight_;
    }
};

// Adds some weights to the elements of the queue and returns them in the expected order.
std::vector<int> Shuffle(TTestBalancer& balancer)
{
    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        *winner = 0;
        balancer.AddWeight(*winner, 100);
    }
    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        *winner = 1;
        balancer.AddWeight(*winner, 80);
    }
    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        *winner = 2;
        balancer.AddWeight(*winner, 90);
    }
    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        *winner = 3;
        balancer.AddWeight(*winner, 60);
    }
    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        *winner = 4;
        balancer.AddWeight(*winner, 70);
    }

    // Now the picture should be the following:
    return {3 /*60*/, 4 /*70*/, 1 /*80*/, 2 /*90*/, 0 /*100*/};
}

std::vector<int> ShuffleWithDefaults(TTestBalancer& balancer, std::vector<int> contenderRange)
{
    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        *winner = 0;
        balancer.AddWeightWithDefault(*winner, 100);
    }
    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        *winner = 1;
        balancer.AddWeightWithDefault(*winner, 80);
    }
    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        *winner = 2;
        balancer.AddWeightWithDefault(*winner, 90);
    }
    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        *winner = 3;
        balancer.AddWeightWithDefault(*winner, 60);
    }
    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        *winner = 4;
        balancer.AddWeightWithDefault(*winner, 70);
    }

    // Now the picture should be the following:
    return {3 /*60*/, 4 /*70*/, 1 /*80*/, 2 /*90*/, 0 /*100*/};
}

TEST(TMaxMinBalancerTest, Basic)
{
    auto decayInterval = TDuration::MilliSeconds(200);
    auto sleepInterval = TDuration::MilliSeconds(300);

    TTestBalancer balancer(0.1, decayInterval);
    for (int i = 0; i < 5; ++i) {
        balancer.AddContender(i);
    }

    auto expected = Shuffle(balancer);

    for (const auto& e : expected) {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        EXPECT_EQ(e, *winner);
        balancer.AddWeight(*winner, 1000); // Just to shift to the back of the queue.
    }

    Sleep(sleepInterval);

    {
        auto winner = balancer.ChooseWinner();
        EXPECT_TRUE(winner);
        balancer.AddWeight(*winner, 10);
    }

    auto& contenders = balancer.PeekContenders();
    EXPECT_THAT(GetValues(contenders), testing::UnorderedElementsAre(
        107, // (70 + 1000) * 0.1
        108, // ditto
        109,
        110,
        116)); // (60 + 1000) * 0.1 + 10
}

TEST(TMaxMinBalancerTest, Lazy)
{
    auto decayInterval = TDuration::MilliSeconds(200);
    auto sleepInterval = TDuration::MilliSeconds(300);

    TTestBalancer balancer(0.1, decayInterval);

    std::vector<int> contenderRange = {0, 1, 2, 3, 4};

    auto expected = ShuffleWithDefaults(balancer, contenderRange);

    for (const auto& e : expected) {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        EXPECT_EQ(e, *winner);
        balancer.AddWeightWithDefault(*winner, 1000); // Just to shift to the back of the queue.
    }

    Sleep(sleepInterval);

    {
        auto winner = balancer.ChooseRangeWinner(contenderRange);
        EXPECT_TRUE(winner);
        balancer.AddWeightWithDefault(*winner, 10);
    }

    auto& contenders = balancer.PeekContenders();
    EXPECT_THAT(GetValues(contenders), testing::UnorderedElementsAre(
        107, // (70 + 1000) * 0.1
        108, // ditto
        109,
        110,
        116)); // (60 + 1000) * 0.1 + 10
}

TEST(TMaxMinBalancerTest, ChooseWinnerIf)
{
    TTestBalancer balancer(0.1, TDuration::Seconds(1));
    for (int i = 0; i < 5; ++i) {
        balancer.AddContender(i);
    }

    auto expected = Shuffle(balancer);

    std::vector<int> toSkip(expected.begin(), expected.begin() + 2);

    {
        auto winner = balancer.ChooseWinnerIf(
            [&] (int i) {
                return std::find(toSkip.begin(), toSkip.end(), i) == toSkip.end();
            });

        EXPECT_TRUE(winner);
        EXPECT_EQ(expected[2], *winner);
    }

    {
        auto winner = balancer.ChooseWinnerIf(
            [&] (int i) {
                return std::find(expected.begin(), expected.end(), i) == toSkip.end();
            });
        EXPECT_FALSE(winner);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
