#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/helpers.h>

namespace NYT::NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TScheduler, SplitTimeIntervalByHours)
{
    auto expected1 = std::vector<std::pair<TInstant, TInstant>>{{TInstant::Minutes(17), TInstant::Minutes(19)}};
    ASSERT_EQ(
        expected1,
        SplitTimeIntervalByHours(TInstant::Minutes(17), TInstant::Minutes(19)));

    auto expected2 = std::vector<std::pair<TInstant, TInstant>>{
        {TInstant::Minutes(17), TInstant::Minutes(60)},
        {TInstant::Minutes(60), TInstant::Minutes(69)}};
    ASSERT_EQ(
        expected2,
        SplitTimeIntervalByHours(TInstant::Minutes(17), TInstant::Minutes(69)));

    auto expected3 = std::vector<std::pair<TInstant, TInstant>>{
        {TInstant::Minutes(10), TInstant::Minutes(60)},
        {TInstant::Minutes(60), TInstant::Minutes(120)},
        {TInstant::Minutes(120), TInstant::Minutes(180)},
        {TInstant::Minutes(180), TInstant::Minutes(210)}};
    ASSERT_EQ(
        expected3,
        SplitTimeIntervalByHours(TInstant::Minutes(10), TInstant::Minutes(210)));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler

