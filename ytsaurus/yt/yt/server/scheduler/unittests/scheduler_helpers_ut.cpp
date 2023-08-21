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
    
    auto expected4 = std::vector<std::pair<TInstant, TInstant>>{{TInstant::Minutes(10), TInstant::Minutes(60)}};
    ASSERT_EQ(
        expected4,
        SplitTimeIntervalByHours(TInstant::Minutes(10), TInstant::Minutes(60)));
    
    auto expected5 = std::vector<std::pair<TInstant, TInstant>>{
        {TInstant::Minutes(10), TInstant::Seconds(3600)},
        {TInstant::Seconds(3600), TInstant::Seconds(3601)}};
    ASSERT_EQ(
        expected5,
        SplitTimeIntervalByHours(TInstant::Minutes(10), TInstant::Seconds(3601)));
    
    auto expected6 = std::vector<std::pair<TInstant, TInstant>>{
        {TInstant::Seconds(3600), TInstant::Seconds(3601)}};
    ASSERT_EQ(
        expected6,
        SplitTimeIntervalByHours(TInstant::Seconds(3600), TInstant::Seconds(3601)));
    
    auto expected7 = std::vector<std::pair<TInstant, TInstant>>{
        {TInstant::Seconds(3600), TInstant::Seconds(7200)}};
    ASSERT_EQ(
        expected7,
        SplitTimeIntervalByHours(TInstant::Seconds(3600), TInstant::Seconds(7200)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler

