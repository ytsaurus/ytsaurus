#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/strategy/policy/scheduling_segment_manager.h>

#include <utility>

namespace NYT::NScheduler::NStrategy::NPolicy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TNodeSchedulingSegmentChangeTest, ApplyToSnapshotRegistration)
{
    auto node = New<TNodeState>();
    auto snapshot = New<TNodeState>(*node);
    auto options = TSetNodeSchedulingSegmentOptions{
        .OldSegment = ESchedulingSegment::Default,
        .NewSegment = ESchedulingSegment::LargeGpu,
        .NodeRegistrationId = snapshot->RegistrationId,
    };

    EXPECT_EQ(node->RegistrationId, snapshot->RegistrationId);
    EXPECT_TRUE(TryApplyNodeSchedulingSegmentChange(node.Get(), options));
    EXPECT_EQ(ESchedulingSegment::LargeGpu, node->SchedulingSegment);
    EXPECT_TRUE(node->ForceRunningAllocationStatisticsUpdate);
}

class TNodeSchedulingSegmentChangeReregistrationTest
    : public testing::TestWithParam<std::pair<ESchedulingSegment, ESchedulingSegment>>
{ };

TEST_P(TNodeSchedulingSegmentChangeReregistrationTest, SkipNewRegistration)
{
    const auto& [oldSegment, newSegment] = GetParam();
    auto oldNode = New<TNodeState>();
    oldNode->SchedulingSegment = oldSegment;
    auto snapshot = New<TNodeState>(*oldNode);
    auto options = TSetNodeSchedulingSegmentOptions{
        .OldSegment = snapshot->SchedulingSegment,
        .NewSegment = newSegment,
        .NodeRegistrationId = snapshot->RegistrationId,
    };
    auto newNode = New<TNodeState>();

    EXPECT_FALSE(TryApplyNodeSchedulingSegmentChange(newNode.Get(), options));
    EXPECT_EQ(ESchedulingSegment::Default, newNode->SchedulingSegment);
    EXPECT_FALSE(newNode->ForceRunningAllocationStatisticsUpdate);
}

INSTANTIATE_TEST_SUITE_P(
    SegmentPairs,
    TNodeSchedulingSegmentChangeReregistrationTest,
    testing::Values(
        std::pair(ESchedulingSegment::LargeGpu, ESchedulingSegment::Default),
        std::pair(ESchedulingSegment::Default, ESchedulingSegment::LargeGpu)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NStrategy::NPolicy
