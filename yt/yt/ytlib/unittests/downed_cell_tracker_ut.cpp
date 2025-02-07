#include <yt/yt/ytlib/hive/downed_cell_tracker.h>
#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NHiveClient {

namespace {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TDownedCellTrackerTest, BanUnbanByTimeout)
{
    auto config = New<TDownedCellTrackerConfig>();
    config->ChaosCellExpirationTime = TDuration::Seconds(10);
    auto downedCellTracker = New<TDownedCellTracker>(config);
    auto cellId1 = MakeRandomId(EObjectType::ChaosCell, TCellTag(1));
    auto cellId2 = MakeRandomId(EObjectType::ChaosCell, TCellTag(2));
    auto cellId3 = MakeRandomId(EObjectType::ChaosCell, TCellTag(3));

    std::vector<TCellId> coordinatorCellIds = {cellId1, cellId2, cellId3};

    ASSERT_TRUE(downedCellTracker->IsEmpty());
    for (auto cellId : coordinatorCellIds) {
        ASSERT_FALSE(downedCellTracker->IsDowned(cellId));
    }

    ASSERT_TRUE(IsIn(coordinatorCellIds, downedCellTracker->ChooseOne(coordinatorCellIds)));

    auto now = TInstant::Now();
    downedCellTracker->Update({}, {cellId1}, now);
    ASSERT_TRUE(downedCellTracker->IsDowned(cellId1));
    ASSERT_FALSE(downedCellTracker->IsEmpty());

    ASSERT_TRUE(EqualToOneOf(
        downedCellTracker->ChooseOne(coordinatorCellIds, now),
        cellId2,
        cellId3));

    now += TDuration::Seconds(2);
    downedCellTracker->Update({}, {cellId2}, now);
    ASSERT_EQ(downedCellTracker->ChooseOne(coordinatorCellIds, now), cellId3);

    now += TDuration::Seconds(2);
    downedCellTracker->Update({}, {cellId3}, now);
    ASSERT_EQ(downedCellTracker->ChooseOne(coordinatorCellIds, now), cellId1);
    ASSERT_TRUE(downedCellTracker->IsDowned(cellId1));

    now += TDuration::Seconds(7);
    ASSERT_EQ(downedCellTracker->ChooseOne(coordinatorCellIds, now), cellId1);
    ASSERT_FALSE(downedCellTracker->IsDowned(cellId1));

    now += TDuration::Seconds(20);
    ASSERT_TRUE(IsIn(coordinatorCellIds, downedCellTracker->ChooseOne(coordinatorCellIds, now)));
    ASSERT_TRUE(downedCellTracker->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHiveClient
