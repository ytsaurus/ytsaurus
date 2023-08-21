#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using ECategory = INodeMemoryTracker::ECategory;

const std::vector<ECategory> TestCategories = {
    ECategory::BlockCache,
    ECategory::Query,
    ECategory::P2P,
};

////////////////////////////////////////////////////////////////////////////////

INodeMemoryTrackerPtr CreateTracker(i64 totalLimit, std::vector<i64> categoryLimits)
{
    YT_VERIFY(categoryLimits.size() == TestCategories.size());

    std::vector<std::pair<ECategory, i64>> limits;
    for (int i = 0; i < ssize(categoryLimits); ++i) {
        limits.emplace_back(
            TestCategories[i],
            categoryLimits[i]);
    }

    return CreateNodeMemoryTracker(totalLimit, limits);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMemoryUsageTrackerTest, Basic)
{
    auto tracker = CreateTracker(10000, {1000, 2000, 7000});

    tracker->Acquire(ECategory::BlockCache, 500);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::BlockCache, 600).IsOK());
    EXPECT_TRUE(tracker->TryAcquire(ECategory::BlockCache, 200).IsOK());
    EXPECT_EQ(tracker->GetUsed(ECategory::BlockCache), 700);

    tracker->Acquire(ECategory::Query, 9500);
    EXPECT_TRUE(tracker->IsTotalExceeded());
    EXPECT_TRUE(tracker->IsExceeded(ECategory::BlockCache));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Query));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::P2P));

    EXPECT_FALSE(tracker->TryAcquire(ECategory::BlockCache, 1).IsOK());

    tracker->Release(ECategory::Query, 500);
    EXPECT_FALSE(tracker->IsTotalExceeded());
    EXPECT_FALSE(tracker->IsExceeded(ECategory::BlockCache));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Query));
    EXPECT_FALSE(tracker->IsExceeded(ECategory::P2P));

    EXPECT_TRUE(tracker->TryAcquire(ECategory::BlockCache, 1).IsOK());
    EXPECT_TRUE(tracker->TryAcquire(ECategory::P2P, 1).IsOK());
}

TEST(TMemoryUsageTrackerTest, NoCategoryLimit)
{
    auto tracker = CreateNodeMemoryTracker(10000, std::vector<std::pair<ECategory, i64>>{});

    EXPECT_EQ(tracker->GetTotalLimit(), 10000);
    EXPECT_EQ(tracker->GetTotalUsed(), 0);
    EXPECT_EQ(tracker->GetTotalFree(), 10000);
    EXPECT_FALSE(tracker->IsTotalExceeded());

    EXPECT_EQ(tracker->GetLimit(ECategory::BlockCache), 10000);
    EXPECT_EQ(tracker->GetUsed(ECategory::BlockCache), 0);
    EXPECT_EQ(tracker->GetFree(ECategory::BlockCache), 10000);
    EXPECT_FALSE(tracker->IsExceeded(ECategory::BlockCache));

    tracker->Acquire(ECategory::BlockCache, 5000);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::Query, 6000).IsOK());
    tracker->Acquire(ECategory::Query, 6000);

    EXPECT_TRUE(tracker->IsTotalExceeded());
    EXPECT_TRUE(tracker->IsExceeded(ECategory::BlockCache));
    EXPECT_EQ(tracker->GetFree(ECategory::BlockCache), 0);
    EXPECT_EQ(tracker->GetTotalFree(), 0);
    EXPECT_EQ(tracker->GetTotalUsed(), 11000);
    EXPECT_EQ(tracker->GetUsed(ECategory::BlockCache), 5000);
}

TEST(TMemoryUsageTrackerTest, ForbidZeroTryAcquireWhenOvercommit)
{
    auto tracker = CreateTracker(3000, {1000, 1000, 1000});

    tracker->Acquire(ECategory::BlockCache, 1000);
    EXPECT_EQ(tracker->GetFree(ECategory::BlockCache), 0);
    EXPECT_TRUE(tracker->TryAcquire(ECategory::BlockCache, 0).IsOK());
    tracker->Acquire(ECategory::BlockCache, 1);
    EXPECT_EQ(tracker->GetFree(ECategory::BlockCache), 0);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::BlockCache, 0).IsOK());
}

TEST(TMemoryUsageTrackerTest, LimitlessCategory)
{
    auto tracker = CreateTracker(3000, {1000, 1000, std::numeric_limits<i64>::max()});

    EXPECT_EQ(tracker->GetTotalLimit(), 3000);
    EXPECT_EQ(tracker->GetExplicitLimit(ECategory::P2P), std::numeric_limits<i64>::max());
    EXPECT_EQ(tracker->GetLimit(ECategory::P2P), 3000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
