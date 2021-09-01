#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECategory,
    (Foo)
    (Bar)
    (Baz)
);

using TTracker = TMemoryUsageTracker<ECategory, TString>;
using TTrackerPtr = TIntrusivePtr<TTracker>;

////////////////////////////////////////////////////////////////////////////////

TTrackerPtr CreateTracker(i64 totalLimit, std::vector<i64> categoryLimits)
{
    YT_VERIFY(categoryLimits.size() == TEnumTraits<ECategory>::GetDomainNames().size());

    std::vector<std::pair<ECategory, i64>> limits;
    for (int i = 0; i < ssize(categoryLimits); ++i) {
        limits.emplace_back(
            TEnumTraits<ECategory>::GetDomainValues()[i],
            categoryLimits[i]);
    }

    return New<TTracker>(totalLimit, limits);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMemoryUsageTrackerTest, Basic)
{
    auto tracker = CreateTracker(10000, {1000, 2000, 7000});

    tracker->Acquire(ECategory::Foo, 500);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::Foo, 600).IsOK());
    EXPECT_TRUE(tracker->TryAcquire(ECategory::Foo, 200).IsOK());
    EXPECT_EQ(tracker->GetUsed(ECategory::Foo), 700);

    tracker->Acquire(ECategory::Bar, 9500);
    EXPECT_TRUE(tracker->IsTotalExceeded());
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Foo));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Bar));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Baz));

    EXPECT_FALSE(tracker->TryAcquire(ECategory::Foo, 1).IsOK());

    tracker->Release(ECategory::Bar, 500);
    EXPECT_FALSE(tracker->IsTotalExceeded());
    EXPECT_FALSE(tracker->IsExceeded(ECategory::Foo));
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Bar));
    EXPECT_FALSE(tracker->IsExceeded(ECategory::Baz));

    EXPECT_TRUE(tracker->TryAcquire(ECategory::Foo, 1).IsOK());
    EXPECT_TRUE(tracker->TryAcquire(ECategory::Baz, 1).IsOK());
}

TEST(TMemoryUsageTrackerTest, NoCategoryLimit)
{
    auto tracker = New<TTracker>(10000, std::vector<std::pair<ECategory, i64>>{});

    EXPECT_EQ(tracker->GetTotalLimit(), 10000);
    EXPECT_EQ(tracker->GetTotalUsed(), 0);
    EXPECT_EQ(tracker->GetTotalFree(), 10000);
    EXPECT_FALSE(tracker->IsTotalExceeded());

    EXPECT_EQ(tracker->GetLimit(ECategory::Foo), 10000);
    EXPECT_EQ(tracker->GetUsed(ECategory::Foo), 0);
    EXPECT_EQ(tracker->GetFree(ECategory::Foo), 10000);
    EXPECT_FALSE(tracker->IsExceeded(ECategory::Foo));

    tracker->Acquire(ECategory::Foo, 5000);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::Bar, 6000).IsOK());
    tracker->Acquire(ECategory::Bar, 6000);

    EXPECT_TRUE(tracker->IsTotalExceeded());
    EXPECT_TRUE(tracker->IsExceeded(ECategory::Foo));
    EXPECT_EQ(tracker->GetFree(ECategory::Foo), 0);
    EXPECT_EQ(tracker->GetTotalFree(), 0);
    EXPECT_EQ(tracker->GetTotalUsed(), 11000);
    EXPECT_EQ(tracker->GetUsed(ECategory::Foo), 5000);
}

TEST(TMemoryUsageTrackerTest, ForbidZeroTryAcquireWhenOvercommit)
{
    auto tracker = CreateTracker(3000, {1000, 1000, 1000});

    tracker->Acquire(ECategory::Foo, 1000);
    EXPECT_EQ(tracker->GetFree(ECategory::Foo), 0);
    EXPECT_TRUE(tracker->TryAcquire(ECategory::Foo, 0).IsOK());
    tracker->Acquire(ECategory::Foo, 1);
    EXPECT_EQ(tracker->GetFree(ECategory::Foo), 0);
    EXPECT_FALSE(tracker->TryAcquire(ECategory::Foo, 0).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
