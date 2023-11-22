#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <util/random/fast.h>

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

DECLARE_REFCOUNTED_STRUCT(TTestValue)

struct TTestValue
    : TSyncCacheValueBase<TString, TTestValue>
{
public:
    using TSyncCacheValueBase::TSyncCacheValueBase;

    i64 Weight = 1;
};

DEFINE_REFCOUNTED_TYPE(TTestValue)

class TTestMemoryTrackingCache
    : public TMemoryTrackingSyncSlruCacheBase<TString, TTestValue>
{
public:
    explicit TTestMemoryTrackingCache(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker)
        : TMemoryTrackingSyncSlruCacheBase(config, memoryTracker)
    { }

    virtual i64 GetWeight(const TTestValuePtr& value) const
    {
        return value->Weight;
    }
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

TSharedRef CreateRandomReference(TFastRng64& rnd, i64 size)
{
    TString s;
    s.resize(size, '*');

    for (i64 index = 0; index < size; ++index) {
        s[index] = (char)rnd.GenRand64();
    }

    auto output = TSharedRef::FromString(s);
    YT_ASSERT(static_cast<i64>(output.Size()) == size);
    return output;
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
    tracker->ClearTrackers();
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
    tracker->ClearTrackers();
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
    tracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, LimitlessCategory)
{
    auto tracker = CreateTracker(3000, {1000, 1000, std::numeric_limits<i64>::max()});

    EXPECT_EQ(tracker->GetTotalLimit(), 3000);
    EXPECT_EQ(tracker->GetExplicitLimit(ECategory::P2P), std::numeric_limits<i64>::max());
    EXPECT_EQ(tracker->GetLimit(ECategory::P2P), 3000);

    tracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, EntryWeightUpdate)
{
    TFastRng64 rng(27);
    auto tracker = CreateTracker(3000, {1000, 1000, 1000});

    auto config = New<TSlruCacheConfig>();
    config->Capacity = 1000;

    auto cache = New<TTestMemoryTrackingCache>(config, tracker->WithCategory(EMemoryCategory::BlockCache));
    for (; cache->GetSize() < 990;) {
        cache->TryInsert(New<TTestValue>(TString(CreateRandomReference(rng,  256).ToStringBuf())));
    }

    EXPECT_GE(990, cache->GetSize());
    EXPECT_GE(cache->GetSize() * 1, tracker->WithCategory(EMemoryCategory::BlockCache)->GetUsed());

    for (auto& value : cache->GetAll()) {
        value->Weight *= 2;
        cache->UpdateWeight(value);
    }

    EXPECT_GE(500, cache->GetSize());
    EXPECT_GE(cache->GetSize() * 2, tracker->WithCategory(EMemoryCategory::BlockCache)->GetUsed());

    for (auto& value : cache->GetAll()) {
        value->Weight *= 2;
        cache->UpdateWeight(value);
    }

    EXPECT_GE(250, cache->GetSize());
    EXPECT_GE(cache->GetSize() * 4, tracker->WithCategory(EMemoryCategory::BlockCache)->GetUsed());
    tracker->ClearTrackers();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
