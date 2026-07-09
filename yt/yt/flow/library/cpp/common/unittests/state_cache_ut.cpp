#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TFixedWeightValue
    : public IStateCacheValue
{
public:
    explicit TFixedWeightValue(i64 weight)
        : Weight_(weight)
    { }

    void Compress() override
    { }

    void Decompress() override
    { }

    i64 GetWeight() override
    {
        return Weight_;
    }

private:
    const i64 Weight_;
};

class TTestableStateCache
    : public TStateCache
{
public:
    using TStateCache::GetKeyWeight;
    using TStateCache::TStateCache;
};

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TTestableStateCache> MakeTestableCache(i64 uncompressed, i64 compressed = 0)
{
    auto spec = New<TDynamicStateCacheSpec>();
    spec->UncompressedCacheWeight = NYTree::TSize(uncompressed);
    spec->CompressedCacheWeight = NYTree::TSize(compressed);
    return New<TTestableStateCache>(spec, NProfiling::TProfiler{});
}

TEST(TStateCacheKeyWeightTest, EmptyKeyHasOnlyJobIdAndName)
{
    auto cache = MakeTestableCache(1 << 20);

    const auto jobId = TJobId(TGuid::Create());
    const std::string name = "stateName";
    const TStateCacheKey key{jobId, std::nullopt, name};

    EXPECT_EQ(cache->GetKeyWeight(key), static_cast<i64>(sizeof(TJobId) + name.size()));
}

TEST(TStateCacheKeyWeightTest, LargeKeyContributesToWeight)
{
    auto cache = MakeTestableCache(1 << 20);

    const auto jobId = TJobId(TGuid::Create());
    const std::string name = "n";
    const std::string smallStr = "x";
    const std::string largeStr(1024, 'z');

    const TStateCacheKey smallKey{jobId, MakeKey(TStringBuf(smallStr)), name};
    const TStateCacheKey largeKey{jobId, MakeKey(TStringBuf(largeStr)), name};

    const auto smallWeight = cache->GetKeyWeight(smallKey);
    const auto largeWeight = cache->GetKeyWeight(largeKey);

    EXPECT_GT(smallWeight, 0);
    EXPECT_GT(largeWeight, smallWeight);
    EXPECT_GE(largeWeight - smallWeight, static_cast<i64>(largeStr.size() - smallStr.size()));
}

TEST(TStateCacheKeyWeightTest, LargeKeysCauseEarlierEviction)
{
    // Keep the budget small so a few large keys exceed it.
    auto cache = MakeTestableCache(/*uncompressed*/ 4096);

    const auto jobId = TJobId(TGuid::Create());
    const std::string name = "tbl";
    const i64 valueWeight = 1;

    // Insert many entries with sizable string keys; each key alone consumes hundreds of bytes,
    // which forces the SLRU cache to evict older entries even though the values are tiny.
    constexpr int Count = 64;
    for (int i = 0; i < Count; ++i) {
        const std::string keyStr(512, static_cast<char>('a' + (i % 26)));
        auto key = MakeKey(TStringBuf(keyStr), static_cast<ui64>(i));
        cache->Insert(
            TStateCacheKey{jobId, key, name},
            New<TFixedWeightValue>(valueWeight));
    }

    // The first inserted entry must have been evicted because the per-key weight pushed us
    // over the 4 KiB budget well before the value weights (Count * 1 byte) ever could.
    auto firstKeyStr = std::string(512, 'a');
    auto firstKey = MakeKey(TStringBuf(firstKeyStr), static_cast<ui64>(0));
    EXPECT_FALSE(cache->Extract(TStateCacheKey{jobId, firstKey, name}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
