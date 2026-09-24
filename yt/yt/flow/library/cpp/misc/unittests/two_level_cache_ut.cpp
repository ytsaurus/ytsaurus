#include <yt/yt/flow/library/cpp/misc/two_level_cache.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>

namespace NYT::NFlow::NCache {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNonStandardCacheError
{ };

class TCacheTestValue
    : public TRefCounted
{
public:
    bool FailCompression = false;
    bool FailNonStandardCompression = false;
    bool FailCompressedWeight = false;
    bool Compressed = false;
    int CompressCount = 0;
    int DecompressCount = 0;

    void Compress()
    {
        if (FailNonStandardCompression) {
            throw TNonStandardCacheError{};
        }
        if (FailCompression) {
            throw std::runtime_error("Compression failed");
        }
        ++CompressCount;
        Compressed = true;
    }

    void Decompress()
    {
        ++DecompressCount;
        Compressed = false;
    }

    i64 GetWeight() const
    {
        if (Compressed && FailCompressedWeight) {
            throw std::runtime_error("Compressed weight failed");
        }
        return Compressed ? 1 : 1_KB;
    }
};

class TTestTwoLevelCache
    : public TTwoLevelCache<int, TCacheTestValue>
{
public:
    TTestTwoLevelCache()
        : TTwoLevelCache(NProfiling::TProfiler{})
    { }

protected:
    i64 GetKeyWeight(const int& /*key*/) const override
    {
        return 0;
    }
};

static_assert(noexcept(std::declval<TTestTwoLevelCache::TCache&>().OnRemoved(
    std::declval<const TTestTwoLevelCache::TItemPtr&>())));

////////////////////////////////////////////////////////////////////////////////

TEST(TTwoLevelCacheTest, InsertAndExtract)
{
    auto cache = New<TTestTwoLevelCache>();
    cache->Reconfigure(/*capacity*/ 1_MB, /*compressedCapacity*/ 1_MB);
    auto value = New<TCacheTestValue>();
    cache->Insert(/*key*/ 0, value);
    EXPECT_EQ(cache->Extract(/*key*/ 0), value);
    EXPECT_EQ(value->CompressCount, 0);
    EXPECT_EQ(value->DecompressCount, 0);

    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
    cache->Insert(/*key*/ 0, value);
    EXPECT_EQ(value->CompressCount, 1);
    EXPECT_EQ(cache->Extract(/*key*/ 0), value);
    EXPECT_EQ(value->DecompressCount, 1);
    EXPECT_FALSE(cache->Extract(/*key*/ 0));
}

TEST(TTwoLevelCacheTest, ReconfigureCompressesValues)
{
    auto cache = New<TTestTwoLevelCache>();
    cache->Reconfigure(/*capacity*/ 1_MB, /*compressedCapacity*/ 1_MB);
    auto value = New<TCacheTestValue>();
    cache->Insert(/*key*/ 0, value);
    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
    EXPECT_EQ(value->CompressCount, 1);
    EXPECT_EQ(cache->Extract(/*key*/ 0), value);
    EXPECT_EQ(value->DecompressCount, 1);
}

TEST(TTwoLevelCacheTest, WeightErrorOutsideEvictionPropagates)
{
    auto config = TSlruCacheConfig::CreateWithCapacity(/*capacity*/ 1_MB, /*shardCount*/ 1);
    auto cache = New<TTestTwoLevelCache::TCache>(
        config,
        /*nextCache*/ nullptr,
        TWeakPtr<TTestTwoLevelCache>(),
        NProfiling::TProfiler{});
    auto value = New<TCacheTestValue>();
    value->Compressed = true;
    auto item = New<TTestTwoLevelCache::TItem>(/*key*/ 0, value);
    auto cookie = cache->BeginInsert(/*key*/ 0);
    cookie.EndInsert(item);

    value->FailCompressedWeight = true;
    EXPECT_THROW_WITH_SUBSTRING(item->UpdateWeight(), "Compressed weight failed");
    EXPECT_EQ(cache->Find(/*key*/ 0), item);

    value->FailCompressedWeight = false;
    EXPECT_NO_THROW(item->UpdateWeight());
    cache->TryRemove(/*key*/ 0);
    EXPECT_FALSE(cache->Find(/*key*/ 0));
}

void ExpectFatalEviction(bool failCompressedWeight, bool nonStandardException = false)
{
    for (bool reconfigure : {false, true}) {
        SCOPED_TRACE(reconfigure);
        auto cache = New<TTestTwoLevelCache>();
        cache->Reconfigure(/*capacity*/ reconfigure ? 1_MB : 0, /*compressedCapacity*/ 1_MB);
        auto value = New<TCacheTestValue>();
        value->FailCompression = !failCompressedWeight && !nonStandardException;
        value->FailNonStandardCompression = nonStandardException;
        value->FailCompressedWeight = failCompressedWeight;
        if (reconfigure) {
            cache->Insert(/*key*/ 0, value);
        }

        const std::string expectedError = nonStandardException
            ? "unknown error"
            : (failCompressedWeight ? "Compressed weight failed" : "Compression failed");
        EXPECT_DEATH({
            // Require the diagnostic abort, not implicit termination or ordinary propagation.
            std::set_terminate([] {
                std::_Exit(42);
            });
            try {
                if (reconfigure) {
                    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
                } else {
                    cache->Insert(/*key*/ 0, value);
                }
            } catch (...) {
                std::_Exit(1);
            } }, "YT_ABORT.*Exception in cache eviction callback.*" + expectedError);
    }
}

TEST(TTwoLevelCacheDeathTest, CompressionErrorAbortsWithDiagnostics)
{
    ExpectFatalEviction(/*failCompressedWeight*/ false);
}

TEST(TTwoLevelCacheDeathTest, NestedInsertionErrorAbortsWithDiagnostics)
{
    ExpectFatalEviction(/*failCompressedWeight*/ true);
}

TEST(TTwoLevelCacheDeathTest, NonStandardErrorAbortsWithDiagnostics)
{
    ExpectFatalEviction(/*failCompressedWeight*/ false, /*nonStandardException*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCache
