#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <atomic>
#include <memory>
#include <random>
#include <thread>

namespace {

////////////////////////////////////////////////////////////////////////////////

using TIntCache = TInsertOnlyConcurrentCache<int, int>;
using TStringCache = TInsertOnlyConcurrentCache<std::string, std::string>;

////////////////////////////////////////////////////////////////////////////////

TEST(TInsertOnlyConcurrentCacheTest, Empty)
{
    auto cache = std::make_unique<TIntCache>();
    EXPECT_EQ(cache->FindPtr(42), nullptr);
}

TEST(TInsertOnlyConcurrentCacheTest, InsertAndFind)
{
    auto cache = std::make_unique<TIntCache>();

    const int& v = cache->FindOrInsert(1, [] {
        return 100;
    });
    EXPECT_EQ(v, 100);

    const int* p = cache->FindPtr(1);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(*p, 100);

    // Same address — value lives in the table.
    EXPECT_EQ(&v, p);
}

TEST(TInsertOnlyConcurrentCacheTest, FindOrInsertReturnsExisting)
{
    auto cache = std::make_unique<TIntCache>();

    cache->FindOrInsert(7, [] {
        return 77;
    });

    const int& v = cache->FindOrInsert(7, [] {
        return 999;
    });
    EXPECT_EQ(v, 77);
}

TEST(TInsertOnlyConcurrentCacheTest, MultipleKeys)
{
    auto cache = std::make_unique<TIntCache>();

    for (int i = 0; i < 100; ++i) {
        const int& v = cache->FindOrInsert(i, [i] {
            return i * 10;
        });
        EXPECT_EQ(v, i * 10);
    }

    for (int i = 0; i < 100; ++i) {
        const int* p = cache->FindPtr(i);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(*p, i * 10);
    }

    EXPECT_EQ(cache->FindPtr(100), nullptr);
}

TEST(TInsertOnlyConcurrentCacheTest, StringKeys)
{
    auto cache = std::make_unique<TStringCache>();

    cache->FindOrInsert(std::string("hello"), [] {
        return std::string("world");
    });
    cache->FindOrInsert(std::string("foo"), [] {
        return std::string("bar");
    });

    const std::string* v = cache->FindPtr(std::string("hello"));
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, "world");

    const std::string* v2 = cache->FindPtr(std::string("foo"));
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(*v2, "bar");

    EXPECT_EQ(cache->FindPtr(std::string("missing")), nullptr);
}

TEST(TInsertOnlyConcurrentCacheTest, TransparentLookup)
{
    // TInsertionKey = std::string_view, TKey = std::string.
    // Verify that FindPtr and FindOrInsert accept std::string_view without copying.
    using TCache = TInsertOnlyConcurrentCache<std::string, int>;
    auto cache = std::make_unique<TCache>();

    cache->FindOrInsert(std::string("key"), [] {
        return 42;
    });

    // Lookup with string_view — no allocation.
    std::string_view sv = "key";
    const int* p = cache->FindPtr(sv);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(*p, 42);

    // FindOrInsert with string_view — converts to std::string only on slow path.
    const int& v = cache->FindOrInsert(sv, [] {
        return 99;
    });
    EXPECT_EQ(v, 42); // Already present.
}

TEST(TInsertOnlyConcurrentCacheTest, RehashPreservesValues)
{
    // Insert enough elements to trigger multiple rehashes.
    auto cache = std::make_unique<TIntCache>();
    constexpr int N = 1000;

    for (int i = 0; i < N; ++i) {
        const int& v = cache->FindOrInsert(i, [i] {
            return i * 3;
        });
        EXPECT_EQ(v, i * 3);
    }

    for (int i = 0; i < N; ++i) {
        const int* p = cache->FindPtr(i);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(*p, i * 3);
    }
}

TEST(TInsertOnlyConcurrentCacheTest, PointerStability)
{
    // References returned by FindOrInsert must remain valid after rehash.
    auto cache = std::make_unique<TIntCache>();
    constexpr int N = 200;

    std::vector<const int*> ptrs(N);
    for (int i = 0; i < N; ++i) {
        ptrs[i] = &cache->FindOrInsert(i, [i] {
            return i;
        });
        ASSERT_NE(ptrs[i], nullptr);
    }

    // Insert more to force rehash.
    for (int i = N; i < N * 4; ++i) {
        cache->FindOrInsert(i, [i] {
            return i;
        });
    }

    // Original pointers must still be valid and correct.
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(*ptrs[i], i);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInsertOnlyConcurrentCacheTest, StressSinglethread)
{
    // 100 rounds × 50 random FindOrInsert operations over keys [0, 100).
    // After each round verify that cache and THashMap contain exactly the same keys/values.
    constexpr int NumRounds = 100;
    constexpr int OpsPerRound = 50;
    constexpr int KeyRange = 100;

    auto cache = std::make_unique<TIntCache>();
    THashMap<int, int> reference;

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> keyDist(0, KeyRange - 1);

    for (int round = 0; round < NumRounds; ++round) {
        for (int op = 0; op < OpsPerRound; ++op) {
            int key = keyDist(rng);
            int value = key * 13 + 1;
            cache->FindOrInsert(key, [value] {
                return value;
            });
            reference.emplace(key, value); // no-op if already present
        }

        // Verify: every key in [0, KeyRange) must agree between cache and reference.
        for (int k = 0; k < KeyRange; ++k) {
            const int* cacheVal = cache->FindPtr(k);
            auto it = reference.find(k);
            if (it == reference.end()) {
                ASSERT_FALSE(cacheVal);
            } else {
                ASSERT_TRUE(cacheVal);
                ASSERT_EQ(*cacheVal, it->second);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Stress test: compare TInsertOnlyConcurrentCache with TConcurrentHashMap.
TEST(TInsertOnlyConcurrentCacheTest, StressMultithread)
{
    constexpr int NumThreads = 8;
    constexpr int OpsPerThread = 5000;
    constexpr int KeyRange = 200;
    constexpr int WaitSeconds = 3;

    auto cache = std::make_unique<TIntCache>();
    TConcurrentHashMap<int, int> reference;

    std::atomic<bool> done = false;
    std::atomic<int> errors = 0;

    // Pre-populate both with the same keys so we can verify reads.
    for (int i = 0; i < KeyRange / 2; ++i) {
        cache->FindOrInsert(i, [i] {
            return i * 7;
        });
        reference.InsertIfAbsent(i, i * 7);
    }

    auto threadFunc = [&] (int threadId) {
        std::mt19937 rng(threadId * 1234567 + 42);
        std::uniform_int_distribution<int> keyDist(0, KeyRange - 1);
        std::uniform_int_distribution<int> opDist(0, 1);

        for (int op = 0; op < OpsPerThread && !done; ++op) {
            int key = keyDist(rng);
            int opType = opDist(rng);

            if (opType == 0) {
                // FindOrInsert.
                const int& v = cache->FindOrInsert(key, [key] {
                    return key * 7;
                });
                if (v != key * 7) {
                    ++errors;
                }
                // Also insert into reference.
                reference.InsertIfAbsent(key, key * 7);
            } else {
                // Find — must agree with reference if key was inserted.
                const int* v = cache->FindPtr(key);
                auto& bucket = reference.GetBucketForKey(key);
                auto guard = Guard(bucket.GetMutex());
                const int* refV = bucket.TryGetUnsafe(key);
                if (refV != nullptr && v != nullptr && *v != *refV) {
                    ++errors;
                }
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(NumThreads);
    for (int i = 0; i < NumThreads; ++i) {
        threads.emplace_back(threadFunc, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(WaitSeconds));
    done = true;

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(errors.load(), 0);

    // Final consistency check: every key in reference must be in cache with correct value.
    for (int i = 0; i < KeyRange; ++i) {
        auto& bucket = reference.GetBucketForKey(i);
        auto guard = Guard(bucket.GetMutex());
        const int* refV = bucket.TryGetUnsafe(i);
        if (refV) {
            const int* v = cache->FindPtr(i);
            ASSERT_NE(v, nullptr) << "Key " << i << " missing from cache";
            EXPECT_EQ(*v, *refV) << "Key " << i << " has wrong value";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
