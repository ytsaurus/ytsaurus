#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/misc/destruction_context.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <util/system/event.h>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <utility>

namespace NYT::NFlow {

struct TCacheTestKey
{
    std::shared_ptr<int> Identity = std::make_shared<int>(0);

    bool operator==(const TCacheTestKey&) const = default;
};

} // namespace NYT::NFlow

template <>
struct THash<NYT::NFlow::TCacheTestKey>
{
    size_t operator()(const NYT::NFlow::TCacheTestKey& /*key*/) const
    {
        return 0;
    }
};

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

class TMutableWeightValue
    : public IStateCacheValue
{
public:
    explicit TMutableWeightValue(i64 weight)
        : Weight(weight)
    { }

    void Compress() override
    { }

    void Decompress() override
    { }

    i64 GetWeight() override
    {
        return Weight;
    }

    i64 Weight;
};

class TReclamation
    : public TRefCounted
{
public:
    explicit TReclamation(std::function<void()> onReclaimed)
        : OnReclaimed_(std::move(onReclaimed))
    { }

    ~TReclamation() override
    {
        OnReclaimed_();
    }

private:
    const std::function<void()> OnReclaimed_;
};

class TReclaimableValue
    : public IStateCacheValue
{
public:
    int CompressCount = 0;
    int DecompressCount = 0;

    TReclaimableValue(
        std::function<void()> onReclaimed,
        std::function<void()> onCompressed = {},
        std::function<void()> onDestroyed = {})
        : OnCompressed_(std::move(onCompressed))
        , OnDestroyed_(std::move(onDestroyed))
        , Garbage_(New<TReclamation>(std::move(onReclaimed)))
    { }

    ~TReclaimableValue() override
    {
        if (OnDestroyed_) {
            OnDestroyed_();
        }
    }

    void Compress() override
    {
        ++CompressCount;
        TDestructionContextGuard::Add(std::exchange(Garbage_, {}));
        if (OnCompressed_) {
            OnCompressed_();
        }
    }

    void Decompress() override
    {
        ++DecompressCount;
    }

    i64 GetWeight() override
    {
        return Garbage_ ? 1_KB : 1;
    }

private:
    const std::function<void()> OnCompressed_;
    const std::function<void()> OnDestroyed_;

    TRefCountedPtr Garbage_;
};

class TCompressibleValue
    : public TRefCounted
{
public:
    int CompressCount = 0;
    int DecompressCount = 0;

    void Compress()
    {
        ++CompressCount;
    }

    void Decompress()
    {
        ++DecompressCount;
    }

    i64 GetWeight()
    {
        return CompressCount == 0 ? 1_KB : 1;
    }
};

class TReclamationTwoLevelCache
    : public NCache::TTwoLevelCache<TCacheTestKey, TReclaimableValue>
{
public:
    TReclamationTwoLevelCache()
        : TTwoLevelCache(NProfiling::TProfiler{})
    { }

protected:
    i64 GetKeyWeight(const TCacheTestKey& /*key*/) const override
    {
        return 0;
    }
};

class TTestTwoLevelCache
    : public NCache::TTwoLevelCache<int, TCompressibleValue>
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

TEST(TStateCacheReclamationTest, ReclaimsBeforeEachInsertReturns)
{
    for (bool keepCompressed : {false, true}) {
        SCOPED_TRACE(keepCompressed);
        std::atomic<int> reclaimedCount = 0;
        auto cache = MakeTestableCache(/*uncompressed*/ 0, keepCompressed ? 1_MB : 0);
        auto namedCache = cache->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})->WithName("state");
        std::vector<TIntrusivePtr<TReclaimableValue>> values;

        constexpr int Count = 128;
        for (int index = 0; index < Count; ++index) {
            auto value = New<TReclaimableValue>([&] {
                ++reclaimedCount;
            });
            namedCache->Insert(MakeKey(static_cast<ui64>(index)), value);
            EXPECT_EQ(reclaimedCount.load(), index + 1);
            EXPECT_EQ(value->CompressCount, 1);
            values.push_back(std::move(value));
        }

        for (int index = 0; index < Count; ++index) {
            auto extracted = namedCache->Extract(MakeKey(static_cast<ui64>(index)));
            if (keepCompressed) {
                EXPECT_EQ(extracted.Get(), values[index].Get());
                EXPECT_EQ(values[index]->DecompressCount, 1);
            } else {
                EXPECT_FALSE(extracted);
                EXPECT_EQ(values[index]->DecompressCount, 0);
            }
        }
    }
}

TEST(TStateCacheReclamationTest, ReclaimsBeforeReconfigureReturns)
{
    std::atomic<int> reclaimedCount = 0;
    auto cache = MakeTestableCache(1_MB, 1_MB);
    auto namedCache = cache->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})->WithName("state");

    constexpr int Count = 64;
    for (int index = 0; index < Count; ++index) {
        namedCache->Insert(MakeKey(static_cast<ui64>(index)), New<TReclaimableValue>([&] {
            ++reclaimedCount;
        }));
    }
    EXPECT_EQ(reclaimedCount.load(), 0);

    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
    EXPECT_EQ(reclaimedCount.load(), Count);

    for (int index = 0; index < Count; ++index) {
        EXPECT_TRUE(namedCache->Extract(MakeKey(static_cast<ui64>(index))));
    }
}

TEST(TStateCacheReclamationTest, ClearingCompressedCacheReleasesKeys)
{
    auto cache = New<TReclamationTwoLevelCache>();
    const int shardCount = New<TSlruCacheConfig>()->ShardCount;
    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 2 * shardCount);
    std::vector<std::weak_ptr<int>> keys;
    for (int index = 0; index < 64; ++index) {
        TCacheTestKey key;
        keys.emplace_back(key.Identity);
        cache->Insert(key, New<TReclaimableValue>([] {
        }));
    }

    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 0);
    for (const auto& key : keys) {
        EXPECT_TRUE(key.expired());
    }
}

TEST(TStateCacheWeightTest, MutableCompressedValueDoesNotCorruptCapacity)
{
    for (bool grow : {false, true}) {
        SCOPED_TRACE(grow);
        auto cache = MakeTestableCache(/*uncompressed*/ 0, /*compressed*/ 1_MB);
        const auto jobId = TJobId(TGuid::Create());
        const TStateCacheKey key{jobId, MakeKey(ui64{0}), "state"};
        TStateCacheKey nextKey = key;
        const int shardCount = New<TSlruCacheConfig>()->ShardCount;
        for (int index = 1;; ++index) {
            nextKey = TStateCacheKey{jobId, MakeKey(static_cast<ui64>(index)), "state"};
            if (THash<TStateCacheKey>()(nextKey) % shardCount == THash<TStateCacheKey>()(key) % shardCount) {
                break;
            }
        }

        const i64 initialWeight = grow ? 1_KB : 10_KB;
        auto value = New<TMutableWeightValue>(initialWeight);
        cache->Insert(key, value);
        // A retained provider may update shared state without extracting its cache entry.
        value->Weight = grow ? 10_KB : 1_KB;
        cache->Insert(key, value);

        cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 0);
        EXPECT_FALSE(cache->Extract(key));
        if (!grow) {
            cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 64_KB);
        }

        auto nextValue = New<TMutableWeightValue>(/*weight*/ 2_KB);
        cache->Insert(nextKey, nextValue);
        if (grow) {
            EXPECT_FALSE(cache->Extract(nextKey));
        } else {
            EXPECT_EQ(cache->Extract(nextKey), nextValue);
        }
    }
}

TEST(TStateCacheReclamationTest, ExtractWaitsForConcurrentCompression)
{
    TManualEvent compressionStarted;
    TManualEvent resumeCompression;
    TManualEvent extractionStarted;
    TManualEvent extractionFinished;
    auto cache = MakeTestableCache(/*uncompressed*/ 1_MB, /*compressed*/ 1_MB);
    const TStateCacheKey key{TJobId(TGuid::Create()), MakeKey(ui64{0}), "state"};
    auto value = New<TReclaimableValue>([] {
    },
        [&] {
            compressionStarted.Signal();
            resumeCompression.WaitI();
        });
    cache->Insert(key, value);

    std::thread producer([&] {
        cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
    });
    std::thread extractor;
    IStateCacheValuePtr extracted;
    auto cleanup = Finally([&] {
        resumeCompression.Signal();
        if (producer.joinable()) {
            producer.join();
        }
        if (extractor.joinable()) {
            extractor.join();
        }
    });

    ASSERT_TRUE(compressionStarted.WaitT(TDuration::Seconds(5)));
    extractor = std::thread([&] {
        extractionStarted.Signal();
        extracted = cache->Extract(key);
        extractionFinished.Signal();
    });
    ASSERT_TRUE(extractionStarted.WaitT(TDuration::Seconds(5)));
    EXPECT_FALSE(extractionFinished.WaitT(TDuration::MilliSeconds(100)));
    resumeCompression.Signal();
    ASSERT_TRUE(extractionFinished.WaitT(TDuration::Seconds(5)));
    producer.join();
    extractor.join();
    EXPECT_EQ(extracted.Get(), value.Get());
    EXPECT_EQ(value->CompressCount, 1);
    EXPECT_EQ(value->DecompressCount, 1);
    EXPECT_FALSE(cache->Extract(key));
}

TEST(TStateCacheMetricsTest, CompressedWeightSizeAndRequestsKeepTheirNames)
{
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    NProfiling::TProfiler profiler(registry, "/test/state_cache");
    auto spec = New<TDynamicStateCacheSpec>();
    spec->UncompressedCacheWeight = NYTree::TSize(0);
    spec->CompressedCacheWeight = NYTree::TSize(1_MB);
    auto cache = New<TTestableStateCache>(spec, profiler);
    auto collect = [&] {
        registry->ProcessRegistrations();
        registry->Collect();
        return registry->DumpSensors();
    };
    collect();
    const TStateCacheKey key{TJobId(TGuid::Create()), MakeKey(ui64{0}), "state"};
    auto value = New<TReclaimableValue>([] {
    });
    cache->Insert(key, value);
    const i64 expectedWeight = cache->GetKeyWeight(key) + value->GetWeight();

    auto dump = collect();
    auto readMetric = [&] (TStringBuf name) -> std::optional<double> {
        for (const auto& cube : dump.cubes()) {
            if (cube.name() != name) {
                continue;
            }
            for (const auto& projection : cube.projections()) {
                bool compressed = false;
                bool hitType = false;
                for (i64 id : projection.tag_ids()) {
                    const auto& tag = dump.tags(id);
                    compressed |= tag.key() == "cache" && tag.value() == "compressed";
                    hitType |= tag.key() == "hit_type";
                }
                if (compressed && !hitType) {
                    if (projection.has_gauge()) {
                        return projection.gauge();
                    }
                    if (projection.has_counter()) {
                        return projection.counter();
                    }
                }
            }
        }
        return std::nullopt;
    };

    EXPECT_EQ(readMetric("yt/test/state_cache/weight"), expectedWeight);
    EXPECT_EQ(readMetric("yt/test/state_cache/size"), 1);
    EXPECT_EQ(readMetric("yt/test/state_cache/missed_count"), 1);
    EXPECT_EQ(readMetric("yt/test/state_cache/missed_weight"), expectedWeight);
    EXPECT_EQ(readMetric("yt/test/state_cache/hit_count"), 0);
    EXPECT_EQ(cache->Extract(key).Get(), value.Get());
    dump = collect();
    EXPECT_EQ(readMetric("yt/test/state_cache/weight"), 0);
    EXPECT_EQ(readMetric("yt/test/state_cache/size"), 0);
    EXPECT_EQ(readMetric("yt/test/state_cache/hit_count"), 1);
    EXPECT_EQ(readMetric("yt/test/state_cache/hit_weight"), expectedWeight);
    EXPECT_FALSE(cache->Extract(key));
    dump = collect();
    EXPECT_EQ(readMetric("yt/test/state_cache/missed_count"), 1);
}

TEST(TStateCacheReclamationTest, BlockingDestructorDoesNotHoldCacheLock)
{
    for (bool reconfigure : {false, true}) {
        SCOPED_TRACE(reconfigure);
        TManualEvent reclamationStarted;
        TManualEvent resumeReclamation;
        TManualEvent producerFinished;
        TManualEvent probeFinished;
        auto cache = MakeTestableCache(reconfigure ? 1_MB : 0, 1_MB);
        const auto jobId = TJobId(TGuid::Create());
        const TStateCacheKey key{jobId, MakeKey(static_cast<ui64>(0)), "state"};
        auto probeKey = key;
        const auto shardMask = New<TSlruCacheConfig>()->ShardCount - 1;
        for (ui64 index = 1;; ++index) {
            probeKey = TStateCacheKey{jobId, MakeKey(index), "state"};
            if ((THash<TStateCacheKey>()(probeKey) & shardMask) == (THash<TStateCacheKey>()(key) & shardMask)) {
                break;
            }
        }

        auto value = New<TReclaimableValue>([&] {
            reclamationStarted.Signal();
            resumeReclamation.WaitI();
        });
        if (reconfigure) {
            cache->Insert(key, value);
        }

        std::thread producer([&] {
            if (reconfigure) {
                cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
            } else {
                cache->Insert(key, value);
            }
            producerFinished.Signal();
        });
        std::thread probe;
        auto cleanup = Finally([&] {
            // Release the destructor and join both threads even if an assertion fails.
            resumeReclamation.Signal();
            producer.join();
            if (probe.joinable()) {
                probe.join();
            }
        });

        ASSERT_TRUE(reclamationStarted.WaitT(TDuration::Seconds(5)));
        probe = std::thread([&] {
            EXPECT_FALSE(cache->Extract(probeKey));
            probeFinished.Signal();
        });
        EXPECT_TRUE(probeFinished.WaitT(TDuration::Seconds(5)));
        EXPECT_FALSE(producerFinished.WaitT(TDuration::MilliSeconds(100)));
    }
}

TEST(TStateCacheReclamationTest, CompressedVictimIsReclaimedOutsidePrimaryCacheLock)
{
    for (bool reconfigure : {false, true}) {
        SCOPED_TRACE(reconfigure);
        TManualEvent reclamationStarted;
        TManualEvent resumeReclamation;
        TManualEvent producerFinished;
        TManualEvent probeFinished;
        std::atomic<bool> armed = false;
        std::atomic<int> destroyedCount = 0;
        auto cache = New<TReclamationTwoLevelCache>();
        auto shardCount = New<TSlruCacheConfig>()->ShardCount;
        cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ shardCount);
        auto victim = New<TReclaimableValue>([] {
        },
            [] {
            },
            [&] {
                ++destroyedCount;
                if (armed.load()) {
                    reclamationStarted.Signal();
                    resumeReclamation.WaitI();
                }
            });
        auto weakVictim = MakeWeak(victim);
        cache->Insert(TCacheTestKey{}, victim);
        victim.Reset();
        // Drain the first compressed item's pending touch before forcing its eviction.
        cache->Insert(TCacheTestKey{}, New<TReclaimableValue>([] {
        }));
        cache->Reconfigure(/*capacity*/ 1_KB * shardCount, /*compressedCapacity*/ shardCount);
        cache->Insert(TCacheTestKey{}, New<TReclaimableValue>([] {
        }));
        ASSERT_TRUE(weakVictim.Lock());
        ASSERT_EQ(destroyedCount.load(), 0);
        armed.store(true);

        std::thread producer([&] {
            if (reconfigure) {
                cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ shardCount);
            } else {
                cache->Insert(TCacheTestKey{}, New<TReclaimableValue>([] {
                }));
            }
            producerFinished.Signal();
        });
        std::thread probe;
        auto cleanup = Finally([&] {
            resumeReclamation.Signal();
            producer.join();
            if (probe.joinable()) {
                probe.join();
            }
        });

        ASSERT_TRUE(reclamationStarted.WaitT(TDuration::Seconds(5)));
        probe = std::thread([&] {
            EXPECT_FALSE(cache->Extract(TCacheTestKey{}));
            probeFinished.Signal();
        });
        EXPECT_TRUE(probeFinished.WaitT(TDuration::Seconds(5)));
        EXPECT_FALSE(producerFinished.WaitT(TDuration::MilliSeconds(100)));
        EXPECT_EQ(destroyedCount.load(), 1);
    }
}

TEST(TStateCacheReclamationTest, ShrinkingCompressedCapacityReclaimsOutsideCacheLock)
{
    TManualEvent reclamationStarted;
    TManualEvent resumeReclamation;
    TManualEvent producerFinished;
    TManualEvent probeFinished;
    std::atomic<bool> armed = false;
    std::atomic<int> destroyedCount = 0;
    auto cache = MakeTestableCache(/*uncompressed*/ 0, /*compressed*/ 1_MB);
    const auto jobId = TJobId(TGuid::Create());
    const TStateCacheKey key{jobId, MakeKey(ui64{0}), "state"};
    auto probeKey = key;
    const auto shardMask = New<TSlruCacheConfig>()->ShardCount - 1;
    for (ui64 index = 1;; ++index) {
        probeKey = TStateCacheKey{jobId, MakeKey(index), "state"};
        if ((THash<TStateCacheKey>()(probeKey) & shardMask) == (THash<TStateCacheKey>()(key) & shardMask)) {
            break;
        }
    }

    auto value = New<TReclaimableValue>(
        /*onReclaimed*/ [] {
        },
        /*onCompressed*/ std::function<void()>{},
        [&] {
            ++destroyedCount;
            if (armed.load()) {
                reclamationStarted.Signal();
                resumeReclamation.WaitI();
            }
        });
    auto weakValue = MakeWeak(value);
    cache->Insert(key, value);
    ASSERT_EQ(value->CompressCount, 1);
    value.Reset();
    ASSERT_TRUE(weakValue.Lock());
    ASSERT_EQ(destroyedCount.load(), 0);
    armed.store(true);

    std::thread producer([&] {
        cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 0);
        producerFinished.Signal();
    });
    std::thread probe;
    auto cleanup = Finally([&] {
        resumeReclamation.Signal();
        producer.join();
        if (probe.joinable()) {
            probe.join();
        }
    });

    ASSERT_TRUE(reclamationStarted.WaitT(TDuration::Seconds(5)));
    probe = std::thread([&] {
        EXPECT_FALSE(cache->Extract(probeKey));
        probeFinished.Signal();
    });
    EXPECT_TRUE(probeFinished.WaitT(TDuration::Seconds(5)));
    EXPECT_FALSE(producerFinished.WaitT(TDuration::MilliSeconds(100)));
    EXPECT_EQ(destroyedCount.load(), 1);
}

// At zero ttl Insert is a no-op, so what a previous non-zero ttl left in the cache must not be served either.
TEST(TStateCacheReclamationTest, ExpiringCacheExtractsNothingAtZeroTtl)
{
    auto queue = New<NConcurrency::TActionQueue>();
    auto invoker = NConcurrency::CreateSerializedInvoker(queue->GetInvoker());
    auto cache = MakeTestableCache(1_MB, 1_MB);
    auto namedCache = cache->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})->WithName("state");
    auto key = MakeKey(ui64{0});
    auto value = New<TReclaimableValue>([] {
    });

    auto spec = New<TDynamicExpiringJobNamedStateCacheSpec>();
    spec->Ttl = TDuration::Hours(1);
    auto zeroTtlSpec = New<TDynamicExpiringJobNamedStateCacheSpec>();
    ASSERT_EQ(zeroTtlSpec->Ttl, TDuration::Zero());

    // The cache captures the invoker it is created in, so its own calls all have to run there.
    auto done = BIND([&] {
        auto expiringCache = New<TExpiringJobNamedStateCache>(namedCache, spec);
        expiringCache->Insert(key, value, /*cookie*/ std::nullopt);
        expiringCache->Reconfigure(zeroTtlSpec);
        EXPECT_FALSE(expiringCache->Extract(key));
    }).AsyncVia(invoker)
        .Run();
    NConcurrency::WaitFor(done).ThrowOnError();
}

TEST(TStateCacheReclamationTest, ExpiringCacheReclaimsOutsideCacheLock)
{
    for (bool reconfigure : {false, true}) {
        SCOPED_TRACE(reconfigure);
        TManualEvent reclamationStarted;
        TManualEvent resumeReclamation;
        TManualEvent probeFinished;
        TManualEvent producerFinished;
        auto queue = New<NConcurrency::TActionQueue>();
        auto invoker = NConcurrency::CreateSerializedInvoker(queue->GetInvoker());
        auto cache = MakeTestableCache(reconfigure ? 1_MB : 0, 1_MB);
        auto jobId = TJobId(TGuid::Create());
        auto namedCache = cache->WithJob(jobId, NProfiling::TProfiler{})->WithName("state");
        auto key = MakeKey(ui64{0});
        const auto shardMask = New<TSlruCacheConfig>()->ShardCount - 1;
        const TStateCacheKey fullKey{jobId, key, "state"};
        auto probeKey = fullKey;
        for (ui64 index = 1;; ++index) {
            probeKey = TStateCacheKey{jobId, MakeKey(index), "state"};
            if ((THash<TStateCacheKey>()(probeKey) & shardMask) == (THash<TStateCacheKey>()(fullKey) & shardMask)) {
                break;
            }
        }
        auto value = New<TReclaimableValue>([&] {
            reclamationStarted.Signal();
            resumeReclamation.WaitI();
        });
        auto cookie = TInstant::Now() + TDuration::Hours(1);
        TExpiringJobNamedStateCachePtr expiringCache;
        auto producer = BIND([&] {
            auto spec = New<TDynamicExpiringJobNamedStateCacheSpec>();
            spec->Ttl = TDuration::Hours(1);
            expiringCache = New<TExpiringJobNamedStateCache>(namedCache, spec);
            expiringCache->Insert(key, value, cookie);
            if (reconfigure) {
                cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
            }
            auto extracted = expiringCache->Extract(key);
            ASSERT_TRUE(extracted);
            EXPECT_EQ(extracted->first.Get(), value.Get());
            EXPECT_EQ(extracted->second, cookie);
            EXPECT_EQ(value->CompressCount, 1);
            EXPECT_EQ(value->DecompressCount, 1);
        }).AsyncVia(invoker)
            .Run();
        producer.Subscribe(BIND([&] (const TError&) {
            producerFinished.Signal();
        }));
        std::thread probe;
        auto cleanup = Finally([&] {
            resumeReclamation.Signal();
            producerFinished.WaitI();
            if (probe.joinable()) {
                probe.join();
            }
        });
        ASSERT_TRUE(reclamationStarted.WaitT(TDuration::Seconds(5)));
        probe = std::thread([&] {
            EXPECT_FALSE(cache->Extract(probeKey));
            probeFinished.Signal();
        });
        EXPECT_TRUE(probeFinished.WaitT(TDuration::Seconds(5)));
        EXPECT_FALSE(producer.IsSet());
        resumeReclamation.Signal();
        ASSERT_TRUE(producerFinished.WaitT(TDuration::Seconds(5)));
        producer.GetOrCrash().ThrowOnError();
    }
}

TEST(TStateCacheReclamationTest, UncompressedExtractionDoesNotCompress)
{
    int reclaimedCount = 0;
    auto cache = MakeTestableCache(1_MB, 1_MB);
    const TStateCacheKey key{TJobId(TGuid::Create()), std::nullopt, "state"};
    auto value = New<TReclaimableValue>([&] {
        ++reclaimedCount;
    });

    cache->Insert(key, value);
    EXPECT_EQ(cache->Extract(key).Get(), value.Get());
    EXPECT_EQ(value->CompressCount, 0);
    EXPECT_EQ(value->DecompressCount, 0);
    EXPECT_EQ(reclaimedCount, 0);
    value.Reset();
    EXPECT_EQ(reclaimedCount, 1);
}

TEST(TStateCacheReclamationTest, CompressionWithoutDetachedStorage)
{
    auto cache = New<TTestTwoLevelCache>();
    cache->Reconfigure(/*capacity*/ 0, /*compressedCapacity*/ 1_MB);
    auto value = New<TCompressibleValue>();

    cache->Insert(0, value);
    EXPECT_EQ(value->CompressCount, 1);
    EXPECT_EQ(cache->Extract(0).Get(), value.Get());
    EXPECT_EQ(value->DecompressCount, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
