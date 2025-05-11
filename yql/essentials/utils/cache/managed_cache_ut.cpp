#include "managed_cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <library/cpp/iterator/enumerate.h>

#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <util/random/normal.h>

#include <random>

// TODO:
// - [ ] Do not cache exceptions
// - [ ] Extract UpdateScan and EvictScan
// - [ ] Think about capacity (LRU)
// - [ ] What if query is longer than quantum
// - [ ] Quantum is a update frequency and eviction rate relative to update rate

using namespace NYql;

using TKey = TString;
using TValue = TString;

using TCacheStorage = TManagedCacheStorage<TKey, TValue>;
using TCacheMaintenance = TManagedCacheMaintenance<TKey, TValue>;

auto Async(auto& pool, auto f) {
    return NThreading::Async(f, *pool);
}

class TIdentityService {
public:
    NThreading::TFuture<TVector<TValue>> Get(const TVector<TKey>& keys) {
        return Async(Pool_, [this, keys = std::move(keys)]() mutable {
            TDuration delay;
            bool is_failed = false;
            with_lock (Mutex_) {
                delay = TDuration::MicroSeconds(LatencyMcs_(Random_));
                if (Failure_) {
                    is_failed = !(*Failure_)(Random_);
                }
            }

            Sleep(delay);

            if (is_failed) {
                throw yexception() << "o_o";
            }

            return keys;
        });
    }

    void SetSuccessRate(double successRate) {
        with_lock (Mutex_) {
            Failure_ = std::bernoulli_distribution(successRate);
        }
    }

    void SetMaxLatencyMs(int maxLatencyMs) {
        with_lock (Mutex_) {
            LatencyMcs_ = std::uniform_int_distribution<int>(0, maxLatencyMs);
        }
    }

    auto QueryFunc() {
        return [this](const TVector<TKey>& keys) { return Get(keys); };
    }

private:
    TMutex Mutex_;
    std::mt19937 Random_{231312};
    TMaybe<std::bernoulli_distribution> Failure_ = std::bernoulli_distribution{0.85};
    std::uniform_int_distribution<int> LatencyMcs_{0, 32};

    THolder<IThreadPool> Pool_ = CreateThreadPool(/* threadCount = */ 16);
};

TManagedCacheStorage<TKey, TValue>::TQuery MakeDummyQuery(size_t& served, bool& isFailing) {
    return [&](const TVector<TKey>& key) {
        served += 1;
        if (isFailing) {
            return NThreading::MakeErrorFuture<TVector<TValue>>(
                std::make_exception_ptr(yexception() << "o_o"));
        }
        return NThreading::MakeFuture<TVector<TValue>>(key);
    };
}

Y_UNIT_TEST_SUITE(TManagedCacheTests) {

    Y_UNIT_TEST(TestValueCached) {
        size_t served = 0;
        bool isFailing = false;
        TCacheStorage cache(MakeDummyQuery(served, isFailing));

        UNIT_ASSERT_VALUES_EQUAL(cache.Get("key").GetValueSync(), "key");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_VALUES_EQUAL(cache.Get("key").GetValueSync(), "key");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);
    }

    Y_UNIT_TEST(TestErrorCached) {
        size_t served = 0;
        bool isFailing = false;
        TCacheStorage cache(MakeDummyQuery(served, isFailing));

        isFailing = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        isFailing = false;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);
    }

    Y_UNIT_TEST(TestGetQueue) {
        auto promise = NThreading::NewPromise<TVector<TValue>>();

        size_t served = 0;
        TCacheStorage cache([&](auto) {
            served += 1;
            return promise.GetFuture();
        });

        auto first = cache.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(first.IsReady(), false);
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        auto second = cache.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(second.IsReady(), false);
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        promise.SetValue({"value"});
        UNIT_ASSERT_VALUES_EQUAL(first.GetValue(), "value");
        UNIT_ASSERT_VALUES_EQUAL(second.GetValue(), "value");
    }

    Y_UNIT_TEST(TestUpdate) {
        // .       e
        // .   u   u
        // |---|---|--
        // 0   1   2
        TManagedCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(served, isFailing));
        TCacheMaintenance maintenance(cache, config);

        cache->Get("key").GetValueSync();

        maintenance.Tick(); // 1: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        maintenance.Tick(); // 2: Outdated => Update
        UNIT_ASSERT_VALUES_EQUAL(served, 2);
    }

    Y_UNIT_TEST(TestEviction) {
        // .       e       e       e
        // .   u   u   u   u   u   u
        // |---|---|---|---|---|---|--
        // 0   1   2   3   4   5   6
        TManagedCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(served, isFailing));
        TCacheMaintenance maintenance(cache, config);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        maintenance.Tick(); // 1: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        maintenance.Tick();                  // 2 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2); // 2 (u): Outdated => Update

        maintenance.Tick(); // 3: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        maintenance.Tick(); // 4: Abandoned => Evict
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        maintenance.Tick();
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 3);
    }

    Y_UNIT_TEST(TestPreventEviction) {
        // .       e       e
        // .   u   u   u   u
        // |---|---|---|---|--
        // 0   1   2   3   4
        TManagedCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(served, isFailing));
        TCacheMaintenance maintenance(cache, config);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        maintenance.Tick();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        maintenance.Tick();                  // 2 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2); // 2 (u): Outdated => Update

        cache->Get("key").GetValueSync(); // Set Referenced
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        maintenance.Tick(); // 3: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        maintenance.Tick();                  // 4 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 3); // 4 (u): Outdated => Update
    }

    Y_UNIT_TEST(TestStress) {
        TIdentityService service;
        service.SetSuccessRate(0.95);

        TManagedCacheConfig config = {
            .TickPause = TDuration::MilliSeconds(10),
            .UpdateFrequency = 1,
            .EvictionFrequency = 3,
        };

        auto cache_pool = CreateThreadPool(/* threadCount = */ 2);
        auto cache = StartManagedCache<TKey, TValue>(
            cache_pool.Get(), config, service.QueryFunc());

        const auto client_pool = CreateThreadPool(/* threadCount = */ 8);
        TVector<NThreading::TFuture<TValue>> futures;
        for (size_t i = 0; i < 400'000; ++i) {
            futures.emplace_back(Async(client_pool, [cache, i]() {
                return cache->Get(ToString(i / 100 % 1000));
            }));
        }
        NThreading::WaitAll(futures).Wait(TDuration::Seconds(8));

        for (auto [i, f] : Enumerate(futures)) {
            UNIT_ASSERT(f.IsReady());
            if (f.HasValue()) {
                UNIT_ASSERT_VALUES_UNEQUAL("", f.GetValue());
            } else {
                UNIT_ASSERT(f.HasException());
                UNIT_ASSERT_EXCEPTION_CONTAINS(f.TryRethrow(), yexception, "o_o");
            }
        }
    }

} // Y_UNIT_TEST_SUITE(TManagedCacheTests)
