#include "async_expiring_cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <library/cpp/iterator/enumerate.h>

#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <util/random/normal.h>

#include <random>

using namespace NYql;

using TKey = TString;
using TValue = TString;

auto Async(auto& pool, auto f) {
    return NThreading::Async(f, *pool);
}

class TIdentityService {
public:
    NThreading::TFuture<TValue> Get(TKey key) {
        return Async(Pool_, [this, key = std::move(key)]() mutable {
            TDuration delay;
            bool is_failed = false;
            with_lock (Mutex_) {
                delay = TDuration::MilliSeconds(LatencyMs_(Random_));
                if (Failure_) {
                    is_failed = (*Failure_)(Random_);
                }
            }

            Sleep(delay);

            if (is_failed) {
                throw yexception() << "o_o";
            }

            return key;
        });
    }

    void SetSuccessRate(double successRate) {
        with_lock (Mutex_) {
            if (successRate == 1.0) {
                Failure_ = Nothing();
            } else {
                Failure_ = std::bernoulli_distribution(successRate);
            }
        }
    }

    void SetMaxLatencyMs(int maxLatencyMs) {
        with_lock (Mutex_) {
            LatencyMs_ = std::uniform_int_distribution<int>(0, maxLatencyMs);
        }
    }

    auto QueryFunc() {
        return [this](TKey k) { return Get(k); };
    }

private:
    TMutex Mutex_;
    std::mt19937 Random_{231312};
    TMaybe<std::bernoulli_distribution> Failure_ = std::bernoulli_distribution{0.75};
    std::uniform_int_distribution<int> LatencyMs_{0, 5};

    THolder<IThreadPool> Pool_ = CreateThreadPool(/* threadCount = */ 64);
};

TAsyncExpiringCache<TKey, TValue>::TPtr MakeDummy(
    size_t& served, bool& isFailing, TAsyncExpiringCacheConfig config) {
    return MakeIntrusive<TAsyncExpiringCache<TKey, TValue>>(
        config, [&](const TKey& key) {
            served += 1;
            if (isFailing) {
                return NThreading::MakeErrorFuture<TValue>(
                    std::make_exception_ptr(yexception() << "o_o"));
            }
            return NThreading::MakeFuture<TValue>(key);
        });
}

Y_UNIT_TEST_SUITE(TAsyncExpiringCacheTests) {

    Y_UNIT_TEST(TestValueCached) {
        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeDummy(served, isFailing, {});

        UNIT_ASSERT_VALUES_EQUAL(cache->Get("key").GetValueSync(), "key");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_VALUES_EQUAL(cache->Get("key").GetValueSync(), "key");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);
    }

    Y_UNIT_TEST(TestErrorCached) {
        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeDummy(served, isFailing, {});

        isFailing = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache->Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        isFailing = false;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache->Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);
    }

    Y_UNIT_TEST(TestUpdate) {
        // .       e
        // .   u   u
        // |---|---|--
        // 0   1   2
        TAsyncExpiringCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeDummy(served, isFailing, config);

        cache->Get("key").GetValueSync();

        cache->OnTick(); // 1: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->OnTick(); // 2: Outdated => Update
        UNIT_ASSERT_VALUES_EQUAL(served, 2);
    }

    Y_UNIT_TEST(TestEviction) {
        // .       e       e       e
        // .   u   u   u   u   u   u
        // |---|---|---|---|---|---|--
        // 0   1   2   3   4   5   6
        TAsyncExpiringCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeDummy(served, isFailing, config);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->OnTick(); // 1: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->OnTick();                     // 2 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2); // 2 (u): Outdated => Update

        cache->OnTick(); // 3: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->OnTick(); // 4: Abandoned => Evict
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->OnTick();
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 3);
    }

    Y_UNIT_TEST(TestPreventEviction) {
        // .       e       e
        // .   u   u   u   u
        // |---|---|---|---|--
        // 0   1   2   3   4
        TAsyncExpiringCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        size_t served = 0;
        bool isFailing = false;
        auto cache = MakeDummy(served, isFailing, config);

        cache->Get("key").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->OnTick();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->OnTick();                     // 2 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2); // 2 (u): Outdated => Update

        cache->Get("key").GetValueSync(); // Set Referenced
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->OnTick(); // 3: Updated => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->OnTick();                     // 4 (e): Referenced => Reset
        UNIT_ASSERT_VALUES_EQUAL(served, 3); // 4 (u): Outdated => Update
    }

    Y_UNIT_TEST(TestGetQueue) {
        // .           e
        // .   u   u   u
        // |---|---|---|--
        // 0   1   2   3
        TAsyncExpiringCacheConfig config = {
            .UpdateFrequency = 1,   // u
            .EvictionFrequency = 2, // e
        };

        auto promise = NThreading::NewPromise<TString>();

        size_t served = 0;
        auto cache = MakeIntrusive<TAsyncExpiringCache<TKey, TValue>>(
            config, [&](const TKey&) {
                served += 1;
                return promise.GetFuture();
            });

        auto first = cache->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(first.IsReady(), false);
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        auto second = cache->Get("key");
        UNIT_ASSERT_VALUES_EQUAL(second.IsReady(), false);
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        promise.SetValue("value");
        UNIT_ASSERT_VALUES_EQUAL(first.GetValue(), "value");
        UNIT_ASSERT_VALUES_EQUAL(second.GetValue(), "value");
    }

    Y_UNIT_TEST(TestStress) {
        TIdentityService service;

        TAsyncExpiringCacheConfig config = {
            .TickPeriod = TDuration::MilliSeconds(100),
            .UpdateFrequency = 1,
            .EvictionFrequency = 3,
        };

        auto cache = MakeIntrusive<TAsyncExpiringCache<TKey, TValue>>(
            config, service.QueryFunc());

        NThreading::TCancellationTokenSource activity;
        auto cache_pool = CreateThreadPool(/* threadCount = */ 2);
        auto refresher = Async(cache_pool, [config, cache, token = activity.Token()]() {
            while (!token.IsCancellationRequested()) {
                Sleep(config.TickPeriod);
                cache->OnTick();
            }
        });

        const auto client_pool = CreateThreadPool(/* threadCount = */ 8);
        TVector<NThreading::TFuture<TValue>> futures;
        for (size_t i = 0; i < 1'000; ++i) {
            futures.emplace_back(Async(client_pool, [cache, i]() {
                return cache->Get(ToString(i / 10 % 100));
            }));
        }
        NThreading::WaitAll(futures).Wait(TDuration::Seconds(16));

        for (auto [i, f] : Enumerate(futures)) {
            UNIT_ASSERT(f.IsReady());
            if (f.HasValue()) {
                UNIT_ASSERT_VALUES_UNEQUAL("", f.GetValue());
            } else {
                UNIT_ASSERT(f.HasException());
                UNIT_ASSERT_EXCEPTION_CONTAINS(f.TryRethrow(), yexception, "o_o");
            }
        }

        activity.Cancel();
        refresher.Wait(TDuration::Seconds(8));
    }

} // Y_UNIT_TEST_SUITE(TAsyncExpiringCacheTests)
