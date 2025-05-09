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

Y_UNIT_TEST_SUITE(TAsyncExpiringCacheTests) {

    Y_UNIT_TEST(TestCached) {
        size_t served = 0;

        auto cache = MakeIntrusive<TAsyncExpiringCache<TKey, TValue>>(
            /* updateFrequency = */ static_cast<size_t>(1),
            /* evictionFrequency = */ static_cast<size_t>(3),
            /* query */ [&](const TKey&) {
                served += 1;
                return NThreading::MakeFuture<TValue>({});
            });

        UNIT_ASSERT_VALUES_EQUAL(served, 0);

        cache->Get("1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->Get("1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        cache->Get("2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        cache->Get("2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(served, 2);
    }

    Y_UNIT_TEST(Stress) {
        TIdentityService service;

        TAsyncExpiringCacheConfig config = {
            .TickPeriod = TDuration::MilliSeconds(100),
            .UpdateFrequency = 1,
            .EvictionFrequency = 3,
        };

        auto cache = MakeIntrusive<TAsyncExpiringCache<TKey, TValue>>(
            config.UpdateFrequency,
            config.EvictionFrequency,
            service.QueryFunc());

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
        for (size_t i = 0; i < 100'000; ++i) {
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
