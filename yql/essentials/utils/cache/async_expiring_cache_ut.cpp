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

Y_UNIT_TEST_SUITE(TAsyncExpiringCacheTests) {

    Y_UNIT_TEST(Stress) {
        using K = size_t;
        using V = size_t;

        TAsyncExpiringCacheConfig config = {
            .TickPeriod = TDuration::MilliSeconds(100),
            .UpdateFrequency = 1,
            .EvictionFrequency = 3,
        };

        TMutex mutex;
        std::mt19937 random(213131);
        std::bernoulli_distribution failure(0.75);
        std::uniform_int_distribution<int> latency(0, 10);

        const auto async = [&](auto& pool, auto f) {
            return NThreading::Async(f, *pool);
        };

        const auto service_pool = CreateThreadPool(/* threadCount = */ 128);

        const auto serve = [&](K key) -> NThreading::TFuture<V> {
            return async(service_pool, [&, key]() {
                TDuration delay;
                bool is_failed;
                with_lock (mutex) {
                    delay = TDuration::MilliSeconds(latency(random));
                    is_failed = failure(random);
                }

                Sleep(delay);

                if (is_failed) {
                    throw yexception() << "o_o";
                }

                return key;
            });
        };

        auto cache = MakeIntrusive<TAsyncExpiringCache<K, V>>(
            config.UpdateFrequency,
            config.EvictionFrequency,
            serve);

        NThreading::TCancellationTokenSource activity;
        auto refresher = async(service_pool, [config, cache, token = activity.Token()]() {
            while (!token.IsCancellationRequested()) {
                Sleep(config.TickPeriod);
                cache->OnTick();
            }
        });

        const auto client_pool = CreateThreadPool(/* threadCount = */ 8);

        const auto map = [](K key) -> K { return key / 10 % 1000; };

        TVector<NThreading::TFuture<V>> futures;
        for (size_t i = 0; i < 10'000; ++i) {
            futures.emplace_back(async(client_pool, [cache, i, map]() {
                return cache->Get(map(i));
            }));
        }
        NThreading::WaitAll(futures).Wait(TDuration::Seconds(32));

        for (auto [i, f] : Enumerate(futures)) {
            UNIT_ASSERT(f.IsReady());
            if (f.HasValue()) {
                UNIT_ASSERT_VALUES_EQUAL(map(i), f.GetValue());
            } else {
                UNIT_ASSERT(f.HasException());
                UNIT_ASSERT_EXCEPTION_CONTAINS(f.TryRethrow(), yexception, "o_o");
            }
        }

        activity.Cancel();
        refresher.Wait(TDuration::Seconds(8));
    }

} // Y_UNIT_TEST_SUITE(TAsyncExpiringCacheTests)
