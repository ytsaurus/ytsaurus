#include "managed_cache_ut.h"
#include "managed_cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <library/cpp/iterator/enumerate.h>

#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <util/random/normal.h>

#include <random>

using namespace NYql;

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
                if (Success_) {
                    is_failed = !(*Success_)(Random_);
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
            Success_ = std::bernoulli_distribution(successRate);
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
    TMaybe<std::bernoulli_distribution> Success_ = std::bernoulli_distribution{0.85};
    std::uniform_int_distribution<int> LatencyMcs_{0, 128};

    THolder<IThreadPool> Pool_ = CreateThreadPool(/* threadCount = */ 16);
};

Y_UNIT_TEST_SUITE(TManagedCacheTests) {

    Y_UNIT_TEST(TestStress) {
        TIdentityService service;

        TManagedCacheConfig config = {
            .UpdatePeriod = TDuration::MilliSeconds(10),
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
