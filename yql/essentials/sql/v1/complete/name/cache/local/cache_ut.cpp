#include "cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/monotonic_provider.h>

#include <util/random/random.h>
#include <util/thread/pool.h>

using namespace NSQLComplete;

class TPausedClock: public NMonotonic::IMonotonicTimeProvider {
public:
    NMonotonic::TMonotonic Now() override {
        return Now_;
    }

    void Skip(TDuration duration) {
        Now_ += duration;
    }

private:
    NMonotonic::TMonotonic Now_ = NMonotonic::CreateDefaultMonotonicTimeProvider()->Now();
};

struct TIdentitySizeProvider {
    size_t operator()(int value) const {
        return value;
    }
};

struct TAction {
    bool IsGet = false;
    ui32 Key = 0;
    ui32 Value = 0;
};

TVector<TAction> GenerateRandomActions(size_t size) {
    constexpr double GetFrequency = 0.75;
    constexpr ui32 MaxKey = 100;
    constexpr ui32 MinValue = 1;
    constexpr ui32 MaxValue = 10;

    TVector<TAction> actions(size);
    for (auto& action : actions) {
        action.IsGet = RandomNumber<double>() < GetFrequency;
        action.Key = RandomNumber(MaxKey);
        action.Value = MinValue + RandomNumber(MaxValue - MinValue);
    }
    return actions;
}

TIntrusivePtr<TPausedClock> MakePausedClock() {
    return new TPausedClock();
}

Y_UNIT_TEST_SUITE(LocalCacheTests) {

    Y_UNIT_TEST(OnEmpty_WhenGet_ThenReturnedExpiredDefault) {
        auto cache = MakeLocalCache<int, int>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {});

        auto entry = cache->Get(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 0);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, true);
    }

    Y_UNIT_TEST(OnEmpty_WhenUpdate_ThenReturnedNew) {
        auto cache = MakeLocalCache<int, int>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {});

        cache->Update(1, 1).GetValueSync();

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, false);
    }

    Y_UNIT_TEST(OnExistingKey_WhenUpdate_ThenReturnedNew) {
        auto cache = MakeLocalCache<int, int>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {});
        cache->Update(1, 1);

        cache->Update(1, 2).GetValueSync();

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 2);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, false);
    }

    Y_UNIT_TEST(OnExistingKey_WhenExpires_ThenReturnedOld) {
        auto clock = MakePausedClock();
        auto cache = MakeLocalCache<int, int>(clock, {.TTL = TDuration::Minutes(2)});
        cache->Update(1, 1);

        clock->Skip(TDuration::Minutes(2) + TDuration::Seconds(1));

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, true);
    }

    Y_UNIT_TEST(OnFull_WhenFatAdded_ThenSomeKeysAreEvicted) {
        auto cache = MakeLocalCache<int, int, TIdentitySizeProvider>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.Capacity = 16});
        cache->Update(1, 4);
        cache->Update(2, 4);
        cache->Update(3, 4);
        cache->Update(4, 4);

        cache->Update(5, 8);

        size_t evicted = 0;
        size_t size = 0;
        for (int x : {1, 2, 3, 4, 5}) {
            auto entry = cache->Get(x).GetValueSync();
            if (entry.IsExpired) {
                evicted += 1;
            } else {
                size += entry.Value;
            }
        }

        UNIT_ASSERT_LE(2, evicted);
        UNIT_ASSERT_LE(size, 16);
        UNIT_ASSERT_LT(0, size);
    }

    Y_UNIT_TEST(WhenRandomlyAccessed_ThenDoesNotDie) {
        constexpr size_t Iterations = 1024 * 1024;
        SetRandomSeed(1);

        auto cache = MakeLocalCache<ui32, ui32, TIdentitySizeProvider>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.Capacity = 128, .TTL = TDuration::MilliSeconds(1)});

        for (auto&& a : GenerateRandomActions(Iterations)) {
            if (a.IsGet) {
                Y_DO_NOT_OPTIMIZE_AWAY(cache->Get(a.Key));
            } else {
                Y_DO_NOT_OPTIMIZE_AWAY(cache->Update(a.Key, a.Value));
            }
        }
    }

    Y_UNIT_TEST(WhenConcurrentlyAccessed_ThenDoesNotDie) {
        constexpr size_t Threads = 8;
        constexpr size_t Iterations = Threads * 16 * 1024;
        SetRandomSeed(1);

        auto cache = MakeLocalCache<ui32, ui32, TIdentitySizeProvider>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.Capacity = 128, .TTL = TDuration::MilliSeconds(1)});

        auto pool = CreateThreadPool(Threads);
        for (auto&& a : GenerateRandomActions(Iterations)) {
            Y_ENSURE(pool->AddFunc([cache, a = std::move(a)] {
                if (a.IsGet) {
                    Y_DO_NOT_OPTIMIZE_AWAY(cache->Get(a.Key));
                } else {
                    Y_DO_NOT_OPTIMIZE_AWAY(cache->Update(a.Key, a.Value));
                }
            }));
        }
        pool->Stop();
    }

} // Y_UNIT_TEST_SUITE(LocalCacheTests)
