#include "cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/random/random.h>

using namespace NSQLComplete;

class TPausedClock: public ITimeProvider {
public:
    TInstant Now() override {
        return Now_;
    }

    void Skip(TDuration duration) {
        Now_ += duration;
    }

private:
    TInstant Now_ = CreateDeterministicTimeProvider(1)->Now();
};

struct TIdentitySizeProvider {
    size_t operator()(int value) const {
        return value;
    }
};

TIntrusivePtr<TPausedClock> MakePausedClock() {
    return new TPausedClock();
}

Y_UNIT_TEST_SUITE(LocalCacheTests) {

    Y_UNIT_TEST(OnEmpty_WhenGet_ThenReturnedExpiredDefault) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});

        auto entry = cache->Get(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 0);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, true);
    }

    Y_UNIT_TEST(OnEmpty_WhenUpdate_ThenReturnedNew) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});

        cache->Update(1, 1).GetValueSync();

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, false);
    }

    Y_UNIT_TEST(OnExistingKey_WhenUpdate_ThenReturnedNew) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});
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
            CreateDefaultTimeProvider(), {.Capacity = 16});
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
        constexpr double GetFrequency = 0.75;
        constexpr ui32 MaxKey = 100;
        constexpr ui32 MinValue = 1;
        constexpr ui32 MaxValue = 10;
        SetRandomSeed(1);

        auto cache = MakeLocalCache<ui32, ui32, TIdentitySizeProvider>(
            CreateDefaultTimeProvider(), {.Capacity = 128, .TTL = TDuration::MilliSeconds(1)});

        for (size_t i = 0; i < Iterations; ++i) {
            ui32 key = RandomNumber(MaxKey);
            if (RandomNumber<double>() < GetFrequency) {
                Y_DO_NOT_OPTIMIZE_AWAY(cache->Get(key));
            } else {
                ui32 value = MinValue + RandomNumber(MaxValue - MinValue);
                Y_DO_NOT_OPTIMIZE_AWAY(cache->Update(key, value));
            }
        }
    }

} // Y_UNIT_TEST_SUITE(LocalCacheTests)
