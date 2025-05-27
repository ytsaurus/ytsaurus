#include "cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/time_provider.h>

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

TIntrusivePtr<TPausedClock> MakePausedClock() {
    return new TPausedClock();
}

Y_UNIT_TEST_SUITE(LocalCacheTests) {

    Y_UNIT_TEST(OnEmpty_WhenGet_ThenReturnsExpiredDefault) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});

        auto entry = cache->Get(1).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 0);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, true);
    }

    Y_UNIT_TEST(OnEmpty_WhenUpdate_ThenGetReturnsNew) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});

        cache->Update(1, 1).GetValueSync();

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, false);
    }

    Y_UNIT_TEST(OnExistingKey_WhenUpdate_ThenGetReturnsNew) {
        auto cache = MakeLocalCache<int, int>(CreateDefaultTimeProvider(), {});
        cache->Update(1, 1);

        cache->Update(1, 2).GetValueSync();

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 2);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, false);
    }

    Y_UNIT_TEST(OnExistingKey_WhenExpires_ThenReturnedExpiredPast) {
        auto clock = MakePausedClock();
        auto cache = MakeLocalCache<int, int>(clock, {.TTL = TDuration::Minutes(2)});
        cache->Update(1, 1);

        clock->Skip(TDuration::Minutes(2) + TDuration::Seconds(1));

        auto entry = cache->Get(1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(entry.Value, 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.IsExpired, true);
    }

} // Y_UNIT_TEST_SUITE(LocalCacheTests)
