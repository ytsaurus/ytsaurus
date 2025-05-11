#include "managed_cache_ut.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TManagedCacheMaintenanceTests) {

    Y_UNIT_TEST(TestUpdate) {
        // .       e
        // .   u   u
        // |---|---|--
        // 0   1   2
        TManagedCacheConfig config = {
            .EvictionFrequency = 2,
        };

        size_t served;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(&served));
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
            .EvictionFrequency = 2,
        };

        size_t served;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(&served));
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
            .EvictionFrequency = 2,
        };

        size_t served = 0;
        auto cache = MakeIntrusive<TCacheStorage>(MakeDummyQuery(&served));
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

} // Y_UNIT_TEST_SUITE(TManagedCacheMaintenanceTests)
