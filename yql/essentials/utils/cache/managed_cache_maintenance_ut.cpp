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
            .UpdatesPerEviction = 2,
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
            .UpdatesPerEviction = 2,
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
            .UpdatesPerEviction = 2,
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

    // Min(TTL) = 1 * (UpdatePeriod + QueryLatency), when added just before the update,
    // Avg(TTL) = 2 * (UpdatePeriod + QueryLatency) = Max(TTL), after 1 update cycle.
    Y_UNIT_TEST(TestActuality) {
        TManagedCacheConfig config = {
            .UpdatesPerEviction = 16, // Disable eviction
        };

        size_t time = 0;
        size_t updatePeriod = 100;
        size_t queryLatency = 10;

        // . <------update period------><--query latency-->
        // --|-------------------------||-----------------|<>-
        // . <----------concurrent get requests----------->
        // Where <> -- publication

        auto cache = MakeIntrusive<TCacheStorage>([&](const TVector<TKey>& keys) {
            time += queryLatency;
            return NThreading::MakeFuture(TVector<TValue>(keys.size(), ToString(time)));
        });
        TCacheMaintenance maintenance(cache, config);

        cache->Get({"exising"}).GetValueSync();
        cache->Update(); // Mark outdated
        // -eu----u-----u-----u-----u---
        //    ^

        cache->Get({"key"}).GetValueSync();
        // -euk---u-----u-----u-----u---
        //     ^

        size_t startTime = time;

        time += updatePeriod;
        cache->Update(); // Mark `key` outdated, update `existing`
        // -euk---u-----u-----u-----u---
        //         ^

        UNIT_ASSERT_VALUES_EQUAL(
            cache->Get({"key"}).GetValueSync(),
            ToString(startTime));
        // .  .----.
        // -euk---uk----u-----u-----u---
        //          ^

        time += updatePeriod;
        // .  .----.
        // -euk---uk----u-----u-----u---
        //             ^

        UNIT_ASSERT_VALUES_EQUAL(
            cache->Get({"key"}).GetValueSync(),
            ToString(startTime));
        // .  .--------.
        // -euk---uk---ku-----u-----u---
        //             ^

        cache->Update(); // Update `key` and `existing`
        // .  .--------.
        // -euk---uk---ku-----u-----u---
        //              ^

        UNIT_ASSERT_VALUES_EQUAL(
            cache->Get({"key"}).GetValueSync(),
            ToString(startTime + 2 * updatePeriod + 2 * queryLatency));
        // .            ..
        // -euk---uk---kuk----u-----u---
        //               ^
    }

} // Y_UNIT_TEST_SUITE(TManagedCacheMaintenanceTests)
