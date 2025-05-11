#include "managed_cache_ut.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TManagedCacheStorageTests) {

    Y_UNIT_TEST(TestValueCached) {
        size_t served;
        TCacheStorage cache(MakeDummyQuery(&served));

        UNIT_ASSERT_VALUES_EQUAL(cache.Get("key").GetValueSync(), "key");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_VALUES_EQUAL(cache.Get("key").GetValueSync(), "key");
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

    Y_UNIT_TEST(TestErrorNotCached) {
        size_t served;
        bool isFailing;
        TCacheStorage cache(MakeDummyQuery(&served, &isFailing));

        isFailing = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 2);
    }

    Y_UNIT_TEST(TestErrorNotUpdated) {
        size_t served;
        bool isFailing;
        TCacheStorage cache(MakeDummyQuery(&served, &isFailing));

        isFailing = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_NO_EXCEPTION(cache.Update()); // Mark outdated
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_NO_EXCEPTION(cache.Update()); // Update
        UNIT_ASSERT_VALUES_EQUAL(served, 1);
    }

    Y_UNIT_TEST(TestUpdateErrorInvalidates) {
        size_t served;
        bool isFailing;
        TCacheStorage cache(MakeDummyQuery(&served, &isFailing));

        UNIT_ASSERT_NO_EXCEPTION(cache.Get("key").GetValueSync());
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_NO_EXCEPTION(cache.Update()); // Mark outdated
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        isFailing = true;
        UNIT_ASSERT_NO_EXCEPTION(cache.Get("key").GetValueSync());
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Update(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 2);

        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 3);
    }

    Y_UNIT_TEST(TestErrorEvicted) {
        size_t served;
        bool isFailing;
        TCacheStorage cache(MakeDummyQuery(&served, &isFailing));

        isFailing = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(cache.Get("key").GetValueSync(), yexception, "o_o");
        UNIT_ASSERT_VALUES_EQUAL(served, 1);

        UNIT_ASSERT_NO_EXCEPTION(cache.Evict());
        // TODO(YQL-19747): Test really evicted
    }

} // Y_UNIT_TEST_SUITE(TManagedCacheStorageTests)
