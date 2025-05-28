#include "cached.h"

#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/monotonic_provider.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(CachedQueryTests) {

    Y_UNIT_TEST(OnExpired_WhenApplied_ThenDefferedUpdateAndReturnOld) {
        size_t queried = 0;
        auto cache = MakeLocalCache<int, int>(
            NMonotonic::CreateDefaultMonotonicTimeProvider(), {.TTL = TDuration::Zero()});
        auto cached = TCachedQuery<int, int>(cache, [&](const int& key) {
            queried += 1;
            return NThreading::MakeFuture<int>(key);
        });
        cache->Update(1, 2);

        int value = cached(1).GetValueSync();

        Y_ENSURE(value, 2);
        Y_ENSURE(queried, 1);
        Y_ENSURE(cached(1).GetValueSync(), 1);
    }

} // Y_UNIT_TEST_SUITE(CachedQueryTests)
