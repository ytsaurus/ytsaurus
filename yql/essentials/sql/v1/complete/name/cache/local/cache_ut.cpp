#include "cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/time_provider/time_provider.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(LocalCacheTests) {

    Y_UNIT_TEST(Example) {
        auto cache = new TLocalCache<int, int>(CreateDefaultTimeProvider(), {});
        Y_DO_NOT_OPTIMIZE_AWAY(cache);
    }

} // Y_UNIT_TEST_SUITE(LocalCacheTests)
