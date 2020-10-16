#include <thread>

#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/sync_expiring_cache.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSyncExpiringCacheTest, MultipleGet_RaceCondition)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        BIND([] (int x) {
            Sleep(TDuration::MilliSeconds(1));
            return x;
        }),
        TDuration::Seconds(1),
        GetSyncInvoker());

    std::thread thread1([&] { cache->Get(1); });
    std::thread thread2([&] { cache->Get(1); });

    thread1.join();
    thread2.join();
}

TEST(TSyncExpiringCacheTest, FindSetClear)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        BIND([] (int x) {
            return x;
        }),
        TDuration::Seconds(1),
        GetSyncInvoker());

    auto value1 = cache->Find(1);
    EXPECT_EQ(std::nullopt, value1);

    cache->Set(1, 2);

    auto value2 = cache->Find(1);
    EXPECT_EQ(2, value2);

    cache->Clear();

    auto value3 = cache->Find(1);
    EXPECT_EQ(std::nullopt, value3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
