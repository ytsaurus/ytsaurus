#include <thread>

#include <yt/core/test_framework/framework.h>

#include <yt/server/scheduler/sync_expiring_cache.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSyncExpiringCacheTest, MultipleGet_RaceCondition)
{
    NScheduler::TSyncExpiringCache<int, int> cache(
        BIND([](int x) {
            Sleep(TDuration::MilliSeconds(1));
            return x;
        }), TDuration::Seconds(1));

    std::thread thread1([&]() { cache.Get(1); });
    std::thread thread2([&]() { cache.Get(1); });

    thread1.join();
    thread2.join();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
