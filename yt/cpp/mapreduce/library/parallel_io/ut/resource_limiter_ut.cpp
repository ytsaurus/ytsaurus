#include <yt/cpp/mapreduce/library/parallel_io/resource_limiter.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <chrono>
#include <thread>

using namespace NYT;
using namespace NYT::NTesting;

Y_UNIT_TEST_SUITE(ResourceLimiter)
{
    Y_UNIT_TEST(AcquireRelease)
    {
        auto limiter = ::MakeIntrusive<TResourceLimiter>(5u);
        limiter->Acquire(2);

        std::atomic<bool> guardedFirst = false;
        std::atomic<bool> guardedSecond = false;

        TSimpleThreadPool threadPool;
        threadPool.Start(1);

        NThreading::Async([&] () {
            {
                TResourceGuard guard(limiter, 1);
                guardedFirst.store(true);
                guardedFirst.notify_all();
            }

            {
                TResourceGuard guard(limiter, 4);
                guardedSecond.store(true);
            }
        }, threadPool);

        guardedFirst.wait(false);
        UNIT_ASSERT_EQUAL(guardedFirst.load(), true);

        UNIT_ASSERT_EQUAL(guardedSecond.load(), false);
        limiter->Release(2);
        threadPool.Stop();
        UNIT_ASSERT_EQUAL(guardedSecond.load(), true);
    }
}
