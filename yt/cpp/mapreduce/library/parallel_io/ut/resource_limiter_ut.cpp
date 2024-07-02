#include <yt/cpp/mapreduce/library/parallel_io/resource_limiter.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <atomic>
#include <condition_variable>

using namespace NYT;
using namespace NYT::NTesting;

TEST(TResourceLimiterTest, AcquireRelease)
{
    auto limiter = ::MakeIntrusive<TResourceLimiter>(5u);
    limiter->Acquire(2, EResourceLimiterLockType::SOFT);

    std::condition_variable cvFirst;
    std::mutex mFirst;

    bool guardedFirst = false;
    std::atomic<bool> guardedSecond = false;

    TSimpleThreadPool threadPool;
    threadPool.Start(1);

    std::unique_lock<std::mutex> lk(mFirst);

    NThreading::Async([&] () {
        {
            TResourceGuard guard(limiter, 1);
            {
                std::lock_guard<std::mutex> guard(mFirst);
                guardedFirst = true;
            }
            cvFirst.notify_all();
        }

        {
            TResourceGuard guard(limiter, 4);
            guardedSecond.store(true);
        }
    }, threadPool);

    cvFirst.wait(lk, [&]{ return guardedFirst == true; });

    EXPECT_EQ(guardedFirst, true);

    EXPECT_EQ(guardedSecond.load(), false);
    limiter->Release(2, EResourceLimiterLockType::SOFT);
    threadPool.Stop();
    EXPECT_EQ(guardedSecond.load(), true);
}

TEST(TResourceLimiterTest, AcquireOutOfLimit)
{
    auto limiter = ::MakeIntrusive<TResourceLimiter>(5u);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(limiter->Acquire(6u, EResourceLimiterLockType::SOFT), yexception, "6");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(limiter->Acquire(6u, EResourceLimiterLockType::HARD), yexception, "6");

    limiter->Acquire(3u, EResourceLimiterLockType::HARD);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(limiter->Acquire(3u, EResourceLimiterLockType::SOFT), yexception, "3");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(limiter->Acquire(3u, EResourceLimiterLockType::HARD), yexception, "3");
    limiter->Acquire(1u, EResourceLimiterLockType::SOFT);
    limiter->Release(3u, EResourceLimiterLockType::HARD);
    limiter->Release(1u, EResourceLimiterLockType::SOFT);

    limiter->Acquire(3u, EResourceLimiterLockType::SOFT);
    limiter->Release(3u, EResourceLimiterLockType::SOFT);
    limiter->Acquire(5u, EResourceLimiterLockType::HARD);
}
