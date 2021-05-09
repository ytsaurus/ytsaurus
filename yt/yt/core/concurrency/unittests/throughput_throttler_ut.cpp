#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/profiling/timing.h>

#include <thread>
#include <vector>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TReconfigurableThroughputThrottlerTest, TestNoLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>());

    NProfiling::TWallTimer timer;
    for (int i = 0; i < 1000; ++i) {
        throttler->Throttle(1).Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 50u);

    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 1000u);
    EXPECT_LE(duration, 3000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestScheduleUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(3).Get().ThrowOnError();

    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 3000u);
    EXPECT_LE(duration, 6000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(1).Get().ThrowOnError();
    Sleep(TDuration::Seconds(1));
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 2000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestCancel)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(5).Get().ThrowOnError();
    auto future = throttler->Throttle(1);
    future.Cancel(TError("Error"));
    auto result = future.Get();

    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(result.GetCode() == NYT::EErrorCode::Canceled);
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestReconfigureSchedulesUpdatesProperly)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(100));
    }

    throttler->Reconfigure(New<TThroughputThrottlerConfig>(100));

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 5000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestSetLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(100));
    }

    throttler->SetLimit(100);

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 5000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestReconfigureMustRescheduleUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    throttler->Acquire(1); // drains throttler to zero

    NProfiling::TWallTimer timer;

    auto scheduled1 = throttler->Throttle(100); // sits in front of the queue
    auto scheduled2 = throttler->Throttle(100); // also waits in the queue

    throttler->Reconfigure(New<TThroughputThrottlerConfig>(100));

    EXPECT_FALSE(scheduled2.IsSet()); // must remain waiting in the queue after Reconfigure

    scheduled2.Get().ThrowOnError();
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000u); // Reconfigure must have rescheduled the update
}

TEST(TReconfigurableThroughputThrottlerTest, TestOverdraft)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(100));

    throttler->Throttle(150).Get().ThrowOnError();

    EXPECT_TRUE(throttler->IsOverdraft());
    Sleep(TDuration::Seconds(2));
    EXPECT_FALSE(throttler->IsOverdraft());
}

#if !defined(_asan_enabled_) && !defined(_msan_enabled_) && !defined(_tsan_enabled_)

TEST(TReconfigurableThroughputThrottlerTest, StressTest)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(100));

    const int N = 30;
    const int M = 10;

    NProfiling::TWallTimer timer;

    std::vector<std::thread> threads;
    for (int i = 0; i < N; i++) {
        threads.emplace_back([&] {
            for (int j = 0; j < M; ++j) {
                throttler->Throttle(1).Get().ThrowOnError();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000u);
}

#endif

TEST(TReconfigurableThroughputThrottlerTest, TestFractionalLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(0.5));

    NProfiling::TWallTimer timer;
    for (int i = 0; i < 2; ++i) {
        throttler->Throttle(1).Get().ThrowOnError();
    }
    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 1500u);
    EXPECT_LE(duration, 4000u);
}

TEST(TReconfigurableThroughputThrottlerTest, TestZeroLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(0));

    throttler->SetLimit(100);

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> scheduled;
    for (int i = 0; i < 4; ++i) {
        scheduled.push_back(throttler->Throttle(10));
    }

    for (const auto& future : scheduled) {
        future.Get().ThrowOnError();
    }

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 1000u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
