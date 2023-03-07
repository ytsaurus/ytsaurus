#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/config.h>

#include <yt/core/profiling/timing.h>

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

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100);
}

TEST(TReconfigurableThroughputThrottlerTest, TestLimit)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 50);

    throttler->Throttle(1).Get().ThrowOnError();
    throttler->Throttle(1).Get().ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 1000);
    EXPECT_LE(duration, 3000);
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
    EXPECT_GE(duration, 3000);
    EXPECT_LE(duration, 6000);
}

TEST(TReconfigurableThroughputThrottlerTest, TestUpdate)
{
    auto throttler = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(1));

    NProfiling::TWallTimer timer;

    throttler->Throttle(1).Get().ThrowOnError();
    Sleep(TDuration::Seconds(1));
    throttler->Throttle(1).Get().ThrowOnError();

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 2000);
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
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 100);
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

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 5000);
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
    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000); // Reconfigure must have rescheduled the update
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

    EXPECT_LE(timer.GetElapsedTime().MilliSeconds(), 3000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
