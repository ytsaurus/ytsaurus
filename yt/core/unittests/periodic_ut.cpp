#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/actions/invoker_util.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/lazy_ptr.h>

#include <yt/core/logging/log.h>

#include <exception>
#include <atomic>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPeriodicTest, Simple)
{
    std::atomic<int> count = {0};
    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(200)))
            .ThrowOnError();
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(100));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(600)))
        .ThrowOnError();
    WaitFor(executor->Stop())
        .ThrowOnError();
    EXPECT_EQ(2, count.load());

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(600)))
        .ThrowOnError();
    WaitFor(executor->Stop())
        .ThrowOnError();
    EXPECT_EQ(4, count.load());

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(250)))
        .ThrowOnError();
    WaitFor(executor->GetExecutedEvent())
        .ThrowOnError();
    EXPECT_EQ(6, count.load());
    WaitFor(executor->Stop())
        .ThrowOnError();
}

TEST_W(TPeriodicTest, ParallelStop)
{
    std::atomic<int> count = {0};
    auto callback = BIND([&] () {
        ++count;
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(500)))
            .ThrowOnError();
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(300)))
        .ThrowOnError();
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(1, count.load());

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(300)))
        .ThrowOnError();
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        auto future3 = executor->Stop();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count.load());
}

TEST_W(TPeriodicTest, ParallelOnExecuted1)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(500)))
            .ThrowOnError();
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(300)))
        .ThrowOnError();
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(450)))
        .ThrowOnError();
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(4, count);
}

TEST_W(TPeriodicTest, ParallelOnExecuted2)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100)))
            .ThrowOnError();
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(400));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(300)))
        .ThrowOnError();
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(100)))
        .ThrowOnError();
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(3, count);
}

TEST_W(TPeriodicTest, Stop)
{
    auto neverSetPromise = NewPromise<void>();
    auto immediatelyCancelableFuture = neverSetPromise.ToFuture().ToImmediatelyCancelable();
    auto callback = BIND([&] {
        Y_UNUSED(WaitFor(immediatelyCancelableFuture));
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(100));

    executor->Start();
    // Wait for the callback to enter WaitFor.
    Sleep(TDuration::MilliSeconds(100));
    WaitFor(executor->Stop())
        .ThrowOnError();
    
    EXPECT_TRUE(immediatelyCancelableFuture.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, immediatelyCancelableFuture.Get().GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
