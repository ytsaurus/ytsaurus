#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/actions/invoker_util.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/public.h>

#include <exception>

namespace NYT {
namespace NConcurrency {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPeriodicTest, Simple)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(20)));
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
    WaitFor(executor->Stop());
    EXPECT_EQ(2, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
    WaitFor(executor->Stop());
    EXPECT_EQ(4, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(20)));
    WaitFor(executor->GetExecutedEvent());
    EXPECT_EQ(6, count);
    WaitFor(executor->Stop());
}

TEST_W(TPeriodicTest, ParallelStop)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(30)));
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})));
    }
    EXPECT_EQ(1, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10)));
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        auto future3 = executor->Stop();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})));
    }
    EXPECT_EQ(2, count);
}

TEST_W(TPeriodicTest, ParallelOnExecuted1)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(30)));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})));
    }
    EXPECT_EQ(2, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(45)));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})));
    }
    EXPECT_EQ(4, count);
}

TEST_W(TPeriodicTest, ParallelOnExecuted2)
{
    int count = 0;

    auto callback = BIND([&] () {
        WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10)));
        ++count;
    });
    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(40));

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(30)));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2})));
    }
    EXPECT_EQ(2, count);

    executor->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10)));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(Combine(std::vector<TFuture<void>>({future1, future2, future3})));
    }
    EXPECT_EQ(3, count);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT
