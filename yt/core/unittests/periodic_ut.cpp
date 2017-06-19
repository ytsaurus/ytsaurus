#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/actions/invoker_util.h>

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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT
