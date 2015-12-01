#include "framework.h"
#include "probe.h"

#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/public.h>

#include <exception>

namespace NYT {
namespace NConcurrency {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicTest
    : public ::testing::Test
{
public:
    void Heartbeat();

protected:
    TLazyIntrusivePtr<TActionQueue> Queue_;
    TPeriodicExecutorPtr Executor_;
    int64_t Count_;

    virtual void TearDown()
    {
        if (Queue_.HasValue()) {
            Queue_->Shutdown();
        }
        if (Executor_) {
            Executor_->Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void
TPeriodicTest::Heartbeat()
{
    ++Count_;
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(20)));
}

TEST_W(TPeriodicTest, Simple)
{
    auto invoker = Queue_->GetInvoker();
    Count_ = 0;
    Executor_ = New<TPeriodicExecutor>(
        invoker,
        BIND(&TPeriodicTest::Heartbeat, this),
        TDuration::MilliSeconds(10));
    Executor_->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
    WaitFor(Executor_->Stop());
    EXPECT_EQ(2, Count_);
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(20)));
    Executor_->Start();
    WaitFor(TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(50)));
    WaitFor(Executor_->Stop());
    EXPECT_EQ(4, Count_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT

