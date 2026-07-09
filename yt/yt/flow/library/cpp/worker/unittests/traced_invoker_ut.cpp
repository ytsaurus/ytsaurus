#include <yt/yt/flow/library/cpp/worker/traced_invoker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/lazy_ptr.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TExtendedCallback<void()> MakeCpuIntensiveCallback(int iterCount = 100'000'000)
{
    return BIND_NO_PROPAGATE([=] {
        for (int i = 0; i < iterCount; ++i) {
            DoNotOptimizeAway(i);
        }
    });
}

//! This callback is used to make sure CurrentContextGuard inside TracedInvoker has called its destructor
//! and accounted for the duration of the previous callback.
TExtendedCallback<void()> MakeTraceContextTimeFlushCallback()
{
    return BIND_NO_PROPAGATE([] {
    });
}

TExtendedCallback<void()> MakeIdleCallback(TDuration idleTime = TDuration::MilliSeconds(100))
{
    return BIND_NO_PROPAGATE([=] {
        TDelayedExecutor::WaitForDuration(idleTime);
    });
}

double ContextCpuUtilization(const TTraceContextPtr& traceContext)
{
    auto duration = traceContext->IsFinished() ? traceContext->GetDuration() : TInstant::Now() - traceContext->GetStartTime();
    auto result = traceContext->GetElapsedTime().SecondsFloat() / duration.SecondsFloat();
    return result;
}

double MeasureCallbackUtilization(TExtendedCallback<void()> callback, IInvokerPtr invoker, std::string spanName = "testing")
{
    auto traceContext = TTraceContext::NewRoot(spanName);
    auto tracedInvoker = CreateTracedInvoker(invoker, traceContext);

    auto future = callback
        .AsyncVia(tracedInvoker)
        .Run();

    Y_UNUSED(WaitFor(future));

    traceContext->Finish();

    auto flushFuture = MakeTraceContextTimeFlushCallback()
        .AsyncVia(invoker)
        .Run();

    WaitFor(flushFuture)
        .ThrowOnError();

    return ContextCpuUtilization(traceContext);
}

class TTracedInvokerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue;

    void TearDown() override
    {
        if (Queue.HasValue()) {
            Queue->Shutdown();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTracedInvokerTest, Basic)
{
    auto utilization1 = MeasureCallbackUtilization(MakeCpuIntensiveCallback(), Queue->GetInvoker(), "span1");
    EXPECT_GE(utilization1, 0);
    EXPECT_LE(utilization1, 1);

    auto utilization2 = MeasureCallbackUtilization(MakeIdleCallback(), Queue->GetInvoker(), "span2");
    EXPECT_GE(utilization1, utilization2);
}

TEST_F(TTracedInvokerTest, BasicThreadPool)
{
    auto threadPool = CreateThreadPool(4, "Test");

    auto traceContext1 = TTraceContext::NewRoot("testing1");
    auto invoker1 = CreateTracedInvoker(threadPool->GetInvoker(), traceContext1);
    auto traceContext2 = TTraceContext::NewRoot("testing2");
    auto invoker2 = CreateTracedInvoker(threadPool->GetInvoker(), traceContext2);

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < threadPool->GetThreadCount(); ++i) {
        futures.push_back(MakeCpuIntensiveCallback().AsyncVia(invoker1).Run());
    }
    futures.push_back(MakeCpuIntensiveCallback().AsyncVia(invoker2).Run());

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();

    auto utilization1 = ContextCpuUtilization(traceContext1);
    auto utilization2 = ContextCpuUtilization(traceContext2);
    EXPECT_GE(utilization1, utilization2);
}

TEST_F(TTracedInvokerTest, ThreadPoolMaxUtilization)
{
    auto traceContext = TTraceContext::NewRoot("testing");
    auto threadPool = CreateThreadPool(4, "Test");
    auto invoker = CreateTracedInvoker(threadPool->GetInvoker(), traceContext);

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < threadPool->GetThreadCount(); ++i) {
        futures.push_back(MakeCpuIntensiveCallback().AsyncVia(invoker).Run());
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();

    auto utilization = ContextCpuUtilization(traceContext);
    EXPECT_GT(utilization, 1);
    EXPECT_LE(utilization, threadPool->GetThreadCount());
}

TEST_F(TTracedInvokerTest, InProgressUtilization)
{
    auto traceContext = TTraceContext::NewRoot("testing");
    auto invoker = CreateTracedInvoker(Queue->GetInvoker(), traceContext);

    EXPECT_EQ(ContextCpuUtilization(traceContext), 0);

    auto future = MakeCpuIntensiveCallback()
        .AsyncVia(invoker)
        .Run();

    EXPECT_EQ(ContextCpuUtilization(traceContext), 0);

    WaitFor(future)
        .ThrowOnError();

    EXPECT_GE(ContextCpuUtilization(traceContext), 0);
}

TEST_F(TTracedInvokerTest, ThreadSleepCountsAsUtilization)
{
    auto callback = BIND_NO_PROPAGATE([] {
        Sleep(TDuration::MilliSeconds(100));
    });
    auto sleepUtilization = MeasureCallbackUtilization(callback, Queue->GetInvoker());
    auto idleUtilization = MeasureCallbackUtilization(MakeIdleCallback(), Queue->GetInvoker());

    EXPECT_GE(sleepUtilization, idleUtilization);
}

TEST_F(TTracedInvokerTest, UtilizationTrackingContinuesAfterContextSwitch)
{
    auto callback = BIND_NO_PROPAGATE([] {
        Yield();
        MakeCpuIntensiveCallback().Run();
    });
    auto yieldUtilization = MeasureCallbackUtilization(callback, Queue->GetInvoker());
    auto idleUtilization = MeasureCallbackUtilization(MakeIdleCallback(), Queue->GetInvoker());

    EXPECT_GE(yieldUtilization, idleUtilization);
}

TEST_F(TTracedInvokerTest, CallbackWithException)
{
    auto callbackWithException = BIND_NO_PROPAGATE([] {
        MakeCpuIntensiveCallback().Run();
        THROW_ERROR_EXCEPTION("Exception from callback");
    });
    auto exceptionUtilization = MeasureCallbackUtilization(callbackWithException, Queue->GetInvoker());
    auto defaultUtilization = MeasureCallbackUtilization(MakeCpuIntensiveCallback(), Queue->GetInvoker());
    EXPECT_LE(std::abs(exceptionUtilization - defaultUtilization) / defaultUtilization, 0.1);
}

TEST_F(TTracedInvokerTest, BindInsideBindIsTracked)
{
    auto bindInsideBindCallback = BIND_NO_PROPAGATE([] {
        BIND([&] {
            MakeCpuIntensiveCallback().Run();
        }).Run();
    });
    auto bindInsideBindUtilization = MeasureCallbackUtilization(bindInsideBindCallback, Queue->GetInvoker());
    auto defaultUtilization = MeasureCallbackUtilization(MakeCpuIntensiveCallback(), Queue->GetInvoker());

    EXPECT_LE(std::abs(bindInsideBindUtilization - defaultUtilization) / defaultUtilization, 0.1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
