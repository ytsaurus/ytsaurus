#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/logging/log.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/misc/lazy_ptr.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/misc/finally.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

#include <exception>

namespace NYT::NConcurrency {

using ::testing::ContainsRegex;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(100);

const NLogging::TLogger Logger("SchedulerTest");

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> MakeDelayedFuture(T x)
{
    return TDelayedExecutor::MakeDelayed(SleepQuantum)
        .Apply(BIND([=] () { return x; }));
}

class TSchedulerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue1;
    TLazyIntrusivePtr<TActionQueue> Queue2;

    virtual void TearDown()
    {
        if (Queue1.HasValue()) {
            Queue1->Shutdown();
        }
        if (Queue2.HasValue()) {
            Queue2->Shutdown();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

int RecursiveFunction(size_t maxDepth, size_t currentDepth)
{
    if (currentDepth >= maxDepth) {
        return 0;
    }

    if (!CheckFreeStackSpace(40 * 1024)) {
        THROW_ERROR_EXCEPTION("Evaluation depth causes stack overflow");
    }

    std::array<int, 4 * 1024> array;

    for (size_t i = 0; i < array.size(); ++i) {
        array[i] = rand();
    }

    return std::accumulate(array.begin(), array.end(), 0)
        + RecursiveFunction(maxDepth, currentDepth + 1);
}

TEST_W(TSchedulerTest, CheckFiberStack)
{
    auto asyncResult1 = BIND(&RecursiveFunction, 10, 0)
        .AsyncVia(Queue1->GetInvoker())
        .Run();

    WaitFor(asyncResult1).ThrowOnError();

#ifdef _asan_enabled_
    constexpr size_t tooLargeDepth = 160;
#else
    constexpr size_t tooLargeDepth = 20;
#endif

    auto asyncResult2 = BIND(&RecursiveFunction, tooLargeDepth, 0)
        .AsyncVia(Queue1->GetInvoker())
        .Run();

    EXPECT_THROW_THAT(
        WaitFor(asyncResult2).ThrowOnError(),
        ContainsRegex("Evaluation depth causes stack overflow"));
}

TEST_W(TSchedulerTest, SimpleAsync)
{
    auto asyncA = MakeDelayedFuture(3);
    int a = WaitFor(asyncA).ValueOrThrow();

    auto asyncB = MakeDelayedFuture(4);
    int b = WaitFor(asyncB).ValueOrThrow();

    EXPECT_EQ(7, a + b);
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

TEST_W(TSchedulerTest, SwitchToInvoker1)
{
    auto invoker = Queue1->GetInvoker();

    auto id0 = TThread::CurrentThreadId();
    auto id1 = invoker->GetThreadId();

    EXPECT_NE(id0, id1);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker);
        EXPECT_EQ(TThread::CurrentThreadId(), id1);
    }
}

TEST_W(TSchedulerTest, SwitchToInvoker2)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto id0 = TThread::CurrentThreadId();
    auto id1 = invoker1->GetThreadId();
    auto id2 = invoker2->GetThreadId();

    EXPECT_NE(id0, id1);
    EXPECT_NE(id0, id2);
    EXPECT_NE(id1, id2);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker1);
        EXPECT_EQ(TThread::CurrentThreadId(), id1);

        SwitchTo(invoker2);
        EXPECT_EQ(TThread::CurrentThreadId(), id2);
    }
}

#endif

TEST_W(TSchedulerTest, SwitchToCancelableInvoker1)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetCurrentInvoker());

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, SwitchToCancelableInvoker2)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = context->CreateInvoker(Queue2->GetInvoker());

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker2); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, SwitchToCancelableInvoker3)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = context->CreateInvoker(Queue2->GetInvoker());

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    EXPECT_NO_THROW({ SwitchTo(invoker2); });

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker2); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, WaitForCancelableInvoker1)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    TDelayedExecutor::Submit(
        BIND([=] () mutable {
            context->Cancel(TError("Error"));
            promise.Set();
        }),
        SleepQuantum);
    WaitFor(BIND([=] () {
            EXPECT_THROW({ WaitFor(future).ThrowOnError(); }, TFiberCanceledException);
        })
        .AsyncVia(invoker)
        .Run()).ThrowOnError();
}

TEST_W(TSchedulerTest, WaitForCancelableInvoker2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    WaitFor(BIND([=] () mutable {
            context->Cancel(TError("Error"));
            promise.Set();
            EXPECT_THROW({ WaitFor(future).ThrowOnError(); }, TFiberCanceledException);
        })
        .AsyncVia(invoker)
        .Run()).ThrowOnError();
}

TEST_W(TSchedulerTest, TerminatedCaught)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel(TError("Error"));

    EXPECT_THROW({ SwitchTo(invoker1); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, TerminatedPropagated)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel(TError("Error"));

    SwitchTo(invoker1);

    YT_ABORT();
}

TEST_W(TSchedulerTest, CurrentInvokerAfterSwitch1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

TEST_W(TSchedulerTest, CurrentInvokerAfterSwitch2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

TEST_W(TSchedulerTest, InvokerAffinity1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    VERIFY_INVOKER_AFFINITY(invoker);
}

TEST_W(TSchedulerTest, InvokerAffinity2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    VERIFY_INVOKER_AFFINITY(invoker);
    VERIFY_INVOKER_AFFINITY(Queue1->GetInvoker());
}

#endif

TEST_F(TSchedulerTest, CurrentInvokerSync)
{
    EXPECT_EQ(GetSyncInvoker(), GetCurrentInvoker())
        << "Current invoker: " << typeid(*GetCurrentInvoker()).name();
}

TEST_F(TSchedulerTest, CurrentInvokerInActionQueue)
{
    auto invoker = Queue1->GetInvoker();
    BIND([=] () {
        EXPECT_EQ(invoker, GetCurrentInvoker());
    })
    .AsyncVia(invoker).Run()
    .Get();
}

TEST_F(TSchedulerTest, Intercept)
{
    auto invoker = Queue1->GetInvoker();
    int counter1 = 0;
    int counter2 = 0;
    BIND([&] () {
        TContextSwitchGuard guard(
            [&] {
                EXPECT_EQ(counter1, 0);
                EXPECT_EQ(counter2, 0);
                ++counter1;
            },
            [&] {
                EXPECT_EQ(counter1, 1);
                EXPECT_EQ(counter2, 0);
                ++counter2;
            });
        TDelayedExecutor::WaitForDuration(SleepQuantum);
    })
    .AsyncVia(invoker).Run()
    .Get();
    EXPECT_EQ(counter1, 1);
    EXPECT_EQ(counter2, 1);
}

TEST_F(TSchedulerTest, InterceptEnclosed)
{
    auto invoker = Queue1->GetInvoker();
    int counter1 = 0;
    int counter2 = 0;
    int counter3 = 0;
    int counter4 = 0;
    BIND([&] () {
        {
            TContextSwitchGuard guard(
                [&] { ++counter1; },
                [&] { ++counter2; });
            TDelayedExecutor::WaitForDuration(SleepQuantum);
            {
                TContextSwitchGuard guard2(
                    [&] { ++counter3; },
                    [&] { ++counter4; });
                TDelayedExecutor::WaitForDuration(SleepQuantum);
            }
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
        TDelayedExecutor::WaitForDuration(SleepQuantum);
    })
    .AsyncVia(invoker).Run()
    .Get();
    EXPECT_EQ(counter1, 3);
    EXPECT_EQ(counter2, 3);
    EXPECT_EQ(counter3, 1);
    EXPECT_EQ(counter4, 1);
}

TEST_F(TSchedulerTest, CurrentInvokerConcurrent)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto result1 = BIND([=] () {
        EXPECT_EQ(invoker1, GetCurrentInvoker());
    }).AsyncVia(invoker1).Run();

    auto result2 = BIND([=] () {
        EXPECT_EQ(invoker2, GetCurrentInvoker());
    }).AsyncVia(invoker2).Run();

    result1.Get();
    result2.Get();
}

TEST_W(TSchedulerTest, WaitForAsyncVia)
{
    auto invoker = Queue1->GetInvoker();

    auto x = BIND([&] () { }).AsyncVia(invoker).Run();

    WaitFor(x)
        .ThrowOnError();
}

// Various invokers.

TEST_F(TSchedulerTest, WaitForInSerializedInvoker1)
{
    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
    }).AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInSerializedInvoker2)
{
    // NB! This may be confusing, but serialized invoker is expected to start
    // executing next action if current action is blocked on WaitFor.

    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    std::vector<TFuture<void>> futures;

    bool finishedFirstAction = false;
    futures.emplace_back(BIND([&] () {
        TDelayedExecutor::WaitForDuration(SleepQuantum);
        finishedFirstAction = true;
    }).AsyncVia(invoker).Run());

    futures.emplace_back(BIND([&] () {
        if (finishedFirstAction) {
            THROW_ERROR_EXCEPTION("Serialization error");
        }
    }).AsyncVia(invoker).Run());

    Combine(futures).Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker1)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            TDelayedExecutor::WaitForDuration(SleepQuantum);
        }
    }).AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker2)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 2);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto a1 = BIND([&] () {
        promise.Set();
    });

    auto a2 = BIND([&] () {
        invoker->Invoke(a1);
        WaitFor(future)
            .ThrowOnError();
    });

    a2.AsyncVia(invoker).Run().Get().ThrowOnError();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker3)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);

    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    bool a1called = false;
    bool a1finished = false;
    auto a1 = BIND([&] () {
        a1called = true;
        WaitFor(future)
            .ThrowOnError();
        a1finished = true;
    });

    bool a2called = false;
    auto a2 = BIND([&] () {
        a2called = true;
    });

    invoker->Invoke(a1);
    invoker->Invoke(a2);

    Sleep(SleepQuantum);
    EXPECT_TRUE(a1called);
    EXPECT_FALSE(a1finished);
    EXPECT_FALSE(a2called);

    promise.Set();

    Sleep(SleepQuantum);
    EXPECT_TRUE(a1called);
    EXPECT_TRUE(a1finished);
    EXPECT_TRUE(a2called);
}

TEST_F(TSchedulerTest, PropagateFiberCancelationToFuture)
{
    auto p1 = NewPromise<void>();
    auto f1 = p1.ToFuture();

    auto a = BIND([=] () mutable {
        WaitUntilSet(f1);
    });

    auto f2 = a.AsyncVia(Queue1->GetInvoker()).Run();

    Sleep(SleepQuantum);

    f2.Cancel(TError("Error"));

    Sleep(SleepQuantum);

    EXPECT_TRUE(p1.IsCanceled());
}

TEST_F(TSchedulerTest, FiberUnwindOrder)
{
    auto p1 = NewPromise<void>();
    // Add empty callback
    p1.OnCanceled(BIND([] (const TError& error) { }));
    auto f1 = p1.ToFuture();

    auto f2 = BIND([=] () mutable {
        auto finally = Finally([&] {
            EXPECT_TRUE(f1.IsSet());
        });

        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));

        WaitUntilSet(f1);
    }).AsyncVia(Queue1->GetInvoker()).Run();

    Sleep(SleepQuantum);
    EXPECT_FALSE(f2.IsSet());

    p1.Set();
    Sleep(SleepQuantum);
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST_F(TSchedulerTest, TestWaitUntilSet)
{
    auto p1 = NewPromise<void>();
    auto f1 = p1.ToFuture();

    BIND([=] () {
        Sleep(SleepQuantum);
        p1.Set();
    }).AsyncVia(Queue1->GetInvoker()).Run();

    WaitUntilSet(f1);
    EXPECT_TRUE(f1.IsSet());
    EXPECT_TRUE(f1.Get().IsOK());
}

TEST_F(TSchedulerTest, AsyncViaCanceledBeforeStart)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([] () {
        Sleep(SleepQuantum * 10);
    }).AsyncVia(invoker).Run();
    auto asyncResult2 = BIND([] () {
        Sleep(SleepQuantum * 10);
    }).AsyncVia(invoker).Run();
    EXPECT_FALSE(asyncResult1.IsSet());
    EXPECT_FALSE(asyncResult2.IsSet());
    asyncResult2.Cancel(TError("Error"));
    EXPECT_TRUE(asyncResult1.Get().IsOK());
    Sleep(SleepQuantum);
    EXPECT_TRUE(asyncResult2.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, asyncResult2.Get().GetCode());
}

TEST_F(TSchedulerTest, CancelCurrentFiber)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult = BIND([=] () {
        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
        SwitchTo(invoker);
    }).AsyncVia(invoker).Run();
    asyncResult.Get();
    EXPECT_TRUE(asyncResult.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, asyncResult.Get().GetCode());
}

TEST_F(TSchedulerTest, YieldToFromCanceledFiber)
{
    auto promise = NewPromise<void>();
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto asyncResult = BIND([=] () mutable {
        BIND([=] () {
            NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
        }).AsyncVia(invoker2).Run().Get();
        WaitFor(promise.ToFuture(), invoker2)
            .ThrowOnError();
    }).AsyncVia(invoker1).Run();

    promise.Set();
    asyncResult.Get();

    EXPECT_TRUE(asyncResult.IsSet());
    EXPECT_TRUE(asyncResult.Get().IsOK());
}

TEST_F(TSchedulerTest, JustYield1)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult = BIND([] () {
        for (int i = 0; i < 10; ++i) {
            Yield();
        }
    }).AsyncVia(invoker).Run().Get();
    EXPECT_TRUE(asyncResult.IsOK());
}

TEST_F(TSchedulerTest, JustYield2)
{
    auto invoker = Queue1->GetInvoker();

    bool flag = false;

    auto asyncResult = BIND([&] () {
        for (int i = 0; i < 2; ++i) {
            Sleep(SleepQuantum);
            Yield();
        }
        flag = true;
    }).AsyncVia(invoker).Run();

    // This callback must complete before the first.
    auto errorOrValue = BIND([&] () {
        return flag;
    }).AsyncVia(invoker).Run().Get();

    EXPECT_TRUE(errorOrValue.IsOK());
    EXPECT_FALSE(errorOrValue.Value());
    EXPECT_TRUE(asyncResult.Get().IsOK());
}

TEST_F(TSchedulerTest, CancelInAdjacentCallback)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([=] () {
        NYT::NConcurrency::GetCurrentFiberCanceler().Run(TError("Error"));
    }).AsyncVia(invoker).Run().Get();
    auto asyncResult2 = BIND([=] () {
        Yield();
    }).AsyncVia(invoker).Run().Get();
    EXPECT_TRUE(asyncResult1.IsOK());
    EXPECT_TRUE(asyncResult2.IsOK());
}

TEST_F(TSchedulerTest, CancelInAdjacentThread)
{
    auto closure = TCallback<void(const TError&)>();
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([=, &closure] () {
        closure = NYT::NConcurrency::GetCurrentFiberCanceler();
    }).AsyncVia(invoker).Run().Get();
    closure.Run(TError("Error")); // *evil laugh*
    auto asyncResult2 = BIND([=] () {
        Yield();
    }).AsyncVia(invoker).Run().Get();
    closure.Reset(); // *evil smile*
    EXPECT_TRUE(asyncResult1.IsOK());
    EXPECT_TRUE(asyncResult2.IsOK());
}

TEST_F(TSchedulerTest, SerializedDoubleWaitFor)
{
    std::atomic<bool> flag(false);

    auto threadPool = New<TThreadPool>(3, "MyPool");
    auto serializedInvoker = CreateSerializedInvoker(threadPool->GetInvoker());

    auto promise = NewPromise<void>();

    BIND([&] () {
        WaitFor(VoidFuture)
            .ThrowOnError();
        WaitFor(VoidFuture)
            .ThrowOnError();
        promise.Set();

        Sleep(SleepQuantum);
        flag = true;
    })
    .Via(serializedInvoker)
    .Run();

    promise.ToFuture().Get();

    auto result = BIND([&] () -> bool {
        return flag;
    })
    .AsyncVia(serializedInvoker)
    .Run()
    .Get()
    .ValueOrThrow();

    EXPECT_TRUE(result);
}

void CheckCurrentFiberRunDuration(TDuration actual, TDuration lo, TDuration hi)
{
    EXPECT_LE(actual, hi);
    EXPECT_GE(actual, lo);
}

TEST_W(TSchedulerTest, FiberTiming)
{
    NProfiling::TFiberWallTimer timer;

    CheckCurrentFiberRunDuration(timer.GetElapsedTime(), TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
    Sleep(TDuration::Seconds(1));
    CheckCurrentFiberRunDuration(timer.GetElapsedTime(),TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    CheckCurrentFiberRunDuration(timer.GetElapsedTime(),TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

TEST_W(TSchedulerTest, CancelDelayedFuture)
{
    auto future = TDelayedExecutor::MakeDelayed(TDuration::Seconds(10));
    future.Cancel(TError("Error"));
    EXPECT_TRUE(future.IsSet());
    auto error = future.Get();
    EXPECT_EQ(NYT::EErrorCode::Canceled, error.GetCode());
    EXPECT_EQ(1, error.InnerErrors().size());
    EXPECT_EQ(NYT::EErrorCode::Generic, error.InnerErrors()[0].GetCode());
}

void CheckTraceContextTime(const NTracing::TTraceContextPtr& traceContext, TDuration lo, TDuration hi)
{
    auto actual = traceContext->GetElapsedTime();
    EXPECT_LE(actual, hi);
    EXPECT_GE(actual, lo);
}

TEST_W(TSchedulerTest, TraceContextZeroTiming)
{
    auto traceContext = NTracing::CreateRootTraceContext("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        Sleep(TDuration::Seconds(0));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
}

TEST_W(TSchedulerTest, TraceContextThreadSleepTiming)
{
    auto traceContext = NTracing::CreateRootTraceContext("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        Sleep(TDuration::Seconds(1));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

TEST_W(TSchedulerTest, TraceContextFiberSleepTiming)
{
    auto traceContext = NTracing::CreateRootTraceContext("Test");

    {
        NTracing::TTraceContextGuard guard(traceContext);
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(0), TDuration::MilliSeconds(100));
}

TEST_W(TSchedulerTest, TraceContextTimingPropagationViaBind)
{
    auto traceContext = NTracing::CreateRootTraceContext("Test");
    auto actionQueue = New<TActionQueue>();

    {
        NTracing::TTraceContextGuard guard(traceContext);
        auto asyncResult = BIND([] {
            Sleep(TDuration::MilliSeconds(700));
        })
            .AsyncVia(actionQueue->GetInvoker())
            .Run();
        Sleep(TDuration::MilliSeconds(300));
        WaitFor(asyncResult)
            .ThrowOnError();
    }

    CheckTraceContextTime(traceContext, TDuration::MilliSeconds(900), TDuration::MilliSeconds(1100));
}

////////////////////////////////////////////////////////////////////////////////

class TSuspendableInvokerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue1;

    virtual void TearDown()
    {
        if (Queue1.HasValue()) {
            Queue1->Shutdown();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSuspendableInvokerTest, PollSuspendFuture)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum * 10);
        flag = true;
    })
    .Via(suspendableInvoker)
    .Run();

    auto future = suspendableInvoker->Suspend();

    while (!flag) {
        EXPECT_EQ(flag, future.IsSet());
        Sleep(SleepQuantum);
    }
    Sleep(SleepQuantum);
    EXPECT_EQ(flag, future.IsSet());
}

TEST_F(TSuspendableInvokerTest, SuspendableDoubleWaitFor)
{
    std::atomic<bool> flag(false);

    auto threadPool = New<TThreadPool>(3, "MyPool");
    auto suspendableInvoker = CreateSuspendableInvoker(threadPool->GetInvoker());

    auto promise = NewPromise<void>();

    auto setFlagFuture = BIND([&] () {
        WaitFor(VoidFuture)
            .ThrowOnError();
        Sleep(SleepQuantum);
        WaitFor(VoidFuture)
            .ThrowOnError();
        promise.Set();

        Sleep(SleepQuantum * 10);
        flag = true;
    })
    .AsyncVia(suspendableInvoker)
    .Run();

    suspendableInvoker->Suspend().Get();
    EXPECT_FALSE(promise.ToFuture().IsSet());
    suspendableInvoker->Resume();
    promise.ToFuture().Get();

    setFlagFuture.Get();

    auto flagValue = BIND([&] () -> bool {
        return flag;
    })
    .AsyncVia(suspendableInvoker)
    .Run()
    .Get()
    .ValueOrThrow();

    EXPECT_TRUE(flagValue);
}

TEST_F(TSuspendableInvokerTest, EarlySuspend)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    suspendableInvoker->Suspend().Get();

    auto promise = NewPromise<void>();

    BIND([&] () {
        promise.Set();
    })
    .Via(suspendableInvoker)
    .Run();

    EXPECT_FALSE(promise.IsSet());
    suspendableInvoker->Resume();
    promise.ToFuture().Get();
    EXPECT_TRUE(promise.IsSet());
}

TEST_F(TSuspendableInvokerTest, ResumeBeforeFullSuspend)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum);
    })
    .Via(suspendableInvoker)
    .Run();

    auto firstFuture = suspendableInvoker->Suspend();

    EXPECT_FALSE(firstFuture.IsSet());
    suspendableInvoker->Resume();
    EXPECT_FALSE(firstFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, AllowSuspendOnContextSwitch)
{
    std::atomic<bool> flag(false);

    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    auto setFlagFuture = BIND([&] () {
        Sleep(SleepQuantum);
        WaitUntilSet(future);
        flag = true;
    })
    .AsyncVia(suspendableInvoker)
    .Run();

    auto suspendFuture = suspendableInvoker->Suspend();
    EXPECT_FALSE(suspendFuture.IsSet());
    EXPECT_TRUE(suspendFuture.Get().IsOK());

    suspendableInvoker->Resume();
    promise.Set();
    setFlagFuture.Get();
    EXPECT_TRUE(flag);
}

TEST_F(TSuspendableInvokerTest, SuspendResumeOnFinishedRace)
{
    std::atomic<bool> flag(false);
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        for (int i = 0; i < 100; ++i) {
            Sleep(TDuration::MilliSeconds(1));
            Yield();
        }
    })
    .Via(suspendableInvoker)
    .Run();

    int hits = 0;
    while (hits < 100) {
        flag = false;
        auto future = suspendableInvoker->Suspend()
            .Apply(BIND([=, &flag] () { flag = true; }));

        if (future.IsSet()) {
            ++hits;
        }
        suspendableInvoker->Resume();
        auto error = future.Get();
        if (!error.IsOK()) {
            EXPECT_FALSE(flag);
            YT_VERIFY(error.GetCode() == EErrorCode::Canceled);
        } else {
            EXPECT_TRUE(flag);
        }
    }
    auto future = suspendableInvoker->Suspend();
    EXPECT_TRUE(future.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, ResumeInApply)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    BIND([&] () {
        Sleep(SleepQuantum);
    })
    .Via(suspendableInvoker)
    .Run();

    auto suspendFuture = suspendableInvoker->Suspend()
        .Apply(BIND([=] () { suspendableInvoker->Resume(); }));

    EXPECT_TRUE(suspendFuture.Get().IsOK());
}

TEST_F(TSuspendableInvokerTest, VerifySerializedActionsOrder)
{
    auto suspendableInvoker = CreateSuspendableInvoker(Queue1->GetInvoker());

    suspendableInvoker->Suspend()
        .Get();

    const int totalActionCount = 100000;

    std::atomic<int> actionIndex = {0};
    std::atomic<int> reorderingCount = {0};

    for (int i = 0; i < totalActionCount / 2; ++i) {
        BIND([&actionIndex, &reorderingCount, i] () {
            reorderingCount += (actionIndex != i);
            ++actionIndex;
        })
        .Via(suspendableInvoker)
        .Run();
    }

    TDelayedExecutor::Submit(
        BIND([&] () {
            suspendableInvoker->Resume();
        }),
        SleepQuantum / 10);

    for (int i = totalActionCount / 2; i < totalActionCount; ++i) {
        BIND([&actionIndex, &reorderingCount, i] () {
            reorderingCount += (actionIndex != i);
            ++actionIndex;
        })
        .Via(suspendableInvoker)
        .Run();
    }

    while (actionIndex < totalActionCount) {
        Sleep(SleepQuantum);
    }
    EXPECT_EQ(actionIndex, totalActionCount);
    EXPECT_EQ(reorderingCount, 0);
}


class TFairShareSchedulerTest
    : public TSchedulerTest
    , public ::testing::WithParamInterface<std::tuple<int, int, int, TDuration>>
{ };

using NProfiling::GetCpuInstant;
using NProfiling::CpuDurationToDuration;

const auto FSSleepQuantum = SleepQuantum * 3;
const auto FSWorkTime = FSSleepQuantum * 5;

TEST_P(TFairShareSchedulerTest, Test)
{
    size_t numThreads = std::get<0>(GetParam());
    size_t numWorkers = std::get<1>(GetParam());
    size_t numPools = std::get<2>(GetParam());
    auto work = std::get<3>(GetParam());


    YT_VERIFY(numWorkers > 0);
    YT_VERIFY(numThreads > 0);
    YT_VERIFY(numPools > 0);
    YT_VERIFY(numWorkers > numPools);
    YT_VERIFY(numThreads <= numWorkers);

    auto threadPool = CreateTwoLevelFairShareThreadPool(numThreads, "MyFairSharePool");

    std::vector<TDuration> progresses(numWorkers);
    std::vector<TDuration> pools(numPools);

    auto getShift = [&] (size_t id) {
        return FSSleepQuantum * (id + 1) / 2 / numWorkers;
    };

    std::vector<TFuture<void>> futures;

    TSpinLock lock;

    for (size_t id = 0; id < numWorkers; ++id) {
        auto invoker = threadPool->GetInvoker(Format("pool%v", id % numPools), 1.0, Format("worker%v", id));
        auto worker = [&, id] () mutable {

            auto instant = GetCpuInstant();

            auto initialShift = getShift(id);
            {
                TGuard<TSpinLock> guard(lock);

                pools[id % numPools] += initialShift;
                progresses[id] += initialShift;
            }

            Sleep(initialShift - CpuDurationToDuration(GetCpuInstant() - instant));
            EXPECT_LE(CpuDurationToDuration(GetCpuInstant() - instant), initialShift * 1.1);
            Yield();

            while (progresses[id] < work + initialShift) {
                auto instant = GetCpuInstant();
                {
                    TGuard<TSpinLock> guard(lock);

                    if (numThreads == 1) {
                        auto minPool = TDuration::Max();
                        auto minPoolIndex = numPools;
                        for (size_t id = 0; id < numPools; ++id) {
                            bool hasBucketsInPool = false;
                            for (size_t workerId = id; workerId < numWorkers; workerId += numPools) {
                                if (progresses[workerId] < work + getShift(workerId)) {
                                    hasBucketsInPool = true;
                                }
                            }
                            if (hasBucketsInPool && pools[id] < minPool) {
                                minPool = pools[id];
                                minPoolIndex = id;
                            }
                        }

                        if (pools[id % numPools].MilliSeconds() != minPool.MilliSeconds()) {
                            YT_LOG_TRACE("Pools time: [%v]",
                                MakeFormattableView(
                                    pools,
                                    [&] (auto* builder, const auto& pool) {
                                        builder->AppendFormat("%v", pool.MilliSeconds());
                                    }));
                        }

                        EXPECT_EQ(pools[id % numPools].MilliSeconds(), minPool.MilliSeconds());

                        auto min = TDuration::Max();
                        for (size_t id = minPoolIndex; id < numWorkers; id += numPools) {
                            if (progresses[id] < min && progresses[id] < work + getShift(id)) {
                                min = progresses[id];
                            }
                        }

                        if (progresses[id].MilliSeconds() != min.MilliSeconds()) {
                            YT_LOG_TRACE("Pools time: [%v]",
                                MakeFormattableView(
                                    pools,
                                    [&] (auto* builder, const auto& pool) {
                                        builder->AppendFormat("%v", pool.MilliSeconds());
                                    }));

                            YT_LOG_TRACE("Progresses time: [%v]",
                                MakeFormattableView(
                                    progresses,
                                    [&] (auto* builder, const auto& progress) {
                                        builder->AppendFormat("%v", progress.MilliSeconds());
                                    }));
                        }

                        EXPECT_EQ(progresses[id].MilliSeconds(), min.MilliSeconds());
                    }

                    pools[id % numPools] += FSSleepQuantum;
                    progresses[id] += FSSleepQuantum;
                }

                Sleep(FSSleepQuantum - CpuDurationToDuration(GetCpuInstant() - instant));
                EXPECT_LE(CpuDurationToDuration(GetCpuInstant() - instant), FSSleepQuantum * 1.1);
                Yield();
            }
        };

        auto result = BIND(worker)
            .AsyncVia(invoker)
            .Run();

        futures.push_back(result);
    }

    WaitFor(Combine(futures))
        .ThrowOnError();
}

TEST_P(TFairShareSchedulerTest, Test2)
{
    size_t numThreads = std::get<0>(GetParam());
    size_t numWorkers = std::get<1>(GetParam());
    size_t numPools = std::get<2>(GetParam());
    auto work = std::get<3>(GetParam());


    YT_VERIFY(numWorkers > 0);
    YT_VERIFY(numThreads > 0);
    YT_VERIFY(numPools > 0);
    YT_VERIFY(numWorkers > numPools);
    YT_VERIFY(numThreads <= numWorkers);

    if (numPools != 1) {
        return;
    }

    auto threadPool = CreateFairShareThreadPool(numThreads, "MyFairSharePool");

    std::vector<TDuration> progresses(numWorkers);

    auto getShift = [&] (size_t id) {
        return FSSleepQuantum * (id + 1) / 2 / numWorkers;
    };

    std::vector<TFuture<void>> futures;

    TSpinLock lock;

    for (size_t id = 0; id < numWorkers; ++id) {
        auto invoker = threadPool->GetInvoker(Format("worker%v", id));
        auto worker = [&, id] () mutable {

            auto instant = GetCpuInstant();

            auto initialShift = getShift(id);
            {
                TGuard<TSpinLock> guard(lock);
                progresses[id] += initialShift;
            }

            Sleep(initialShift - CpuDurationToDuration(GetCpuInstant() - instant));
            EXPECT_LE(CpuDurationToDuration(GetCpuInstant() - instant), initialShift * 1.1);
            Yield();

            while (progresses[id] < work + initialShift) {
                auto instant = GetCpuInstant();
                {
                    TGuard<TSpinLock> guard(lock);

                    if (numThreads == 1) {
                        auto min = TDuration::Max();
                        for (size_t id = 0; id < numWorkers; ++id) {
                            if (progresses[id] < min && progresses[id] < work + getShift(id)) {
                                min = progresses[id];
                            }
                        }

                        if (progresses[id].MilliSeconds() != min.MilliSeconds()) {
                            YT_LOG_TRACE("Progresses time: [%v]",
                                MakeFormattableView(
                                    progresses,
                                    [&] (TStringBuilderBase* builder, const auto& progress) {
                                        builder->AppendFormat("%v", progress.MilliSeconds());
                                    }));
                        }

                        EXPECT_EQ(progresses[id].MilliSeconds(), min.MilliSeconds());
                    }

                    progresses[id] += FSSleepQuantum;
                }

                Sleep(FSSleepQuantum - CpuDurationToDuration(GetCpuInstant() - instant));
                EXPECT_LE(CpuDurationToDuration(GetCpuInstant() - instant), FSSleepQuantum * 1.1);

                Yield();
            }
        };

        auto result = BIND(worker)
            .AsyncVia(invoker)
            .Run();

        futures.push_back(result);
    }

    WaitFor(Combine(futures))
        .ThrowOnError();
}

INSTANTIATE_TEST_SUITE_P(
    Test,
    TFairShareSchedulerTest,
    ::testing::Values(
        std::make_tuple(1, 5, 1, FSWorkTime),
        std::make_tuple(1, 7, 3, FSWorkTime),
        std::make_tuple(5, 7, 1, FSWorkTime),
        std::make_tuple(5, 7, 3, FSWorkTime)
        ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

