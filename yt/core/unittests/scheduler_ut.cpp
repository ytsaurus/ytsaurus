#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fiber.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/invoker_util.h>

#include <yt/core/misc/lazy_ptr.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

#include <exception>

namespace NYT::NConcurrency {

using ::testing::ContainsRegex;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(100);

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

    if (!GetCurrentScheduler()->GetCurrentFiber()->CheckFreeStackSpace(40 * 1024)) {
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
        [&] { WaitFor(asyncResult2).ThrowOnError(); },
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

    context->Cancel();

    EXPECT_THROW({ SwitchTo(invoker); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, SwitchToCancelableInvoker2)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = context->CreateInvoker(Queue2->GetInvoker());

    EXPECT_NO_THROW({ SwitchTo(invoker1); });

    context->Cancel();

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

    context->Cancel();

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
            context->Cancel();
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
            context->Cancel();
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

    context->Cancel();

    EXPECT_THROW({ SwitchTo(invoker1); }, TFiberCanceledException);
}

TEST_W(TSchedulerTest, TerminatedPropagated)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel();

    SwitchTo(invoker1);

    Y_UNREACHABLE();
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
        WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
            .ThrowOnError();
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
            WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                .ThrowOnError();
            {
                TContextSwitchGuard guard2(
                    [&] { ++counter3; },
                    [&] { ++counter4; });
                WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                    .ThrowOnError();
            }
            WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                .ThrowOnError();
        }
        WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
            .ThrowOnError();
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
            WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                .ThrowOnError();
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
        WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                .ThrowOnError();
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
            WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
                .ThrowOnError();
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
        WaitFor(f1);
    });

    auto f2 = a.AsyncVia(Queue1->GetInvoker()).Run();

    Sleep(SleepQuantum);

    f2.Cancel();

    Sleep(SleepQuantum);

    EXPECT_TRUE(p1.IsCanceled());
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
    asyncResult2.Cancel();
    EXPECT_TRUE(asyncResult1.Get().IsOK());
    Sleep(SleepQuantum);
    EXPECT_TRUE(asyncResult2.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, asyncResult2.Get().GetCode());
}

TEST_F(TSchedulerTest, CancelCurrentFiber)
{
    auto invoker = Queue1->GetInvoker();
    auto asyncResult = BIND([=] () {
        NYT::NConcurrency::GetCurrentFiberCanceler().Run();
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
            NYT::NConcurrency::GetCurrentFiberCanceler().Run();
        }).AsyncVia(invoker2).Run().Get();
        WaitFor(promise.ToFuture(), invoker2);
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
        NYT::NConcurrency::GetCurrentFiberCanceler().Run();
    }).AsyncVia(invoker).Run().Get();
    auto asyncResult2 = BIND([=] () {
        Yield();
    }).AsyncVia(invoker).Run().Get();
    EXPECT_TRUE(asyncResult1.IsOK());
    EXPECT_TRUE(asyncResult2.IsOK());
}

TEST_F(TSchedulerTest, CancelInAdjacentThread)
{
    auto closure = TClosure();
    auto invoker = Queue1->GetInvoker();
    auto asyncResult1 = BIND([=, &closure] () {
        closure = NYT::NConcurrency::GetCurrentFiberCanceler();
    }).AsyncVia(invoker).Run().Get();
    closure.Run(); // *evil laugh*
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
        WaitFor(VoidFuture);
        WaitFor(VoidFuture);
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
        WaitFor(VoidFuture);
        Sleep(SleepQuantum);
        WaitFor(VoidFuture);
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
        WaitFor(future);
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
            YCHECK(error.GetCode() == EErrorCode::Canceled);
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
    , public ::testing::WithParamInterface<std::tuple<int, int, TDuration>>
{ };

TEST_P(TFairShareSchedulerTest, Test)
{
    size_t numThreads = std::get<0>(GetParam());
    size_t numWorkers = std::get<1>(GetParam());
    auto work = std::get<2>(GetParam());

    YCHECK(numWorkers > 0);
    YCHECK(numThreads > 0);
    YCHECK(numThreads <= numWorkers);

    auto threadPool = CreateFairShareThreadPool(numThreads, "MyFairSharePool");
    std::vector<TDuration> progresses(numWorkers);
    std::vector<TFuture<void>> futures;

    for (size_t id = 0; id < numWorkers; ++id) {
        auto invoker = threadPool->GetInvoker(Format("worker%v", id));
        auto worker = [&, id] () {
            double factor = (id + 1);

            while (progresses[id] < work) {
                if (numThreads == 1) {
                    auto min = *std::min_element(progresses.begin(), progresses.end());
                    EXPECT_EQ(progresses[id].MilliSeconds(), min.MilliSeconds());
                }

                progresses[id] += SleepQuantum * factor;
                Sleep(SleepQuantum * factor);
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

INSTANTIATE_TEST_CASE_P(
    Test,
    TFairShareSchedulerTest,
    ::testing::Values(
        std::make_tuple(1, 3, TDuration::Seconds(3)),
        std::make_tuple(5, 7, TDuration::Seconds(3))));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

