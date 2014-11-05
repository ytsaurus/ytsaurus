#include "stdafx.h"
#include "framework.h"
#include "probe.h"

#include <core/misc/common.h>
#include <core/misc/lazy_ptr.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/actions/cancelable_context.h>
#include <core/actions/invoker_util.h>

#include <exception>

namespace NYT {
namespace NConcurrency {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> DelayedIdentity(T x)
{
    return
        MakeDelayed(TDuration::MilliSeconds(10))
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

TEST_W(TSchedulerTest, SimpleAsync)
{
    auto asyncA = DelayedIdentity(3);
    int a = WaitFor(asyncA);

    auto asyncB = DelayedIdentity(4);
    int b = WaitFor(asyncB);

    EXPECT_EQ(7, a + b);
}

TEST_W(TSchedulerTest, SwitchToInvoker1)
{
    auto invoker = Queue1->GetInvoker();

    auto id0 = GetCurrentThreadId();
    auto id1 = invoker->GetThreadId();

    EXPECT_NE(id0, id1);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker);
        EXPECT_EQ(GetCurrentThreadId(), id1);
    }
}

TEST_W(TSchedulerTest, SwitchToInvoker2)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto id0 = GetCurrentThreadId();
    auto id1 = invoker1->GetThreadId();
    auto id2 = invoker2->GetThreadId();

    EXPECT_NE(id0, id1);
    EXPECT_NE(id0, id2);
    EXPECT_NE(id1, id2);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker1);
        EXPECT_EQ(GetCurrentThreadId(), id1);

        SwitchTo(invoker2);
        EXPECT_EQ(GetCurrentThreadId(), id2);
    }
}

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

    YUNREACHABLE();
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

    auto x = BIND([&] () {
    }).AsyncVia(invoker).Run();

    WaitFor(x);
}

// Various invokers.

TEST_F(TSchedulerTest, WaitForInSerializedInvoker)
{
    auto invoker = CreateSerializedInvoker(Queue1->GetInvoker());
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(10)));
        }
    }).AsyncVia(invoker).Run().Get();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker1)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);
    BIND([&] () {
        for (int i = 0; i < 10; ++i) {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(10)));
        }
    }).AsyncVia(invoker).Run().Get();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker2)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 2);

    auto promise = NewPromise<void>();

    auto a1 = BIND([&] () {
        promise.Set();
    });

    auto a2 = BIND([&] () {
        invoker->Invoke(a1);
        WaitFor(promise);
    });

    a2.AsyncVia(invoker).Run().Get();
}

TEST_F(TSchedulerTest, WaitForInBoundedConcurrencyInvoker3)
{
    auto invoker = CreateBoundedConcurrencyInvoker(Queue1->GetInvoker(), 1);

    auto promise = NewPromise<void>();

    bool a1called = false;
    bool a1finished = false;
    auto a1 = BIND([&] () {
        a1called = true;
        WaitFor(promise);
        a1finished = true;
    });

    bool a2called = false;
    auto a2 = BIND([&] () {
        a2called = true;
    });

    invoker->Invoke(a1);
    invoker->Invoke(a2);

    Sleep(TDuration::MilliSeconds(10));
    EXPECT_TRUE(a1called);
    EXPECT_FALSE(a1finished);
    EXPECT_FALSE(a2called);

    promise.Set();

    Sleep(TDuration::MilliSeconds(10));
    EXPECT_TRUE(a1called);
    EXPECT_TRUE(a1finished);
    EXPECT_TRUE(a2called);
}

TEST_F(TSchedulerTest, ShouldUnwindFiberOnFutureCancellation)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    TProbeState state;

    auto a1 = BIND([=, &state] () {
        TProbe probe(&state);
        WaitFor(future);
        probe.Tackle(); // Should not be called.
    });

    Queue1->GetInvoker()->Invoke(std::move(a1));

    Sleep(TDuration::MilliSeconds(10));

    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(0, state.Destructors);
    EXPECT_EQ(0, state.Tackles);

    future.Cancel();

    Sleep(TDuration::MilliSeconds(10));

    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
    EXPECT_EQ(0, state.Tackles);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT

