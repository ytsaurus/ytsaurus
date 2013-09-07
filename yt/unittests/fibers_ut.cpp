#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/fibers/fiber.h>

#include <ytlib/concurrency/action_queue.h>
#include <ytlib/actions/cancelable_context.h>
#include <ytlib/concurrency/parallel_awaiter.h>

#include <contrib/testing/framework.h>

#include <exception>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Fiber1(TFiber* main, TFiber* self, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    EXPECT_EQ(EFiberState::Running, main->GetState());

    Yield();

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    EXPECT_EQ(EFiberState::Running, main->GetState());
}

TEST(TFiberTest, Simple)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(&Fiber1, main, Unretained(self.Get()), &v));
    EXPECT_NE(main, self);

    EXPECT_EQ(0, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, self->GetState());

    self->Run();
    ++v;

    EXPECT_EQ(2, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(self->GetState(), EFiberState::Suspended);

    self->Run();
    ++v;

    EXPECT_EQ(4, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, self->GetState());
}

void Fiber2A(TFiber* main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    fibB->Run();

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());
}

void Fiber2B(TFiber* main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(2, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_NE(fibA, TFiber::GetCurrent());
    EXPECT_EQ(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Running, fibB->GetState());
}

TEST(TFiberTest, Nested)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto fibA = New<TFiber>(TClosure());
    auto fibB = New<TFiber>(TClosure());

    fibA->Reset(BIND(
        &Fiber2A,
        main,
        Unretained(fibA.Get()),
        Unretained(fibB.Get()),
        &v));
    fibB->Reset(BIND(
        &Fiber2B,
        main,
        Unretained(fibA.Get()),
        Unretained(fibB.Get()),
        &v));
    EXPECT_NE(main, fibA);
    EXPECT_NE(main, fibB);

    EXPECT_EQ(0, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    fibA->Run();
    ++v;

    EXPECT_EQ(4, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());
}

void Fiber3(TFiber* main, TFiber* self, int* p, bool throwException)
{
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    (void)(*p)++;
    if (throwException) {
        throw std::runtime_error("Hooray!");
    }
}

TEST(TFiberTest, Reset)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(
        &Fiber3,
        main,
        Unretained(self.Get()),
        &v,
        false));
    EXPECT_NE(main, self);

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(3 * i + 0, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Initialized, self->GetState());

        self->Run();
        ++v;

        EXPECT_EQ(3 * i + 2, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Terminated, self->GetState());

        self->Reset();
        ++v;
    }

    self->Reset(BIND(
        &Fiber3,
        main,
        Unretained(self.Get()),
        &v,
        true));

    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(12 + 2 * i + 0, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Initialized, self->GetState());

        EXPECT_THROW({ self->Run(); }, std::runtime_error);

        EXPECT_EQ(12 + 2 * i + 1, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Exception, self->GetState());

        self->Reset();
        ++v;
    }
}

TEST(TFiberTest, ThrowIsPropagated)
{
    auto fiber1 = New<TFiber>(BIND([] () { throw std::bad_alloc(); }));
    auto fiber2 = New<TFiber>(BIND([] () { throw std::bad_exception(); }));

    EXPECT_EQ(EFiberState::Initialized, fiber1->GetState());
    EXPECT_EQ(EFiberState::Initialized, fiber2->GetState());

    EXPECT_THROW({ fiber1->Run(); }, std::bad_alloc);
    EXPECT_THROW({ fiber2->Run(); }, std::bad_exception);

    EXPECT_EQ(EFiberState::Exception, fiber1->GetState());
    EXPECT_EQ(EFiberState::Exception, fiber2->GetState());

    auto fiber3 = New<TFiber>(BIND([] () {
        throw std::runtime_error("Hooray!");
    }));

    try {
        fiber3->Run();
        EXPECT_TRUE(false) << "Stack should be unwinding here.";
    } catch (const std::runtime_error& ex) {
        EXPECT_STREQ("Hooray!", ex.what());
    } catch (...) {
        EXPECT_TRUE(false) << "Unexpected exception was thrown.";
    }
}

class TMyException
    : public std::exception
{
public:
    // Assume that |what| is a static string.
    TMyException(const char* what)
        : What(what)
    { }

    virtual const char* what() const throw() override
    {
        return What;
    }

private:
    const char* What;
};

std::exception_ptr GetMyException(const char* what)
{
    std::exception_ptr exception;
    try {
        throw TMyException(what);
    } catch (...) {
        exception = std::current_exception();
    }
    return exception;
}

TEST(TFiberTest, InjectIntoInitializedFiber)
{
    int v = 0;

    auto ex = GetMyException("InjectIntoInitializedFiber");
    auto fiber = New<TFiber>(BIND([&v] () {
        v += 1;
        throw std::runtime_error("Bad");
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Inject(std::move(ex)); });
    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_THROW({ fiber->Run(); }, TMyException);
    EXPECT_EQ(EFiberState::Exception, fiber->GetState());

    EXPECT_EQ(0, v);
}

TEST(TFiberTest, InjectIntoSuspendedFiber)
{
    int v = 0;

    auto ex = GetMyException("InjectIntoSuspendedFiber");
    auto fiber = New<TFiber>(BIND([&v] () {
        v += 1;
        Yield();
        v += 2;
        throw std::runtime_error("Bad");
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Inject(std::move(ex)); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_THROW({ fiber->Run(); }, TMyException);
    EXPECT_EQ(EFiberState::Exception, fiber->GetState());

    EXPECT_EQ(1, v);
}

TEST(TFiberTest, InjectAndCatch)
{
    int v = 0;

    auto ex = GetMyException("InjectAndCatch");
    auto fiber = New<TFiber>(BIND([&v] () {
        try {
            v += 1;
            Yield();
            v += 2;
            YUNREACHABLE();
        } catch (const TMyException&) {
            v += 4;
        } catch (...) {
            YUNREACHABLE();
        }
        v += 8;
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Inject(std::move(ex)); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Terminated, fiber->GetState());

    EXPECT_EQ(1 + 4 + 8, v);
}

TEST(TFiberTest, InjectAndCatchAndThrow)
{
    int v = 0;

    auto ex = GetMyException("InjectAndCatchAndThrow");
    auto fiber = New<TFiber>(BIND([&v] () {
        try {
            v += 1;
            Yield();
            v += 2;
            YUNREACHABLE();
        } catch (const TMyException&) {
            v += 4;
            throw;
            YUNREACHABLE();
        } catch (...) {
            YUNREACHABLE();
        }
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Inject(std::move(ex)); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_THROW({ fiber->Run(); }, TMyException);
    EXPECT_EQ(EFiberState::Exception, fiber->GetState());

    EXPECT_EQ(1 + 4, v);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> DelayedIdentity(T x)
{
    return
        MakeDelayed(TDuration::MilliSeconds(100)).Apply(
        BIND([=] () { return x; }));
}

NThread::TThreadId GetInvokerThreadId(IInvokerPtr invoker)
{
    return BIND([] () {
        return NThread::GetCurrentThreadId();
    }).AsyncVia(invoker).Run().Get();
}

////////////////////////////////////////////////////////////////////////////////

#define WRAPPED_FIBER_TEST(testCaseName, testName) \
    void FiberTest_##testCaseName##_##testName(); \
    \
    TEST(testCaseName, testName) \
    { \
        TWeakPtr<TFiber> weakFiber; \
        { \
            auto fiber = New<TFiber>(BIND(&FiberTest_##testCaseName##_##testName)); \
            weakFiber = fiber; \
            fiber->Run(); \
        } \
        while (weakFiber.Lock()) { \
            Sleep(TDuration::MilliSeconds(1)); \
        } \
    } \
    \
    void FiberTest_##testCaseName##_##testName()

static TLazyIntrusivePtr<TActionQueue> Queue1(TActionQueue::CreateFactory("Queue1"));
static TLazyIntrusivePtr<TActionQueue> Queue2(TActionQueue::CreateFactory("Queue2"));

WRAPPED_FIBER_TEST(TFiberTest, SimpleAsync)
{
    auto asyncA = DelayedIdentity(3);
    int a = WaitFor(asyncA);

    auto asyncB = DelayedIdentity(4);
    int b = WaitFor(asyncB);

    EXPECT_EQ(a + b, 7);
}

WRAPPED_FIBER_TEST(TFiberTest, SwitchToInvoker1)
{
    auto invoker = Queue1->GetInvoker();

    auto id0 = NThread::GetCurrentThreadId();
    auto id1 = GetInvokerThreadId(invoker);
    EXPECT_NE(id0, id1);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker);
        EXPECT_EQ(NThread::GetCurrentThreadId(), id1);
    }
}

WRAPPED_FIBER_TEST(TFiberTest, SwitchToInvoker2)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto id0 = NThread::GetCurrentThreadId();
    auto id1 = GetInvokerThreadId(invoker1);
    auto id2 = GetInvokerThreadId(invoker2);
    EXPECT_NE(id0, id1);
    EXPECT_NE(id0, id2);
    EXPECT_NE(id1, id2);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker1);
        EXPECT_EQ(NThread::GetCurrentThreadId(), id1);

        SwitchTo(invoker2);
        EXPECT_EQ(NThread::GetCurrentThreadId(), id2);
    }
}

WRAPPED_FIBER_TEST(TFiberTest, TerminatedCaught)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel();

    EXPECT_THROW({ SwitchTo(invoker1); }, TFiberTerminatedException);
}

WRAPPED_FIBER_TEST(TFiberTest, TerminatedPropagated)
{
    auto context = New<TCancelableContext>();
    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel();

    SwitchTo(invoker1);

    EXPECT_EQ(2 * 2, 5);
}

WRAPPED_FIBER_TEST(TFiberTest, CurrentInvokerAfterSwitch1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    EXPECT_EQ(GetCurrentInvoker(), invoker);
}

WRAPPED_FIBER_TEST(TFiberTest, CurrentInvokerAfterSwitch2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    EXPECT_EQ(GetCurrentInvoker(), invoker);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFiberTest, ActionQueue1)
{
    auto actionQueue = New<TActionQueue>();
    auto invoker = actionQueue->GetInvoker();

    TFiber* fiber1;
    NThread::TThreadId thread1;

    TFiber* fiber2;
    NThread::TThreadId thread2;

    BIND([&] () {
        fiber1 = TFiber::GetCurrent();
        thread1 = NThread::GetCurrentThreadId();

        WaitFor(MakeDelayed(TDuration::MilliSeconds(100)));

        fiber2 = TFiber::GetCurrent();
        thread2 = NThread::GetCurrentThreadId();
    }).AsyncVia(invoker).Run().Get();

    EXPECT_EQ(fiber1, fiber2);
    EXPECT_EQ(thread1, thread2);

    TFiber* fiber3;
    NThread::TThreadId thread3;

    TFiber* fiber4;
    NThread::TThreadId thread4;

    BIND([&] () {
        fiber3 = TFiber::GetCurrent();
        thread3 = NThread::GetCurrentThreadId();

        WaitFor(MakeDelayed(TDuration::MilliSeconds(100)));

        fiber4 = TFiber::GetCurrent();
        thread4 = NThread::GetCurrentThreadId();
    }).AsyncVia(invoker).Run().Get();

    EXPECT_NE(fiber2, fiber3);
    EXPECT_EQ(fiber3, fiber4);
    EXPECT_EQ(thread3, thread4);

    actionQueue->Shutdown();
}

TEST(TFiberTest, ActionQueue2)
{
    auto actionQueue = New<TActionQueue>();
    auto invoker = actionQueue->GetInvoker();
    auto awaiter = New<TParallelAwaiter>(invoker);

    int sum = 0;

    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([&sum, i] () {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(100)));
            sum += i;
        });

        awaiter->Await(callback.AsyncVia(invoker).Run());
    }

    awaiter->Complete().Get();

    EXPECT_EQ(sum, 45);

    actionQueue->Shutdown();
}

TEST(TFiberTest, ActionQueue3)
{
    auto actionQueue = New<TActionQueue>();
    auto invoker = actionQueue->GetInvoker();
    auto awaiter = New<TParallelAwaiter>(invoker);

    int sum = 0;

    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([&sum, i, invoker] () {
            auto thread1 = NThread::GetCurrentThreadId();
            
            WaitFor(MakeDelayed(TDuration::MilliSeconds(100)));
            
            auto thread2 = NThread::GetCurrentThreadId();
            EXPECT_EQ(thread1, thread2);

            sum += i;
        });

        awaiter->Await(callback.AsyncVia(invoker).Run());
    }

    awaiter->Complete().Get();

    EXPECT_EQ(sum, 45);

    actionQueue->Shutdown();
}

TEST(TFiberTest, CurrentInvokerSync)
{
    EXPECT_EQ(GetCurrentInvoker(), GetSyncInvoker())
        << "Current invoker: " << typeid(*GetCurrentInvoker()).name();
}

TEST(TFiberTest, CurrentInvokerInActionQueue)
{
    auto invoker = Queue1->GetInvoker();
    BIND([=] () {
        EXPECT_EQ(GetCurrentInvoker(), invoker);
    })
    .AsyncVia(invoker)
    .Run()
    .Get();
}

TEST(TFiberTest, CurrentInvokerConcurrent)
{
    auto invoker1 = Queue1->GetInvoker();
    auto invoker2 = Queue2->GetInvoker();

    auto result1 = BIND([=] () {
        EXPECT_EQ(GetCurrentInvoker(), invoker1);
    }).AsyncVia(invoker1).Run();

    auto result2 = BIND([=] () {
        EXPECT_EQ(GetCurrentInvoker(), invoker2);
    }).AsyncVia(invoker2).Run();

    result1.Get();
    result2.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

