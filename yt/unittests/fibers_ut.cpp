#include "stdafx.h"

#include <core/misc/common.h>
#include <core/misc/lazy_ptr.h>

#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/actions/cancelable_context.h>

#include <exception>

#include <contrib/testing/framework.h>
#include "probe.h"

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TFiberTest
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

void Fiber1(TFiber* main, TFiber* self, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());

    Yield();

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
}

TEST_F(TFiberTest, Simple)
{
    int v = -1;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(&Fiber1, main, Unretained(self.Get()), &v));
    EXPECT_NE(main, self);

    ++v;
    EXPECT_EQ(0, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, self->GetState());

    {
        auto rv = self->Run();
        EXPECT_EQ(EFiberState::Suspended, rv);
    }

    ++v;
    EXPECT_EQ(2, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Suspended, self->GetState());

    {
        auto rv = self->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

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
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    {
        auto rv = fibB->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
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
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Blocked, fibA->GetState());
    EXPECT_EQ(EFiberState::Running, fibB->GetState());
}

TEST_F(TFiberTest, Nested1)
{
    int v = -1;

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

    ++v;
    EXPECT_EQ(0, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    {
        auto rv = fibA->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    ++v;
    EXPECT_EQ(4, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());
}

void Fiber3A(TFiber* main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    {
        auto rv = fibB->Run();
        EXPECT_EQ(EFiberState::Suspended, rv);
    }

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Suspended, fibB->GetState());

    Yield();

    (void)(*p)++;
    EXPECT_EQ(5, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Suspended, fibB->GetState());

    {
        auto rv = fibB->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    (void)(*p)++;
    EXPECT_EQ(7, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(fibA, TFiber::GetCurrent());
    EXPECT_NE(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());
}

void Fiber3B(TFiber* main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(2, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_NE(fibA, TFiber::GetCurrent());
    EXPECT_EQ(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Blocked, fibA->GetState());
    EXPECT_EQ(EFiberState::Running, fibB->GetState());

    Yield();

    (void)(*p)++;
    EXPECT_EQ(6, *p);
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_NE(fibA, TFiber::GetCurrent());
    EXPECT_EQ(fibB, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Blocked, fibA->GetState());
    EXPECT_EQ(EFiberState::Running, fibB->GetState());
}

TEST_F(TFiberTest, Nested2)
{
    int v = -1;

    auto main = TFiber::GetCurrent();
    auto fibA = New<TFiber>(TClosure());
    auto fibB = New<TFiber>(TClosure());

    fibA->Reset(BIND(
        &Fiber3A,
        main,
        Unretained(fibA.Get()),
        Unretained(fibB.Get()),
        &v));
    fibB->Reset(BIND(
        &Fiber3B,
        main,
        Unretained(fibA.Get()),
        Unretained(fibB.Get()),
        &v));
    EXPECT_NE(main, fibA);
    EXPECT_NE(main, fibB);

    ++v;
    EXPECT_EQ(0, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibA->GetState());
    EXPECT_EQ(EFiberState::Initialized, fibB->GetState());

    {
        auto rv = fibA->Run();
        EXPECT_EQ(EFiberState::Suspended, rv);
    }

    ++v;
    EXPECT_EQ(4, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Suspended, fibA->GetState());
    EXPECT_EQ(EFiberState::Suspended, fibB->GetState());

    {
        auto rv = fibA->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    ++v;
    EXPECT_EQ(8, v);
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());
}

void Fiber4(TFiber* main, TFiber* self, int* p, bool doYield, bool doThrow)
{
    EXPECT_NE(main, TFiber::GetCurrent());
    EXPECT_EQ(self, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Blocked, main->GetState());
    EXPECT_EQ(EFiberState::Running, self->GetState());
    (void)(*p)++;
    if (doYield) {
        Yield();
    }
    if (doThrow) {
        throw std::runtime_error("Hooray!");
    }
}

TEST_F(TFiberTest, Reset)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(
        &Fiber4,
        main,
        Unretained(self.Get()),
        &v,
        false,
        false));
    EXPECT_NE(main, self);

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(i, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Initialized, self->GetState());

        self->Run();

        EXPECT_EQ(i + 1, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Terminated, self->GetState());

        self->Reset();
    }
}

TEST_F(TFiberTest, ResetAfterThrow)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(
        &Fiber4,
        main,
        Unretained(self.Get()),
        &v,
        false,
        true));

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(i, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Initialized, self->GetState());

        EXPECT_THROW({ self->Run(); }, std::runtime_error);

        EXPECT_EQ(i + 1, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Exception, self->GetState());

        self->Reset();
    }
}

TEST_F(TFiberTest, ResetAfterCancel)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(
        &Fiber4,
        main,
        Unretained(self.Get()),
        &v,
        true,
        false));

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(i, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Initialized, self->GetState());

        self->Run();

        EXPECT_EQ(i + 1, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Suspended, self->GetState());

        self->Cancel();

        EXPECT_EQ(i + 1, v) << "Iteration #" << i;
        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Canceled, self->GetState());

        self->Reset();
    }
}

TEST_F(TFiberTest, ThrowIsPropagated)
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
        FAIL() << "Stack should be unwinding here.";
    } catch (const std::runtime_error& ex) {
        EXPECT_STREQ("Hooray!", ex.what());
    } catch (...) {
        FAIL() << "Unexpected exception was thrown.";
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

TEST_F(TFiberTest, InjectIntoInitializedFiber)
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

TEST_F(TFiberTest, InjectIntoSuspendedFiber)
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

TEST_F(TFiberTest, InjectAndCatch)
{
    int v = 0;

    auto ex = GetMyException("InjectAndCatch");
    auto fiber = New<TFiber>(BIND([&v] () {
        try {
            v += 1;
            Yield();
            YUNREACHABLE();
        } catch (const TMyException&) {
            v += 2;
        } catch (...) {
            YUNREACHABLE();
        }
        v += 4;
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Inject(std::move(ex)); });
    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_NO_THROW({ fiber->Run(); });
    EXPECT_EQ(EFiberState::Terminated, fiber->GetState());

    EXPECT_EQ(1 + 2 + 4, v);
}

TEST_F(TFiberTest, InjectAndCatchAndThrow)
{
    int v = 0;

    auto ex = GetMyException("InjectAndCatchAndThrow");
    auto fiber = New<TFiber>(BIND([&v] () {
        try {
            v += 1;
            Yield();
            YUNREACHABLE();
        } catch (const TMyException&) {
            v += 2;
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

    EXPECT_EQ(1 + 2, v);
}

TEST_F(TFiberTest, Cancel)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND([&] () {
        TProbe probe(&state);
        Yield();
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());
    EXPECT_NO_THROW({ fiber->Run(); });

    EXPECT_EQ(EFiberState::Suspended, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());
    EXPECT_NO_THROW({ fiber->Cancel(); });

    EXPECT_EQ(EFiberState::Canceled, fiber->GetState());
    EXPECT_TRUE(fiber->IsCanceled());

    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
}

TEST_F(TFiberTest, CancelInitializedFiber)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND([&] () {
        TProbe probe(&state);
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());

    EXPECT_NO_THROW({ fiber->Cancel(); });

    EXPECT_EQ(EFiberState::Initialized, fiber->GetState());
    EXPECT_TRUE(fiber->IsCanceled());

    EXPECT_NO_THROW({ fiber->Run(); });

    EXPECT_EQ(EFiberState::Canceled, fiber->GetState());
    EXPECT_TRUE(fiber->IsCanceled());

    EXPECT_EQ(0, state.Constructors);
}

TEST_F(TFiberTest, CancelTerminatedFiber)
{
    auto fiber = New<TFiber>(BIND([] () {}));
    EXPECT_NO_THROW({ fiber->Run(); });

    EXPECT_EQ(EFiberState::Terminated, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());

    EXPECT_NO_THROW({ fiber->Cancel(); });

    EXPECT_EQ(EFiberState::Terminated, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());
}

TEST_F(TFiberTest, CancelExceptionFiber)
{
    auto fiber = New<TFiber>(BIND([] () { throw std::runtime_error(":)"); }));
    EXPECT_THROW({ fiber->Run(); }, std::runtime_error);

    EXPECT_EQ(EFiberState::Exception, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());

    EXPECT_NO_THROW({ fiber->Cancel(); });

    EXPECT_EQ(EFiberState::Exception, fiber->GetState());
    EXPECT_FALSE(fiber->IsCanceled());
}

TEST_F(TFiberTest, YieldTo1)
{
    TProbeState state;
    Stroka flow;

    auto main = TFiber::GetCurrent();
    TFiberPtr inner;
    TFiberPtr outer;

    inner = New<TFiber>(BIND([&] () {
        TProbe probe(&state);

        flow.append('B');

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_NE(outer, TFiber::GetCurrent());
        EXPECT_EQ(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Blocked, outer->GetState());
        EXPECT_EQ(EFiberState::Running, inner->GetState());

        Yield(main);

        flow.append('C');

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_NE(outer, TFiber::GetCurrent());
        EXPECT_EQ(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Blocked, outer->GetState());
        EXPECT_EQ(EFiberState::Running, inner->GetState());
    }));

    outer = New<TFiber>(BIND([&] () {
        TProbe probe(&state);

        flow.append('A');

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_EQ(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Running, outer->GetState());
        EXPECT_EQ(EFiberState::Initialized, inner->GetState());

        {
            auto rv = inner->Run();
            EXPECT_EQ(EFiberState::Terminated, rv);
        }

        flow.append('D');

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_EQ(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Running, outer->GetState());
        EXPECT_EQ(EFiberState::Terminated, inner->GetState());
    }));

    EXPECT_NE(main, inner.Get());
    EXPECT_NE(main, outer.Get());

    EXPECT_TRUE(flow.empty());
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_NE(outer, TFiber::GetCurrent());
    EXPECT_NE(inner, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, outer->GetState());
    EXPECT_EQ(EFiberState::Initialized, inner->GetState());

    {
        auto rv = outer->Run();
        EXPECT_EQ(EFiberState::Suspended, rv);
    }

    EXPECT_STREQ("AB", flow.c_str());
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_NE(outer, TFiber::GetCurrent());
    EXPECT_NE(inner, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Suspended, outer->GetState());
    EXPECT_EQ(EFiberState::Suspended, inner->GetState());

    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    {
        auto rv = outer->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    EXPECT_STREQ("ABCD", flow.c_str());
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_NE(outer, TFiber::GetCurrent());
    EXPECT_NE(inner, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, outer->GetState());
    EXPECT_EQ(EFiberState::Terminated, inner->GetState());

    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(2, state.Destructors);

    EXPECT_EQ(1, inner->GetRefCount());
    EXPECT_EQ(1, outer->GetRefCount());
}

TEST_F(TFiberTest, YieldTo2)
{
    TProbeState state;
    Stroka flow;

    auto main = TFiber::GetCurrent();
    TFiberPtr inner;
    TFiberPtr outer;

    inner = New<TFiber>(BIND([&] () {
        flow.append('(');
        TProbe probe(&state);

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_NE(outer, TFiber::GetCurrent());
        EXPECT_EQ(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Blocked, outer->GetState());
        EXPECT_EQ(EFiberState::Running, inner->GetState());

        Yield();

        for (int i = 0; i < 10; ++i) {
            SCOPED_TRACE(Sprintf("Inner Iteration #%d", i));

            flow.append('C');

            EXPECT_NE(main, TFiber::GetCurrent());
            EXPECT_NE(outer, TFiber::GetCurrent());
            EXPECT_EQ(inner, TFiber::GetCurrent());
            EXPECT_EQ(EFiberState::Blocked, main->GetState());
            EXPECT_EQ(EFiberState::Blocked, outer->GetState());
            EXPECT_EQ(EFiberState::Running, inner->GetState());

            Yield(main);

            flow.append('E');

            EXPECT_NE(main, TFiber::GetCurrent());
            EXPECT_NE(outer, TFiber::GetCurrent());
            EXPECT_EQ(inner, TFiber::GetCurrent());
            EXPECT_EQ(EFiberState::Blocked, main->GetState());
            EXPECT_EQ(EFiberState::Blocked, outer->GetState());
            EXPECT_EQ(EFiberState::Running, inner->GetState());

            Yield();
        }

        flow.append(')');
    }));

    outer = New<TFiber>(BIND([&] () {
        flow.append('[');
        TProbe probe(&state);

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_EQ(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Running, outer->GetState());
        EXPECT_EQ(EFiberState::Initialized, inner->GetState());

        inner->Run();
        Yield();

        for (int i = 0; i < 10; ++i) {
            SCOPED_TRACE(Sprintf("Outer Iteration #%d", i));

            flow.append('B');

            EXPECT_NE(main, TFiber::GetCurrent());
            EXPECT_EQ(outer, TFiber::GetCurrent());
            EXPECT_NE(inner, TFiber::GetCurrent());
            EXPECT_EQ(EFiberState::Blocked, main->GetState());
            EXPECT_EQ(EFiberState::Running, outer->GetState());
            EXPECT_EQ(EFiberState::Suspended, inner->GetState());

            {
                auto rv = inner->Run();
                EXPECT_EQ(EFiberState::Suspended, rv);
            }

            flow.append('F');

            EXPECT_NE(main, TFiber::GetCurrent());
            EXPECT_EQ(outer, TFiber::GetCurrent());
            EXPECT_NE(inner, TFiber::GetCurrent());
            EXPECT_EQ(EFiberState::Blocked, main->GetState());
            EXPECT_EQ(EFiberState::Running, outer->GetState());
            EXPECT_EQ(EFiberState::Suspended, inner->GetState());

            Yield();
        }

        inner->Run();

        EXPECT_NE(main, TFiber::GetCurrent());
        EXPECT_EQ(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Blocked, main->GetState());
        EXPECT_EQ(EFiberState::Running, outer->GetState());
        EXPECT_EQ(EFiberState::Terminated, inner->GetState());

        flow.append(']');
    }));

    EXPECT_NE(main, inner.Get());
    EXPECT_NE(main, outer.Get());

    EXPECT_TRUE(flow.empty());
    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_NE(outer, TFiber::GetCurrent());
    EXPECT_NE(inner, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Initialized, outer->GetState());
    EXPECT_EQ(EFiberState::Initialized, inner->GetState());

    {
        auto rv = outer->Run();
        EXPECT_EQ(EFiberState::Suspended, rv);
    }

    EXPECT_STREQ("[(", flow.c_str());

    for (int i = 0; i < 10; ++i) {
        SCOPED_TRACE(Sprintf("Main Iteration #%d", i));

        flow.append('A');

        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_NE(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Suspended, outer->GetState());
        EXPECT_EQ(EFiberState::Suspended, inner->GetState());

        {
            auto rv = outer->Run();
            EXPECT_EQ(EFiberState::Suspended, rv);
        }

        flow.append('D');

        EXPECT_EQ(main, TFiber::GetCurrent());
        EXPECT_NE(outer, TFiber::GetCurrent());
        EXPECT_NE(inner, TFiber::GetCurrent());
        EXPECT_EQ(EFiberState::Running, main->GetState());
        EXPECT_EQ(EFiberState::Suspended, outer->GetState());
        EXPECT_EQ(EFiberState::Suspended, inner->GetState());

        {
            auto rv = outer->Run();
            EXPECT_EQ(EFiberState::Suspended, rv);
        }
    }

    {
        auto rv = outer->Run();
        EXPECT_EQ(EFiberState::Terminated, rv);
    }

    EXPECT_STREQ(
        ("[(" + Stroka("ABCDEF") * 10 + ")]").c_str(),
        flow.c_str());

    EXPECT_EQ(main, TFiber::GetCurrent());
    EXPECT_NE(outer, TFiber::GetCurrent());
    EXPECT_NE(inner, TFiber::GetCurrent());
    EXPECT_EQ(EFiberState::Running, main->GetState());
    EXPECT_EQ(EFiberState::Terminated, outer->GetState());
    EXPECT_EQ(EFiberState::Terminated, inner->GetState());

    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(2, state.Destructors);

    EXPECT_EQ(1, inner->GetRefCount());
    EXPECT_EQ(1, outer->GetRefCount());
}

TEST_F(TFiberTest, AbandonYieldedFiber1)
{
    TProbeState state;

    auto fiber =
    New<TFiber>(BIND([&] () {
        TProbe probe(&state);
        Yield();
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    fiber.Reset();

    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
}

TEST_F(TFiberTest, AbandonYieldedFiber2)
{
    TProbeState state;

    auto main = TFiber::GetCurrent();
    auto fiber =
    New<TFiber>(BIND([&] () {
        TProbe probe(&state);
        New<TFiber>(BIND([&] () {
            TProbe probe(&state);
            Yield(main);
            YUNREACHABLE();
        }))->Run();
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    fiber.Reset();

    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(2, state.Destructors);
}

TEST_F(TFiberTest, AbandonYieldedFiber3)
{
    TProbeState state;

    auto main = TFiber::GetCurrent();
    auto fiber =
    New<TFiber>(BIND([&] {
        TProbe probe(&state);
        New<TFiber>(BIND([&] {
            TProbe probe(&state);
            New<TFiber>(BIND([&] {
                TProbe probe(&state);
                Yield(main);
                YUNREACHABLE();
            }))->Run();
            YUNREACHABLE();
        }))->Run();
        YUNREACHABLE();
    }));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(3, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    fiber.Reset();

    EXPECT_EQ(3, state.Constructors);
    EXPECT_EQ(3, state.Destructors);
}

////////////////////////////////////////////////////////////////////////////////
// Stress tests.
//

void Fiber5(
    TFiber* main,
    bool useYieldTo,
    int depth,
    TProbeState* state,
    bool shouldTerminate)
{
    // Code is intentionally verbose to keep proper scoping.
    --depth; // Count one.
    TProbe probe(state);
    TClosure deeper(BIND(&Fiber5, main, useYieldTo, depth, state, shouldTerminate));
    if (useYieldTo) {
        if (depth > 0) {
            auto fiber = New<TFiber>(std::move(deeper));
            auto result = fiber->Run();
            EXPECT_EQ(EFiberState::Terminated, result);
            if (!shouldTerminate) {
                YUNREACHABLE();
            }
        } else {
            Yield(main);
            if (!shouldTerminate) {
                YUNREACHABLE();
            }
        }
    } else {
        if (depth > 0) {
            auto fiber = New<TFiber>(deeper);
            auto result = fiber->Run();
            EXPECT_EQ(EFiberState::Suspended, result);
            Yield();
            if (!shouldTerminate) {
                YUNREACHABLE();
            }
        } else {
            Yield();
            if (!shouldTerminate) {
                YUNREACHABLE();
            }
        }
    }
}

class TFiberYieldTo
    : public TFiberTest
    , public ::testing::WithParamInterface<std::tr1::tuple<bool, int>>
{
protected:
    virtual void SetUp()
    {
        bool useYieldTo = std::tr1::get<0>(GetParam());
        int depth = std::tr1::get<1>(GetParam());
        N = depth;

        Main_ = TFiber::GetCurrent();
        Body_ = BIND(&Fiber5, Unretained(Main_), useYieldTo, depth);
    }

    void CheckGlobals()
    {
        EXPECT_EQ(Main_, TFiber::GetCurrent());
        EXPECT_EQ(2, TFiber::GetCurrent()->GetRefCount());
    }

    int N;

    TFiber* Main_;
    TCallback<void(TProbeState*, bool)> Body_;
};

TEST_P(TFiberYieldTo, Yield)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND(Body_, &state, true));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    EXPECT_EQ(EFiberState::Terminated, fiber->Run());
    EXPECT_EQ(EFiberState::Terminated, fiber->GetState());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(N, state.Destructors);

    EXPECT_EQ(1, fiber->GetRefCount());

    CheckGlobals();
}

TEST_P(TFiberYieldTo, Inject)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND(Body_, &state, false));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    EXPECT_NO_THROW({ fiber->Inject(GetMyException("Oh My!")); });
    EXPECT_THROW({ fiber->Run(); }, TMyException);
    EXPECT_EQ(EFiberState::Exception, fiber->GetState());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(N, state.Destructors);

    EXPECT_EQ(1, fiber->GetRefCount());

    CheckGlobals();
}

TEST_P(TFiberYieldTo, Cancel)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND(Body_, &state, false));

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    EXPECT_NO_THROW({ fiber->Cancel(); });
    EXPECT_EQ(EFiberState::Canceled, fiber->GetState());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(N, state.Destructors);

    EXPECT_EQ(1, fiber->GetRefCount());

    CheckGlobals();
}

TEST_P(TFiberYieldTo, Destroy)
{
    TProbeState state;
    auto fiber = New<TFiber>(BIND(Body_, &state, false));
    auto weakFiber = MakeWeak(fiber);

    EXPECT_EQ(EFiberState::Suspended, fiber->Run());

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(0, state.Destructors);

    fiber.Reset();

    EXPECT_EQ(N, state.Constructors);
    EXPECT_EQ(N, state.Destructors);

    EXPECT_TRUE(weakFiber.IsExpired());

    CheckGlobals();
}

INSTANTIATE_TEST_CASE_P(
    Stress,
    TFiberYieldTo,
    ::testing::Combine(
        ::testing::Bool(), 
        ::testing::Values(1, 2, 3, 4, 5, 10, 100, 1000)));

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> DelayedIdentity(T x)
{
    return
        MakeDelayed(TDuration::MilliSeconds(25))
        .Apply(BIND([=] () { return x; }));
}

NThread::TThreadId GetInvokerThreadId(IInvokerPtr invoker)
{
    return BIND(&NThread::GetCurrentThreadId).AsyncVia(invoker).Run().Get();
}

////////////////////////////////////////////////////////////////////////////////
// Async tests.
//

// Wraps tests in an extra fiber and awaits termination. Adapted from `gtest.h`.
#define TEST_W_(test_case_name, test_name, parent_class, parent_id)\
class GTEST_TEST_CLASS_NAME_(test_case_name, test_name) : public parent_class {\
 public:\
  GTEST_TEST_CLASS_NAME_(test_case_name, test_name)() {}\
 private:\
  virtual void TestBody();\
  void TestInnerBody();\
  static ::testing::TestInfo* const test_info_ GTEST_ATTRIBUTE_UNUSED_;\
  GTEST_DISALLOW_COPY_AND_ASSIGN_(\
    GTEST_TEST_CLASS_NAME_(test_case_name, test_name));\
};\
\
::testing::TestInfo* const GTEST_TEST_CLASS_NAME_(test_case_name, test_name)\
  ::test_info_ =\
    ::testing::internal::MakeAndRegisterTestInfo(\
        #test_case_name, #test_name, NULL, NULL, \
        (parent_id), \
        parent_class::SetUpTestCase, \
        parent_class::TearDownTestCase, \
        new ::testing::internal::TestFactoryImpl<\
            GTEST_TEST_CLASS_NAME_(test_case_name, test_name)>);\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestBody() {\
  TWeakPtr<TFiber> weakFiber;\
  {\
    auto strongFiber = New<TFiber>(\
      BIND(&GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody,\
        this));\
    weakFiber = strongFiber;\
    strongFiber->Run();\
  }\
  auto startedAt = TInstant::Now();\
  while (weakFiber.Lock()) {\
    if (TInstant::Now() - startedAt > TDuration::Seconds(5)) {\
      FAIL() << "Probably stuck.";\
      break;\
    } \
    Sleep(TDuration::MilliSeconds(1));\
  }\
  SUCCEED();\
}\
void GTEST_TEST_CLASS_NAME_(test_case_name, test_name)::TestInnerBody()
#define TEST_W(test_fixture, test_name)\
  TEST_W_(test_fixture, test_name, test_fixture, \
    ::testing::internal::GetTypeId<test_fixture>())

TEST_W(TFiberTest, SimpleAsync)
{
    auto asyncA = DelayedIdentity(3);
    int a = WaitFor(asyncA);

    auto asyncB = DelayedIdentity(4);
    int b = WaitFor(asyncB);

    EXPECT_EQ(7, a + b);
}

TEST_W(TFiberTest, SwitchToInvoker1)
{
    auto invoker = Queue1->GetInvoker();

    auto id0 = NThread::GetCurrentThreadId();
    auto id1 = GetInvokerThreadId(invoker);

    EXPECT_NE(id0, id1);

    for (int i = 0; i < 10; ++i) {
        SwitchTo(invoker);
        EXPECT_EQ(id1, NThread::GetCurrentThreadId());
    }
}

TEST_W(TFiberTest, SwitchToInvoker2)
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
        EXPECT_EQ(id1, NThread::GetCurrentThreadId());

        SwitchTo(invoker2);
        EXPECT_EQ(id2, NThread::GetCurrentThreadId());
    }
}

TEST_W(TFiberTest, TerminatedCaught)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel();

    EXPECT_THROW({ SwitchTo(invoker1); }, TFiberCanceledException);
}

TEST_W(TFiberTest, TerminatedPropagated)
{
    auto context = New<TCancelableContext>();

    auto invoker1 = context->CreateInvoker(Queue1->GetInvoker());
    auto invoker2 = Queue2->GetInvoker();

    SwitchTo(invoker2);

    context->Cancel();

    SwitchTo(invoker1);

    YUNREACHABLE();
}

TEST_W(TFiberTest, CurrentInvokerAfterSwitch1)
{
    auto invoker = Queue1->GetInvoker();

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

TEST_W(TFiberTest, CurrentInvokerAfterSwitch2)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(Queue1->GetInvoker());

    SwitchTo(invoker);

    EXPECT_EQ(invoker, GetCurrentInvoker());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFiberTest, ActionQueue1)
{
    auto invoker = Queue1->GetInvoker();

    TFiber* fiber1;
    NThread::TThreadId thread1;

    TFiber* fiber2;
    NThread::TThreadId thread2;

    BIND([&] () {
        fiber1 = TFiber::GetCurrent();
        thread1 = NThread::GetCurrentThreadId();

        WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));

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

        WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));

        fiber4 = TFiber::GetCurrent();
        thread4 = NThread::GetCurrentThreadId();
    }).AsyncVia(invoker).Run().Get();

    EXPECT_NE(fiber1, fiber3);
    EXPECT_NE(fiber2, fiber3);
    EXPECT_EQ(fiber3, fiber4);
    EXPECT_EQ(thread3, thread4);
}

TEST_F(TFiberTest, ActionQueue2)
{
    auto invoker = Queue1->GetInvoker();
    auto awaiter = New<TParallelAwaiter>(invoker);

    int sum = 0;
    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([&sum, i] () {
            WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));
            sum += i;
        });
        awaiter->Await(callback.AsyncVia(invoker).Run());
    }
    awaiter->Complete().Get();

    EXPECT_EQ(45, sum);
}

TEST_F(TFiberTest, ActionQueue3)
{
    auto invoker = Queue1->GetInvoker();
    auto awaiter = New<TParallelAwaiter>(invoker);

    int sum = 0;
    for (int i = 0; i < 10; ++i) {
        auto callback = BIND([&sum, i, invoker] () {
            auto thread1 = NThread::GetCurrentThreadId();
            WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));
            auto thread2 = NThread::GetCurrentThreadId();
            EXPECT_EQ(thread1, thread2);
            sum += i;
        });
        awaiter->Await(callback.AsyncVia(invoker).Run());
    }
    awaiter->Complete().Get();

    EXPECT_EQ(45, sum);
}

TEST_F(TFiberTest, CurrentInvokerSync)
{
    EXPECT_EQ(GetSyncInvoker(), GetCurrentInvoker())
        << "Current invoker: " << typeid(*GetCurrentInvoker()).name();
}

TEST_F(TFiberTest, CurrentInvokerInActionQueue)
{
    auto invoker = Queue1->GetInvoker();
    BIND([=] () {
        EXPECT_EQ(invoker, GetCurrentInvoker());
    })
    .AsyncVia(invoker).Run()
    .Get();
}

TEST_F(TFiberTest, CurrentInvokerConcurrent)
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

TEST_F(TFiberTest, NestedWaitFor)
{
    auto invoker = Queue1->GetInvoker();
    Stroka flow;

    auto fibA = New<TFiber>(TClosure());
    auto fibB = New<TFiber>(TClosure());

    fibA->Reset(BIND([&flow, fibB] {
        flow.append('A');
        fibB->Run();
        flow.append('D');
    }));
    fibB->Reset(BIND([&flow] {
        flow.append('B');
        WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));
        flow.append('C');
    }));

    BIND(&TFiber::Run, fibA).AsyncVia(invoker).Run().Get();

    EXPECT_EQ(EFiberState::Terminated, fibA->GetState());
    EXPECT_EQ(EFiberState::Terminated, fibB->GetState());

    EXPECT_STREQ("ABCD", flow.c_str());
}

TEST_F(TFiberTest, NestedWaitForOnManyLevels)
{
    auto invoker = Queue1->GetInvoker();
    Stroka flow;

    auto inner = BIND([&flow] (int i) {
        flow.append('B');
        WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));
        flow.append('C');
    });

    auto outer = BIND([&flow, inner] {
        for (int i = 0; i < 10; ++i) {
            flow.append('A');
            New<TFiber>(BIND(inner, 0))->Run();
            flow.append('D');
            WaitFor(MakeDelayed(TDuration::MilliSeconds(25)));
            flow.append('E');
            flow.append(' ');
        }
    });

    BIND(&TFiber::Run, New<TFiber>(outer)).AsyncVia(invoker).Run().Get();

    EXPECT_STREQ((Stroka("ABCDE ") * 10).c_str(), flow.c_str());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

