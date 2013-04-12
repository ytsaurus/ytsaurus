#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/fibers/fiber.h>

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

    TFiber::Yield();

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

    self->Reset(BIND(&Fiber1, Unretained(main.Get()), Unretained(self.Get()), &v));
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
        Unretained(main.Get()),
        Unretained(fibA.Get()),
        Unretained(fibB.Get()),
        &v));
    fibB->Reset(BIND(
        &Fiber2B,
        Unretained(main.Get()),
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
        Unretained(main.Get()),
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
        Unretained(main.Get()),
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
        TFiber::Yield();
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
            TFiber::Yield();
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
            TFiber::Yield();
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

} // namespace
} // namespace NYT

