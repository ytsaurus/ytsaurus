#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/fibers/fiber.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Fiber1(TFiberPtr main, TFiber* self, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_EQ(TFiber::GetCurrent(), self);
    EXPECT_NE(TFiber::GetCurrent(), main);
    EXPECT_EQ(self->GetState(), TFiber::EState::Running);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);

    TFiber::Yield();

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_EQ(TFiber::GetCurrent(), self);
    EXPECT_NE(TFiber::GetCurrent(), main);
    EXPECT_EQ(self->GetState(), TFiber::EState::Running);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
}

TEST(TFiberTest, Simple)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto self = New<TFiber>(TClosure());

    self->Reset(BIND(&Fiber1, main, Unretained(self.Get()), &v));
    EXPECT_NE(main, self);

    EXPECT_EQ(0, v);
    EXPECT_EQ(TFiber::GetCurrent(), main);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(self->GetState(), TFiber::EState::Initialized);

    self->Run();
    ++v;

    EXPECT_EQ(2, v);
    EXPECT_EQ(TFiber::GetCurrent(), main);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(self->GetState(), TFiber::EState::Suspended);

    self->Run();
    ++v;

    EXPECT_EQ(4, v);
    EXPECT_EQ(TFiber::GetCurrent(), main);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(self->GetState(), TFiber::EState::Terminated);
}

void Fiber2A(TFiberPtr main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(1, *p);
    EXPECT_NE(TFiber::GetCurrent(), main);
    EXPECT_EQ(TFiber::GetCurrent(), fibA);
    EXPECT_NE(TFiber::GetCurrent(), fibB);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibA->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibB->GetState(), TFiber::EState::Initialized);

    fibB->Run();

    (void)(*p)++;
    EXPECT_EQ(3, *p);
    EXPECT_NE(TFiber::GetCurrent(), main);
    EXPECT_EQ(TFiber::GetCurrent(), fibA);
    EXPECT_NE(TFiber::GetCurrent(), fibB);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibA->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibB->GetState(), TFiber::EState::Terminated);
}

void Fiber2B(TFiberPtr main, TFiber* fibA, TFiber* fibB, int* p)
{
    (void)(*p)++;
    EXPECT_EQ(2, *p);
    EXPECT_NE(TFiber::GetCurrent(), main);
    EXPECT_NE(TFiber::GetCurrent(), fibA);
    EXPECT_EQ(TFiber::GetCurrent(), fibB);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibA->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibB->GetState(), TFiber::EState::Running);
}

TEST(TFiberTest, Nested)
{
    int v = 0;

    auto main = TFiber::GetCurrent();
    auto fibA = New<TFiber>(TClosure());
    auto fibB = New<TFiber>(TClosure());

    fibA->Reset(BIND(
        &Fiber2A, main, Unretained(fibA.Get()), Unretained(fibB.Get()), &v));
    fibB->Reset(BIND(
        &Fiber2B, main, Unretained(fibA.Get()), Unretained(fibB.Get()), &v));
    EXPECT_NE(main, fibA);
    EXPECT_NE(main, fibB);

    EXPECT_EQ(0, v);
    EXPECT_EQ(TFiber::GetCurrent(), main);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibA->GetState(), TFiber::EState::Initialized);
    EXPECT_EQ(fibB->GetState(), TFiber::EState::Initialized);

    fibA->Run();
    ++v;

    EXPECT_EQ(4, v);
    EXPECT_EQ(TFiber::GetCurrent(), main);
    EXPECT_EQ(main->GetState(), TFiber::EState::Running);
    EXPECT_EQ(fibA->GetState(), TFiber::EState::Terminated);
    EXPECT_EQ(fibB->GetState(), TFiber::EState::Terminated);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

