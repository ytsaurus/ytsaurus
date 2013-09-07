#include "stdafx.h"

#include <ytlib/misc/common.h>

#include <ytlib/concurrency/coroutine.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Coroutine0(TCoroutine<int()>& self)
{
    self.Yield(1);
    self.Yield(2);
    self.Yield(3);
    self.Yield(4);
    self.Yield(5);
}

TEST(TCoroutineTest, Nullary)
{
    TCoroutine<int()> coro(BIND(&Coroutine0));
    EXPECT_EQ(coro.GetState(), EFiberState::Initialized);

    int i;
    TNullable<int> actual;
    for (i = 1; /**/; ++i) {
        actual = coro.Run();
        if (coro.GetState() == EFiberState::Terminated) {
            break;
        }
        EXPECT_TRUE(actual.HasValue());
        EXPECT_EQ(actual.Get(), i);
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(i, 6);

    EXPECT_EQ(coro.GetState(), EFiberState::Terminated);
}

void Coroutine1(TCoroutine<int(int)>& self, int arg)
{
    EXPECT_EQ(arg, 0);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(arg, 2);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(arg, 4);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(arg, 6);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(arg, 8);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(arg, 10);
}

TEST(TCoroutineTest, Unary)
{
    TCoroutine<int(int)> coro(BIND(&Coroutine1));
    EXPECT_EQ(coro.GetState(), EFiberState::Initialized);

    // Alternative syntax.
    int i = 0, j = 0;
    TNullable<int> actual;
    while ((actual = coro.Run(j))) {
        ++i;
        EXPECT_EQ(i * 2 - 1, actual.Get());
        EXPECT_EQ(i * 2 - 2, j);
        j = actual.Get() + 1;
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(i, 5);
    EXPECT_EQ(j, 10);
}

// In this case I've got lazy and set up these test cases.
struct { int lhs; int rhs; int sum; } Coroutine2TestCases[] = {
    { 10, 20, 30 },
    { 11, 21, 32 },
    { 12, 22, 34 },
    { 13, 23, 36 },
    { 14, 24, 38 },
    { 15, 25, 40 }
};

void Coroutine2(TCoroutine<int(int, int)>& self, int lhs, int rhs)
{
    for (int i = 0; i < ARRAY_SIZE(Coroutine2TestCases); ++i) {
        EXPECT_EQ(lhs, Coroutine2TestCases[i].lhs) << "Iteration #" << i;
        EXPECT_EQ(rhs, Coroutine2TestCases[i].rhs) << "Iteration #" << i;
        std::tie(lhs, rhs) = self.Yield(lhs + rhs);
    }
}

TEST(TCoroutineTest, Binary)
{
    TCoroutine<int(int, int)> coro(BIND(&Coroutine2));
    EXPECT_EQ(coro.GetState(), EFiberState::Initialized);

    int i = 0;
    TNullable<int> actual;
    for (
        i = 0;
        (actual = coro.Run(
            Coroutine2TestCases[i].lhs,
            Coroutine2TestCases[i].rhs));
        ++i
    ) {
        EXPECT_EQ(Coroutine2TestCases[i].sum, actual.Get());
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(i, ARRAY_SIZE(Coroutine2TestCases));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

