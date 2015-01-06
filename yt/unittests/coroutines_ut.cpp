#include "stdafx.h"
#include "framework.h"

#include <core/misc/public.h>

#include <core/concurrency/coroutine.h>

namespace NYT {
namespace NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCoroutineTest
    : public ::testing::Test
{ };

void Coroutine0(TCoroutine<int()>& self)
{
    self.Yield(1);
    self.Yield(2);
    self.Yield(3);
    self.Yield(4);
    self.Yield(5);
}

TEST_F(TCoroutineTest, Nullary)
{
    TCoroutine<int()> coro(BIND(&Coroutine0));
    EXPECT_FALSE(coro.IsCompleted());

    int i;
    TNullable<int> actual;
    for (i = 1; /**/; ++i) {
        actual = coro.Run();
        if (coro.IsCompleted()) {
            break;
        }
        EXPECT_TRUE(actual.HasValue());
        EXPECT_EQ(i, actual.Get());
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(6, i);

    EXPECT_TRUE(coro.IsCompleted());
}

void Coroutine1(TCoroutine<int(int)>& self, int arg)
{
    EXPECT_EQ(0, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(2, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(4, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(6, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(8, arg);
    std::tie(arg) = self.Yield(arg + 1);
    EXPECT_EQ(10, arg);
}

TEST_F(TCoroutineTest, Unary)
{
    TCoroutine<int(int)> coro(BIND(&Coroutine1));
    EXPECT_FALSE(coro.IsCompleted());

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
    EXPECT_EQ(5, i);
    EXPECT_EQ(10, j);

    EXPECT_TRUE(coro.IsCompleted());
}

// In this case I've got lazy and set up these test cases.
struct TTestCase { 
    int lhs;
    int rhs;
    int sum;
};

std::vector<TTestCase> Coroutine2TestCases = {
    { 10, 20, 30 },
    { 11, 21, 32 },
    { 12, 22, 34 },
    { 13, 23, 36 },
    { 14, 24, 38 },
    { 15, 25, 40 }
};

void Coroutine2(TCoroutine<int(int, int)>& self, int lhs, int rhs)
{
    for (int i = 0; i < Coroutine2TestCases.size(); ++i) {
        EXPECT_EQ(Coroutine2TestCases[i].lhs, lhs) << "Iteration #" << i;
        EXPECT_EQ(Coroutine2TestCases[i].rhs, rhs) << "Iteration #" << i;
        std::tie(lhs, rhs) = self.Yield(lhs + rhs);
    }
}

TEST_F(TCoroutineTest, Binary)
{
    TCoroutine<int(int, int)> coro(BIND(&Coroutine2));
    EXPECT_FALSE(coro.IsCompleted());

    int i = 0;
    TNullable<int> actual;
    for (
        i = 0;
        (actual = coro.Run(
            i < Coroutine2TestCases.size() ? Coroutine2TestCases[i].lhs : 0,
            i < Coroutine2TestCases.size() ? Coroutine2TestCases[i].rhs : 0));
        ++i
    ) {
        EXPECT_EQ(Coroutine2TestCases[i].sum, actual.Get());
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(i, Coroutine2TestCases.size());

    EXPECT_TRUE(coro.IsCompleted());
}

void Coroutine3(TCoroutine<void()>& self)
{
    for (int i = 0; i < 10; ++i) {
        WaitFor(MakeDelayed(TDuration::MilliSeconds(1)));
        self.Yield();
    }
}

TEST_W(TCoroutineTest, WaitFor)
{
    TCoroutine<void()> coro(BIND(&Coroutine3));
    for (int i = 0; i < 11; ++i) {
        EXPECT_FALSE(coro.IsCompleted());
        coro.Run();
    }
    EXPECT_TRUE(coro.IsCompleted());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NConcurrency
} // namespace NYT

