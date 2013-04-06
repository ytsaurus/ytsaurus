#include "stdafx.h"

#include <ytlib/misc/common.h>
#include <ytlib/fibers/coroutine.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Coroutine1(TCoroutine<int()>& self)
{
    self.Yield(1);
    self.Yield(2);
    self.Yield(3);
    self.Yield(4);
    self.Yield(5);
}

TEST(TCoroutineTest, Simple)
{
    TCoroutine<int()> coro(BIND(&Coroutine1));
    EXPECT_EQ(coro.GetState(), TFiber::EState::Initialized);

    int i;
    TNullable<int> actual;
    for (i = 1; /**/; ++i) {
        actual = coro.Run();
        if (coro.GetState() == TFiber::EState::Terminated) {
            break;
        }
        EXPECT_TRUE(actual.HasValue());
        EXPECT_EQ(actual.Get(), i);
    }

    EXPECT_FALSE(actual.HasValue());
    EXPECT_EQ(i, 6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

