#include "stdafx.h"

#include <ytlib/actions/future.h>
#include <ytlib/actions/bind.h>
#include <ytlib/actions/callback.h>

#include <util/system/thread.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {
////////////////////////////////////////////////////////////////////////////////


TEST(TFutureTest, IsNull)
{
    TFuture<int> empty;
    TFuture<int> nonEmpty = MakeFuture(42);

    EXPECT_IS_TRUE(empty.IsNull());
    EXPECT_IS_FALSE(nonEmpty.IsNull());

    empty = MoveRV(nonEmpty);

    EXPECT_IS_FALSE(empty.IsNull());
    EXPECT_IS_TRUE(nonEmpty.IsNull());
}

TEST(TFutureTest, Reset)
{
    TFuture<int> foo = MakeFuture(42);

    EXPECT_IS_FALSE(foo.IsNull());
    foo.Reset();
    EXPECT_IS_TRUE(foo.IsNull());
}

TEST(TFutureTest, IsSet)
{
    TPromise<int> promise;
    TFuture<int> future = promise.ToFuture();

    EXPECT_IS_FALSE(future.IsSet());
    EXPECT_IS_FALSE(promise.IsSet());
    promise.Set(42);
    EXPECT_IS_TRUE(future.IsSet());
    EXPECT_IS_TRUE(promise.IsSet());
}

TEST(TFutureTest, SetAndGet)
{
    TPromise<int> promise;
    TFuture<int> future = promise.ToFuture();

    promise.Set(57);
    EXPECT_EQ(57, future.Get());
    EXPECT_EQ(57, future.Get()); // Second Get() should also work.
}

TEST(TFutureDeathTest, DoubleSet)
{
    // Debug-only.
    TPromise<int> promise;

    promise.Set(17);
    ASSERT_DEATH({ promise.Set(42); }, ".*");
}

TEST(TFutureTest, SetAndTryGet)
{
    TPromise<int> promise;
    TFuture<int> future = promise.ToFuture();

    {
        auto result = future.TryGet();
        EXPECT_IS_FALSE(result);
    }

    promise.Set(42);

    {
        auto result = future.TryGet();
        EXPECT_IS_TRUE(result);
        EXPECT_EQ(42, *result);
    }
}

class TMock
{
public:
    MOCK_METHOD1(Tackle, void(int));
};

TEST(TFutureTest, Subscribe)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tackle(42)).Times(1);
    EXPECT_CALL(secondMock, Tackle(42)).Times(1);

    auto firstSubscriber = BIND([&] (int x) { firstMock.Tackle(x); });
    auto secondSubscriber = BIND([&] (int x) { secondMock.Tackle(x); });

    TPromise<int> promise;
    promise.ToFuture().Subscribe(firstSubscriber);
    promise.Set(42);
    promise.ToFuture().Subscribe(secondSubscriber);
}

static void* AsynchronousSetter(void* param)
{
    Sleep(TDuration::Seconds(0.100));

    TPromise<int>* promise = reinterpret_cast<TPromise<int>*>(param);
    promise->Set(42);

    return NULL;
}

TEST(TFutureTest, SubscribeWithAsynchronousSet)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tackle(42)).Times(1);
    EXPECT_CALL(secondMock, Tackle(42)).Times(1);

    auto firstSubscriber = BIND([&] (int x) { firstMock.Tackle(x); });
    auto secondSubscriber = BIND([&] (int x) { secondMock.Tackle(x); });

    TPromise<int> promise;
    promise.ToFuture().Subscribe(firstSubscriber);

    TThread thread(&AsynchronousSetter, &promise);
    thread.Start();
    thread.Join();

    promise.ToFuture().Subscribe(secondSubscriber);
}

TEST(TFutureTest, Apply1)
{
    TPromise<int> promise;
    TFuture<int> future1 = promise.ToFuture();
    TFuture<int> future2 = future1.Apply(BIND([] (int x) { return x + 10; }));

    ASSERT_FALSE(future1.IsSet());
    ASSERT_FALSE(future2.IsSet());

    promise.Set(32);

    ASSERT_TRUE(future1.IsSet());
    ASSERT_TRUE(future2.IsSet());

    EXPECT_EQ(32, future1.Get());
    EXPECT_EQ(42, future2.Get());
}

TEST(TFutureTest, Apply2)
{
    TPromise<bool> kicker;

    TPromise<int> left;
    TPromise<int> right;

    TThread thread(&AsynchronousSetter, &left);

    TFuture<int> leftPrime =
        kicker.ToFuture()
        .Apply(BIND([=, &thread] (bool f) -> TFuture<int> {
            thread.Start();
            return left.ToFuture();
        }))
        .Apply(BIND([=] (int xv) -> int          {
            return xv + 8;
        }));
    TFuture<int> rightPrime =
        right.ToFuture()
        .Apply(BIND([=] (int xv) -> TFuture<int> {
            return MakeFuture(xv + 4);
        }));

    int accumulator = 0;
    TCallback<void(int)> accumulate = BIND([&] (int x) { accumulator += x; });

    leftPrime.Subscribe(accumulate);
    rightPrime.Subscribe(accumulate);

    // Initial computation condition.
    EXPECT_IS_FALSE(left.IsSet());  EXPECT_IS_FALSE(leftPrime.IsSet());
    EXPECT_IS_FALSE(right.IsSet()); EXPECT_IS_FALSE(rightPrime.IsSet());
    EXPECT_EQ(0, accumulator);

    // Kick off!
    kicker.Set(true);
    right.Set(1);

    // (1)
    EXPECT_IS_FALSE(left.IsSet());  EXPECT_IS_FALSE(leftPrime.IsSet());
    EXPECT_IS_TRUE(right.IsSet());  EXPECT_IS_TRUE(rightPrime.IsSet());
    EXPECT_EQ( 5, accumulator);
    EXPECT_EQ( 1, right.ToFuture().Get());
    EXPECT_EQ( 5, rightPrime.Get());

    // (2)
    thread.Join();

    // (3)
    EXPECT_IS_TRUE(left.IsSet());   EXPECT_IS_TRUE(leftPrime.IsSet());
    EXPECT_IS_TRUE(right.IsSet());  EXPECT_IS_TRUE(rightPrime.IsSet());
    EXPECT_EQ(55, accumulator);
    EXPECT_EQ(42, left.ToFuture().Get());
    EXPECT_EQ(50, leftPrime.Get());
}

////////////////////////////////////////////////////////////////////////////////
} // namespace <anonymous>
} // namespace NYT

