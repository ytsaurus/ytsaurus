#include "stdafx.h"

#include <ytlib/actions/future.h>
#include <ytlib/actions/bind.h>
#include <ytlib/actions/callback.h>

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/invoker_util.h>

#include <util/system/thread.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

double SleepQuantum = 0.050;

TEST(TFutureTest, IsNull)
{
    TFuture<int> empty;
    TFuture<int> nonEmpty = MakeFuture(42);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = MoveRV(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    empty.Swap(nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST(TFutureTest, IsNullVoid)
{
    TFuture<void> empty;
    TFuture<void> nonEmpty = MakeFuture();

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = MoveRV(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    empty.Swap(nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST(TFutureTest, Reset)
{
    TFuture<int> foo = MakeFuture(42);

    EXPECT_TRUE(foo);
    foo.Reset();
    EXPECT_FALSE(foo);
}

TEST(TFutureTest, ResetVoid)
{
    TFuture<void> foo = MakeFuture();

    EXPECT_TRUE(foo);
    foo.Reset();
    EXPECT_FALSE(foo);
}

TEST(TFutureTest, IsSet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());
    EXPECT_FALSE(promise.IsSet());
    promise.Set(42);
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(promise.IsSet());
}

TEST(TFutureTest, IsSetVoid)
{
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();

    EXPECT_FALSE(future.IsSet());
    EXPECT_FALSE(promise.IsSet());
    promise.Set();
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(promise.IsSet());
}

TEST(TFutureTest, SetAndGet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    promise.Set(57);
    EXPECT_EQ(57, future.Get());
    EXPECT_EQ(57, future.Get()); // Second Get() should also work.
}

#ifndef NDEBUG
TEST(TFutureDeathTest, DoubleSet)
{
    // Debug-only.
    auto promise = NewPromise<int>();

    promise.Set(17);
    ASSERT_DEATH({ promise.Set(42); }, ".*");
}
#endif

TEST(TFutureTest, SetAndTryGet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    {
        auto result = future.TryGet();
        EXPECT_FALSE(result);
    }

    promise.Set(42);

    {
        auto result = future.TryGet();
        EXPECT_TRUE(result);
        EXPECT_EQ(42, *result);
    }
}

class TMock
{
public:
    MOCK_METHOD1(IntTackle, void(int));
    MOCK_METHOD0(VoidTackle, void(void));
};

TEST(TFutureTest, IntSubscribe)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, IntTackle(42)).Times(1);
    EXPECT_CALL(secondMock, IntTackle(42)).Times(1);

    auto firstSubscriber = BIND([&] (int x) { firstMock.IntTackle(x); });
    auto secondSubscriber = BIND([&] (int x) { secondMock.IntTackle(x); });

    auto promise = NewPromise<int>();

    promise.Subscribe(firstSubscriber);
    promise.Set(42);
    promise.Subscribe(secondSubscriber);
}

TEST(TFutureTest, VoidSubscribe)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, VoidTackle()).Times(1);
    EXPECT_CALL(secondMock, VoidTackle()).Times(1);

    auto firstSubscriber = BIND([&] { firstMock.VoidTackle(); });
    auto secondSubscriber = BIND([&] { secondMock.VoidTackle(); });

    auto promise = NewPromise<void>();

    promise.Subscribe(firstSubscriber);
    promise.Set();
    promise.Subscribe(secondSubscriber);
}

static void* AsynchronousIntSetter(void* param)
{
    Sleep(TDuration::Seconds(SleepQuantum));

    TPromise<int>* promise = reinterpret_cast<TPromise<int>*>(param);
    promise->Set(42);

    return NULL;
}

static void* AsynchronousVoidSetter(void* param)
{
    Sleep(TDuration::Seconds(SleepQuantum));

    TPromise<void>* promise = reinterpret_cast<TPromise<void>*>(param);
    promise->Set();

    return NULL;
}

TEST(TFutureTest, SubscribeWithAsynchronousSet)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, IntTackle(42)).Times(1);
    EXPECT_CALL(secondMock, IntTackle(42)).Times(1);

    auto firstSubscriber = BIND([&] (int x) { firstMock.IntTackle(x); });
    auto secondSubscriber = BIND([&] (int x) { secondMock.IntTackle(x); });

    auto promise = NewPromise<int>();

    promise.Subscribe(firstSubscriber);

    TThread thread(&AsynchronousIntSetter, &promise);
    thread.Start();
    thread.Join();

    promise.Subscribe(secondSubscriber);
}

TEST(TFutureTest, CascadedApply)
{
    TPromise<bool> kicker = NewPromise<bool>();

    TPromise<int>  left   = NewPromise<int>();
    TPromise<int>  right  = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &left);

    TFuture<int> leftPrime =
        kicker.ToFuture()
        .Apply(BIND([=, &thread] (bool f) -> TFuture<int> {
            thread.Start();
            return left.ToFuture();
        }))
        .Apply(BIND([=] (int xv) -> int {
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

    // Ensure that thread was not started.
    Sleep(TDuration::Seconds(2.0 * SleepQuantum));

    // Initial computation condition.
    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_FALSE(right.IsSet()); EXPECT_FALSE(rightPrime.IsSet());
    EXPECT_EQ(0, accumulator);

    // Kick off!
    kicker.Set(true);
    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_FALSE(right.IsSet()); EXPECT_FALSE(rightPrime.IsSet());
    EXPECT_EQ(0, accumulator);

    // Kick off!
    right.Set(1);

    EXPECT_FALSE(left.IsSet());  EXPECT_FALSE(leftPrime.IsSet());
    EXPECT_TRUE(right.IsSet());  EXPECT_TRUE(rightPrime.IsSet());
    EXPECT_EQ( 5, accumulator);
    EXPECT_EQ( 1, right.Get());
    EXPECT_EQ( 5, rightPrime.Get());

    // This will sleep for a while until left branch will be evaluated.
    thread.Join();

    EXPECT_TRUE(left.IsSet());   EXPECT_TRUE(leftPrime.IsSet());
    EXPECT_TRUE(right.IsSet());  EXPECT_TRUE(rightPrime.IsSet());
    EXPECT_EQ(55, accumulator);
    EXPECT_EQ(42, left.Get());
    EXPECT_EQ(50, leftPrime.Get());
}

TEST(TFutureTest, ApplyVoidToVoid)
{
    int state = 0;

    auto kicker = NewPromise<void>();

    TFuture<void> source = kicker.ToFuture();
    TFuture<void> target = source
        .Apply(BIND([&] () -> void { ++state; }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST(TFutureTest, ApplyVoidToFutureVoid)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    auto setter = NewPromise<void>();

    TThread thread(&AsynchronousVoidSetter, &setter);

    TFuture<void> source = kicker.ToFuture();
    TFuture<void> target = source
        .Apply(BIND([&] () -> TFuture<void> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(TDuration::Seconds(2.0 * SleepQuantum));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();
    
    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST(TFutureTest, ApplyVoidToInt)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    
    TFuture<void> source = kicker.ToFuture();
    TFuture<int>  target = source
        .Apply(BIND([&] () -> int {
            ++state;
            return 17;
        }));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();
    
    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(17, target.Get());
}

TEST(TFutureTest, ApplyVoidToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    auto setter = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &setter);

    TFuture<void> source = kicker.ToFuture();
    TFuture<int>  target = source
        .Apply(BIND([&] () -> TFuture<int> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(TDuration::Seconds(2.0 * SleepQuantum));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set();
    
    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(1, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(42, target.Get());
}

TEST(TFutureTest, ApplyIntToVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();

    TFuture<int>  source = kicker.ToFuture();
    TFuture<void> target = source
        .Apply(BIND([&] (int x) -> void { state += x; }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get());
}

TEST(TFutureTest, ApplyIntToFutureVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<void>();

    TThread thread(&AsynchronousVoidSetter, &setter);

    TFuture<int> source = kicker.ToFuture();
    TFuture<void> target = source
        .Apply(BIND([&] (int x) -> TFuture<void> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(TDuration::Seconds(2.0 * SleepQuantum));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);
    
    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());
}

TEST(TFutureTest, ApplyIntToInt)
{
    int state = 0;

    auto kicker = NewPromise<int>();

    TFuture<int> source = kicker.ToFuture();
    TFuture<int> target = source
        .Apply(BIND([&] (int x) -> int {
            state += x;
            return x * 2;
        }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get());
    EXPECT_EQ(42, target.Get());
}

TEST(TFutureTest, ApplyIntToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &setter);

    TFuture<int> source = kicker.ToFuture();
    TFuture<int> target = source
        .Apply(BIND([&] (int x) -> TFuture<int> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(TDuration::Seconds(2.0 * SleepQuantum));

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);
    
    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get());
    EXPECT_EQ(42, target.Get());
}

TEST(TFutureTest, Regression_de94ea0)
{
    int counter = 0;

    auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
    auto trigger = NewPromise<void>();

    awaiter->Await(trigger.ToFuture(), BIND([&counter] () { ++counter; }));

    EXPECT_EQ(0, counter);
    trigger.Set();
    EXPECT_EQ(1, counter);

    TPromise<void> completed(NewPromise<void>());
    awaiter->Complete(BIND(&TPromise<void>::Set, &completed));
    EXPECT_TRUE(completed.IsSet());
}

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT
