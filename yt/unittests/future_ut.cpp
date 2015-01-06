#include "stdafx.h"
#include "framework.h"

#include <core/actions/future.h>
#include <core/actions/invoker_util.h>
#include <core/actions/cancelable_context.h>

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/parallel_collector.h>

#include <util/system/thread.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(50);

TEST(TFutureTest, IsNull)
{
    TFuture<int> empty;
    TFuture<int> nonEmpty = MakeFuture(42);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = std::move(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    swap(empty, nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST(TFutureTest, IsNullVoid)
{
    TFuture<void> empty;
    TFuture<void> nonEmpty = VoidFuture;

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);

    empty = std::move(nonEmpty);

    EXPECT_TRUE(empty);
    EXPECT_FALSE(nonEmpty);

    swap(empty, nonEmpty);

    EXPECT_FALSE(empty);
    EXPECT_TRUE(nonEmpty);
}

TEST(TFutureTest, Reset)
{
    auto foo = MakeFuture(42);

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

TEST(TFutureTest, SetAndGet)
{
    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    promise.Set(57);
    EXPECT_EQ(57, future.Get().Value());
    EXPECT_EQ(57, future.Get().Value()); // Second Get() should also work.
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
        EXPECT_TRUE(result.HasValue());
        EXPECT_EQ(42, result->Value());
    }
}

class TMock
{
public:
    MOCK_METHOD1(Tacke, void(int));
};

TEST(TFutureTest, Subscribe)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tacke(42)).Times(1);
    EXPECT_CALL(secondMock, Tacke(42)).Times(1);

    auto firstSubscriber = BIND([&] (const TErrorOr<int>& x) { firstMock.Tacke(x.Value()); });
    auto secondSubscriber = BIND([&] (const TErrorOr<int>& x) { secondMock.Tacke(x.Value()); });

    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    future.Subscribe(firstSubscriber);
    promise.Set(42);
    future.Subscribe(secondSubscriber);
}

static void* AsynchronousIntSetter(void* param)
{
    Sleep(SleepQuantum);

    auto* promise = reinterpret_cast<TPromise<int>*>(param);
    promise->Set(42);

    return NULL;
}

static void* AsynchronousVoidSetter(void* param)
{
    Sleep(SleepQuantum);

    auto* promise = reinterpret_cast<TPromise<void>*>(param);
    promise->Set();

    return NULL;
}

TEST(TFutureTest, SubscribeWithAsynchronousSet)
{
    TMock firstMock;
    TMock secondMock;

    EXPECT_CALL(firstMock, Tacke(42)).Times(1);
    EXPECT_CALL(secondMock, Tacke(42)).Times(1);

    auto firstSubscriber = BIND([&] (const TErrorOr<int>& x) { firstMock.Tacke(x.Value()); });
    auto secondSubscriber = BIND([&] (const TErrorOr<int>& x) { secondMock.Tacke(x.Value()); });

    auto promise = NewPromise<int>();
    auto future = promise.ToFuture();

    future.Subscribe(firstSubscriber);

    TThread thread(&AsynchronousIntSetter, &promise);
    thread.Start();
    thread.Join();

    future.Subscribe(secondSubscriber);
}

TEST(TFutureTest, CascadedApply)
{
    auto kicker = NewPromise<bool>();

    auto left   = NewPromise<int>();
    auto right  = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &left);

    auto leftPrime =
        kicker.ToFuture()
        .Apply(BIND([=, &thread] (bool f) -> TFuture<int> {
            thread.Start();
            return left.ToFuture();
        }))
        .Apply(BIND([=] (int xv) -> int {
            return xv + 8;
        }));
    auto rightPrime =
        right.ToFuture()
        .Apply(BIND([=] (int xv) -> TFuture<int> {
            return MakeFuture(xv + 4);
        }));

    int accumulator = 0;
    auto accumulate = BIND([&] (const TErrorOr<int>& x) { accumulator += x.Value(); });

    leftPrime.Subscribe(accumulate);
    rightPrime.Subscribe(accumulate);

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

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
    EXPECT_EQ( 1, right.Get().Value());
    EXPECT_EQ( 5, rightPrime.Get().Value());

    // This will sleep for a while until left branch will be evaluated.
    thread.Join();

    EXPECT_TRUE(left.IsSet());   EXPECT_TRUE(leftPrime.IsSet());
    EXPECT_TRUE(right.IsSet());  EXPECT_TRUE(rightPrime.IsSet());
    EXPECT_EQ(55, accumulator);
    EXPECT_EQ(42, left.Get().Value());
    EXPECT_EQ(50, leftPrime.Get().Value());
}

TEST(TFutureTest, ApplyVoidToVoid)
{
    int state = 0;

    auto kicker = NewPromise<void>();

    auto source = kicker.ToFuture();
    auto target = source
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

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] () -> TFuture<void> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

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

    auto source = kicker.ToFuture();
    auto target = source
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

    EXPECT_EQ(17, target.Get().Value());
}

TEST(TFutureTest, ApplyVoidToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<void>();
    auto setter = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &setter);

    auto source = kicker.ToFuture();
    auto  target = source
        .Apply(BIND([&] () -> TFuture<int> {
            ++state;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

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

    EXPECT_EQ(42, target.Get().Value());
}

TEST(TFutureTest, ApplyIntToVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();

    auto  source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> void { state += x; }));

    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());
}

TEST(TFutureTest, ApplyIntToFutureVoid)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<void>();

    TThread thread(&AsynchronousVoidSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> TFuture<void> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());

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

    auto source = kicker.ToFuture();
    auto target = source
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

    EXPECT_EQ(21, source.Get().Value());
    EXPECT_EQ(42, target.Get().Value());
}

TEST(TFutureTest, ApplyIntToFutureInt)
{
    int state = 0;

    auto kicker = NewPromise<int>();
    auto setter = NewPromise<int>();

    TThread thread(&AsynchronousIntSetter, &setter);

    auto source = kicker.ToFuture();
    auto target = source
        .Apply(BIND([&] (int x) -> TFuture<int> {
            state += x;
            thread.Start();
            return setter.ToFuture();
        }));

    // Ensure that thread was not started.
    Sleep(SleepQuantum * 2);

    // Initial computation condition.
    EXPECT_EQ(0, state);
    EXPECT_FALSE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    // Kick off!
    kicker.Set(21);

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_FALSE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());

    // This will sleep for a while until evaluation completion.
    thread.Join();

    EXPECT_EQ(21, state);
    EXPECT_TRUE(source.IsSet());
    EXPECT_TRUE(target.IsSet());

    EXPECT_EQ(21, source.Get().Value());
    EXPECT_EQ(42, target.Get().Value());
}

TEST(TFutureTest, Regression_de94ea0)
{
    int counter = 0;

    auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
    auto trigger = NewPromise<void>();

    awaiter->Await(
        trigger.ToFuture(),
        BIND([&counter] (const TError& error) {
            EXPECT_TRUE(error.IsOK());
            ++counter;
        }));

    EXPECT_EQ(0, counter);
    trigger.Set();
    EXPECT_EQ(1, counter);

    auto completed = NewPromise<void>();
    completed.SetFrom(awaiter->Complete());

    EXPECT_TRUE(completed.IsSet());
}

static TFuture<int> AsyncDivide(int a, int b, TDuration delay)
{
    auto promise = NewPromise<int>();
    TDelayedExecutor::Submit(
        BIND([=] () mutable {
            if (b == 0) {
                promise.Set(TError("Division by zero"));
            } else {
                promise.Set(a / b);
            }
        }),
        delay);
    return promise;
}

TEST(TFutureTest, ParallelCollectorEmpty)
{
    auto collector = New< TParallelCollector<int> >();
    auto resultOrError = collector->Complete().Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_TRUE(result.size() == 0);
}

TEST(TFutureTest, ParallelCollectorSuccess)
{
    auto collector = New< TParallelCollector<int> >();
    collector->Collect(AsyncDivide(5, 2, TDuration::Seconds(0.1)));
    collector->Collect(AsyncDivide(30, 3, TDuration::Seconds(0.2)));
    auto resultOrError = collector->Complete().Get();
    EXPECT_TRUE(resultOrError.IsOK());
    const auto& result = resultOrError.Value();
    EXPECT_TRUE(result.size() == 2);
    EXPECT_TRUE(result[0] == 2);
    EXPECT_TRUE(result[1] == 10);
}

TEST(TFutureTest, ParallelCollectorError)
{
    auto collector = New< TParallelCollector<int> >();
    collector->Collect(AsyncDivide(5, 2, TDuration::Seconds(0.1)));
    collector->Collect(AsyncDivide(30, 0, TDuration::Seconds(0.2)));
    auto resultOrError = collector->Complete().Get();
    EXPECT_FALSE(resultOrError.IsOK());
}

TEST(TFutureTest, AsyncViaCanceledInvoker)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetSyncInvoker());
    auto generator = BIND([] () {}).AsyncVia(invoker);
    context->Cancel();
    auto future = generator.Run();
    auto error = future.Get();
    ASSERT_EQ(NYT::EErrorCode::Canceled, error.GetCode());
}

TEST(TFutureTest, LastPromiseDied)
{
    TFuture<void> future;
    {
        auto promise = NewPromise<void>();
        future = promise;
        EXPECT_FALSE(future.IsSet());
    }
    Sleep(SleepQuantum);
    EXPECT_TRUE(future.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, future.Get().GetCode());
}

TEST(TFutureTest, PropagateErrorSync)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.Apply(BIND([] (int x) { return x + 1; }));
    p.Set(TError("Oops"));
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST(TFutureTest, PropagateErrorAsync)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.Apply(BIND([] (int x) { return MakeFuture(x + 1);}));
    p.Set(TError("Oops"));
    EXPECT_TRUE(f2.IsSet());
    EXPECT_FALSE(f2.Get().IsOK());
}

TEST(TFutureTest, Timeout)
{
    auto p = NewPromise<int>();
    auto f1 = p.ToFuture();
    auto f2 = f1.WithTimeout(SleepQuantum);
    auto result = f2.Get();
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Timeout, result.GetCode());
}

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT
