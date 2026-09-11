#include <yt/yt/flow/library/cpp/connectors/sorted_dynamic_table/retrying_writer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/test_framework/framework.h>

#include <limits>
#include <thread>

namespace NYT::NFlow::NSortedDynamicTable {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

TExponentialBackoffOptions MakeImmediateBackoffOptions()
{
    return {
        .InvocationCount = std::numeric_limits<int>::max(),
        .MinBackoff = TDuration::Zero(),
        .MaxBackoff = TDuration::Zero(),
        .BackoffJitter = 0.0,
    };
}

class TTestWriter
    : public TRefCounted
{
public:
    explicit TTestWriter(std::shared_ptr<std::atomic<bool>> destroyed)
        : Destroyed_(std::move(destroyed))
    { }

    ~TTestWriter() override
    {
        Destroyed_->store(true);
    }

private:
    const std::shared_ptr<std::atomic<bool>> Destroyed_;
};

DEFINE_REFCOUNTED_TYPE(TTestWriter);

TEST(TAsyncRetryingWriterTest, SemaphoreWaitDoesNotRetainWriter)
{
    auto semaphore = New<TAsyncSemaphore>(1);
    auto occupiedGuard = WaitFor(semaphore->AsyncAcquire().AsUnique())
        .ValueOrThrow();
    auto actionQueue = New<TActionQueue>("RetryingWriterTest");
    auto invoker = CreateSerializedInvoker(actionQueue->GetInvoker());

    auto destroyed = std::make_shared<std::atomic<bool>>(false);
    auto writer = New<TTestWriter>(destroyed);
    auto writeFuture = NDetail::RunSerializedRetries(
        MakeWeak(writer.Get()),
        semaphore,
        invoker,
        MakeImmediateBackoffOptions(),
        [] (TTestWriter* /*writer*/) {
            return true;
        });

    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (semaphore->GetWaiterCount() != 1 && TInstant::Now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_EQ(semaphore->GetWaiterCount(), 1);
    writer.Reset();
    EXPECT_TRUE(destroyed->load());

    occupiedGuard.Release();
    WaitFor(writeFuture).ThrowOnError();
    actionQueue->Shutdown();
}

TEST(TAsyncRetryingWriterTest, RetriesUntilSuccess)
{
    auto semaphore = New<TAsyncSemaphore>(1);
    auto actionQueue = New<TActionQueue>("RetryingWriterTest");
    auto invoker = CreateSerializedInvoker(actionQueue->GetInvoker());

    auto destroyed = std::make_shared<std::atomic<bool>>(false);
    auto writer = New<TTestWriter>(destroyed);
    std::atomic<int> attemptCount = 0;
    auto writeFuture = NDetail::RunSerializedRetries(
        MakeWeak(writer.Get()),
        semaphore,
        invoker,
        MakeImmediateBackoffOptions(),
        [&] (TTestWriter* /*writer*/) {
            return ++attemptCount == 3;
        });

    WaitFor(writeFuture).ThrowOnError();
    EXPECT_EQ(attemptCount.load(), 3);
    actionQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NSortedDynamicTable
