#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFinalizerThreadTest, DISABLED_DebugShutdown)
{
    auto actionQueue = New<TActionQueue>("ShutdownHang");

    BIND([actionQueue] {
        auto promise = NewPromise<void>();
        WaitFor(promise.ToFuture())
            .ThrowOnError();
    })
        .AsyncVia(actionQueue->GetInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

