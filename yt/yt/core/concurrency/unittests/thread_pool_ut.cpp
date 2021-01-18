#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/actions/invoker.h>
#include <yt/core/actions/future.h>

#include <util/random/random.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TThreadPoolTest, Configure)
{
    auto threadPool = New<TThreadPool>(1, "Test");
    auto counter = std::make_shared<std::atomic<int>>();
    auto callback = BIND([=] { ++*counter; });
    std::vector<TFuture<void>> futures;

    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        futures.push_back(callback.AsyncVia(threadPool->GetInvoker()).Run());
        if (i % 100 == 0) {
            threadPool->Configure(RandomNumber<size_t>(10) + 1);
        }
    }

    AllSucceeded(std::move(futures))
        .Get();
    threadPool->Shutdown();
    EXPECT_EQ(N, counter->load());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

