#include <yt/yt/library/auth_server/auth_cache.h>
#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <vector>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TControlledAuthCache
    : public TAuthCache<int, int, int>
{
public:
    std::vector<bool> TraceContextPresence;

    explicit TControlledAuthCache(TAuthCacheConfigPtr config)
        : TAuthCache(std::move(config))
    { }

private:
    TFuture<int> DoGet(const int& /*key*/, const int& /*context*/) noexcept override
    {
        TraceContextPresence.push_back(TryGetCurrentTraceContext() != nullptr);
        return MakeFuture<int>(std::ssize(TraceContextPresence));
    }
};

TEST(TAuthCacheTest, BackgroundRefreshDoesNotCaptureRequestTraceContext)
{
    auto config = New<TAuthCacheConfig>();
    config->CacheTtl = TDuration::Zero();
    auto cache = New<TControlledAuthCache>(config);

    auto traceContext = TTraceContext::NewRoot("AuthCacheRequest");
    TCurrentTraceContextGuard traceGuard(traceContext);
    EXPECT_EQ(WaitFor(cache->Get(1, 0)).ValueOrThrow(), 1);
    // Return the previous value and start a background refresh.
    EXPECT_EQ(WaitFor(cache->Get(1, 0)).ValueOrThrow(), 1);
    EXPECT_EQ(WaitFor(cache->Get(1, 0)).ValueOrThrow(), 2);

    EXPECT_THAT(cache->TraceContextPresence, ::testing::ElementsAre(true, false, false));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
