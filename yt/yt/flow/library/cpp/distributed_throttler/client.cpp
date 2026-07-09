#include "client.h"

#include "metrics_throttler.h"
#include "remote_throttler.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger DefaultLogger("DistributedThrottlerClient");

IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::function<TPriority()> priorityProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    if (!logger) {
        logger = DefaultLogger;
    }

    // Wrap the provider so each RPC gets a freshly-wrapped retrying channel
    // around whatever the caller's provider returns at that moment. A null
    // from the caller means "no leader known" — propagate it so TRemoteThrottler
    // can surface a clean error.
    auto retryingConfig = config->RetryingChannel;
    auto wrappedProvider = [callerProvider = std::move(channelProvider), retryingConfig] () -> NRpc::IChannelPtr {
        auto channel = callerProvider ? callerProvider() : NRpc::IChannelPtr();
        if (!channel) {
            return nullptr;
        }
        return NRpc::CreateRetryingChannel(retryingConfig, std::move(channel));
    };

    IStatusErrorStatePtr errorState;
    if (statusProfiler) {
        errorState = statusProfiler
            ->WithPrefix("/distributed_throttler")
            ->ErrorState(config->ThrottlerName);
    }

    auto remoteThrottler = New<TRemoteThrottler>(
        std::move(wrappedProvider),
        config->ThrottlerName,
        config->ClientId,
        config->RpcTimeout,
        std::move(priorityProvider),
        std::move(errorState),
        logger);

    // Prefetching absorbs bursts and batches RPCs to the server; it also
    // hides per-unit consumption from any upstream metrics, so wrap with
    // TMetricsTrackingThrottler to expose what the consumer actually draws.
    auto prefetching = CreatePrefetchingThrottler(
        config->PrefetchingConfig,
        remoteThrottler,
        logger);

    return CreateMetricsTrackingThrottler(
        std::move(prefetching),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
