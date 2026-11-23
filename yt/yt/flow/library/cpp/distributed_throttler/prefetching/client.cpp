#include "client.h"

#include "remote_throttler.h"

#include <yt/yt/flow/library/cpp/common/controller/channel_provider.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/client.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/local_throttler.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NFlow::NDistributedThrottler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger DefaultLogger("DistributedThrottlerClient");

IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    NRpc::IChannelPtr controllerChannel,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    if (!logger) {
        logger = DefaultLogger;
    }

    IStatusErrorStatePtr errorState;
    if (statusProfiler) {
        errorState = statusProfiler
            ->WithPrefix("/distributed_throttler")
            ->ErrorState(config->ThrottlerName);
    }

    auto remoteThrottler = New<TRemoteThrottler>(
        NRpc::CreateRetryingChannel(config->RetryingChannel, std::move(controllerChannel)),
        config->ThrottlerName,
        config->ClientId,
        config->RpcTimeout,
        std::move(priorityProvider),
        std::move(quotaClassProvider),
        std::move(errorState),
        logger);

    auto prefetching = CreatePrefetchingThrottler(
        config->PrefetchingConfig,
        remoteThrottler,
        logger);

    return CreateLocalThrottler(
        std::move(prefetching),
        remoteThrottler->GetFailureFuture(),
        std::move(profiler));
}

IThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerClientConfigPtr config,
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::function<TPriority()> priorityProvider,
    std::function<TQuotaClassId()> quotaClassProvider,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    return CreateDistributedThrottler(
        std::move(config),
        NRpc::CreateRoamingChannel(New<TControllerChannelProvider>(std::move(channelProvider))),
        std::move(priorityProvider),
        std::move(quotaClassProvider),
        std::move(statusProfiler),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
