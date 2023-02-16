#include "per_user_queues.h"

namespace NYT::NRpc {

static const auto InfiniteRequestThrottlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();

////////////////////////////////////////////////////////////////////////////////

TPerUserRequestQueues::TPerUserRequestQueues()
    : ThrottlerProfiler_()
{
    TRequestQueueThrottlerConfigs configs = {
        InfiniteRequestThrottlerConfig,
        InfiniteRequestThrottlerConfig
    };
    DefaultConfigs_.Store(std::move(configs));
}

TPerUserRequestQueues::TPerUserRequestQueues(
    TReconfigurationCallback reconfigurationCallback,
    NProfiling::TProfiler throttlerProfiler)
    : ReconfigurationCallback_(std::move(reconfigurationCallback))
    , ThrottlerProfiler_(std::move(throttlerProfiler))
{
    // Creating pool for the root user to make sure it won't be throttled.
    RequestQueues_.FindOrInsert(RootUserName, [&] {
        auto queue = CreateRequestQueue(RootUserName, ThrottlerProfiler_);

        auto config = InfiniteRequestThrottlerConfig;

        auto guard = ReaderGuard(ThrottlingEnabledFlagsSpinLock_);
        if (WeightThrottlingEnabled_) {
            queue->ConfigureWeightThrottler(config);
        }
        if (BytesThrottlingEnabled_) {
            queue->ConfigureBytesThrottler(config);
        }

        return queue;
    });
}

TRequestQueuePtr TPerUserRequestQueues::GetOrCreateUserQueue(const TString& userName)
{
    // It doesn't matter if configs are outdated in this thread since we will reconfigure them
    // again in the automaton thread.
    auto configs = DefaultConfigs_.Load();

    auto queue = RequestQueues_.FindOrInsert(userName, [&] {
        auto queue = CreateRequestQueue(userName, ThrottlerProfiler_);

        auto guard = ReaderGuard(ThrottlingEnabledFlagsSpinLock_);
        if (WeightThrottlingEnabled_) {
            queue->ConfigureWeightThrottler(configs.WeightThrottlerConfig);
        }
        if (BytesThrottlingEnabled_) {
            queue->ConfigureBytesThrottler(configs.BytesThrottlerConfig);
        }

        if (ReconfigurationCallback_) {
            ReconfigurationCallback_(userName, queue);
        }

        return queue;
    }).first;

    return *queue;
}

TRequestQueueProvider TPerUserRequestQueues::GetProvider()
{
    return BIND_NO_PROPAGATE([this] (const NRpc::NProto::TRequestHeader& header) {
        const auto& name = header.has_user()
            ? header.user()
            : RootUserName;
        return GetOrCreateUserQueue(name).Get();
    });
}

void TPerUserRequestQueues::EnableThrottling(bool enableWeightThrottling, bool enableBytesThrottling)
{
    {
        auto guard = WriterGuard(ThrottlingEnabledFlagsSpinLock_);
        WeightThrottlingEnabled_ = enableWeightThrottling;
        BytesThrottlingEnabled_ = enableBytesThrottling;
    }

    if (ReconfigurationCallback_) {
        RequestQueues_.Flush();
        RequestQueues_.IterateReadOnly([&] (const auto& userName, const auto& queue) {
            ReconfigurationCallback_(userName, queue);
        });
    }
}

void TPerUserRequestQueues::ReconfigureCustomUserThrottlers(const TString& userName)
{
    if (!ReconfigurationCallback_) {
        return;
    }

    // Extra layer of defense from throttling root and killing server as a result.
    if (userName == RootUserName) {
        return;
    }

    if (auto* queue = RequestQueues_.Find(userName)) {
        ReconfigurationCallback_(userName, *queue);
    }
}

void TPerUserRequestQueues::ReconfigureDefaultUserThrottlers(const TRequestQueueThrottlerConfigs& configs)
{
    YT_ASSERT(configs.WeightThrottlerConfig && configs.BytesThrottlerConfig);

    DefaultConfigs_.Store(configs);
    if (ReconfigurationCallback_) {
        RequestQueues_.Flush();
        RequestQueues_.IterateReadOnly([&] (const auto& userName, const auto& queue) {
            ReconfigurationCallback_(userName, queue);
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
