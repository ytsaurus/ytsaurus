#include "throttler_manager.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TThrottlerManager::TThrottlerManager(
    TThroughputThrottlerConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Config_(std::move(config))
    , Logger_(std::move(logger))
    , Profiler_(std::move(profiler))
{ }

void TThrottlerManager::Reconfigure(TThroughputThrottlerConfigPtr config)
{
    auto guard = Guard(SpinLock_);
    Config_ = config;
    for (const auto& [cellTag, throttler] : ThrottlerMap_) {
        throttler->Reconfigure(config);
    }
}

IThroughputThrottlerPtr TThrottlerManager::GetThrottler(TCellTag cellTag)
{
    auto guard = Guard(SpinLock_);

    auto it = ThrottlerMap_.find(cellTag);
    if (it != ThrottlerMap_.end()) {
        return it->second;
    }

    auto throttler = CreateReconfigurableThroughputThrottler(
        Config_,
        Logger_.WithTag("CellTag: %v", cellTag),
        Profiler_.WithTag("cell_tag", ToString(cellTag)));

    YT_VERIFY(ThrottlerMap_.emplace(cellTag, throttler).second);

    return throttler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
