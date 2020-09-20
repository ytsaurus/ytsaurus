#include "throttler_manager.h"

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/profiling/profile_manager.h>

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
    TGuard<TAdaptiveLock> guard(SpinLock_);
    Config_ = config;
    for (auto& pair : ThrottlerMap_) {
        pair.second->Reconfigure(config);
    }
}

IThroughputThrottlerPtr TThrottlerManager::GetThrottler(TCellTag cellTag)
{
    TGuard<TAdaptiveLock> guard(SpinLock_);

    auto it = ThrottlerMap_.find(cellTag);
    if (it != ThrottlerMap_.end()) {
        return it->second;
    }

    auto logger = Logger_;
    logger.AddTag("CellTag: %v", cellTag);

    TTagIdList tagIds{
        TProfileManager::Get()->RegisterTag("cell_tag", cellTag)
    };

    auto throttler = CreateReconfigurableThroughputThrottler(
        Config_,
        logger,
        Profiler_.AddTags(tagIds));

    YT_VERIFY(ThrottlerMap_.insert(std::make_pair(cellTag, throttler)).second);

    return throttler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
