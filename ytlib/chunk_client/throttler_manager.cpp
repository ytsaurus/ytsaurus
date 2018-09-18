#include "throttler_manager.h"

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TThrottlerManager::TThrottlerManager(
    TThroughputThrottlerConfigPtr config,
    const NLogging::TLogger& logger)
    : Config_(config)
    , Logger_(logger)
{ }

void TThrottlerManager::Reconfigure(TThroughputThrottlerConfigPtr config)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Config_ = config;
    for (auto& pair : ThrottlerMap_) {
        pair.second->Reconfigure(config);
    }
}

IThroughputThrottlerPtr TThrottlerManager::GetThrottler(TCellTag cellTag)
{
    TGuard<TSpinLock> guard(SpinLock_);

    auto it = ThrottlerMap_.find(cellTag);
    if (it != ThrottlerMap_.end()) {
        return it->second;
    }

    auto logger = Logger_;
    logger.AddTag("CellTag: %v", cellTag);

    TTagIdList tagIds { TProfileManager::Get()->RegisterTag("cell_tag", cellTag) };

    auto throttler = CreateReconfigurableThroughputThrottler(
        Config_,
        logger,
        TProfiler("/locate_chunks_throttler", tagIds));

    YCHECK(ThrottlerMap_.insert(std::make_pair(cellTag, throttler)).second);

    return throttler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
