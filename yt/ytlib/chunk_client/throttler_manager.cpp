#include "throttler_manager.h"

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TThrottlerManager::TThrottlerManager(
    TThroughputThrottlerConfigPtr config,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Config_(config)
    , Logger_(logger)
    , Profiler_(profiler)
{ }

IThroughputThrottlerPtr TThrottlerManager::GetThrottler(TCellTag cellTag)
{
    TGuard<TSpinLock> guard(SpinLock_);

    auto it = ThrottlerMap_.find(cellTag);
    if (it != ThrottlerMap_.end()) {
        return it->second;
    }

    auto logger = Logger_;
    logger.AddTag("CellTag: %v", cellTag);
    auto throttler = CreateReconfigurableThroughputThrottler(Config_, logger, Profiler_);

    YCHECK(ThrottlerMap_.insert(std::make_pair(cellTag, throttler)).second);

    return throttler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
