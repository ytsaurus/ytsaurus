#include "stdafx.h"
#include "throttler_manager.h"

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
    , Logger(logger)
    , Profiler_(profiler)
{
    LOG_INFO("Creating throttler manager");
}

IThroughputThrottlerPtr TThrottlerManager::GetThrottler(TCellTag cellTag)
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = ThrottlerMap_.find(cellTag);
    if (it != ThrottlerMap_.end()) {
        return it->second;
    }
    LOG_INFO("Creating new throttler, CellTag: %v", cellTag);
    auto throttler = CreateLimitedThrottler(Config_, Logger, Profiler_);
    ThrottlerMap_.insert(std::make_pair(cellTag, throttler));
    return throttler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
