#include "discovery_cache.h"

#include "config.h"

namespace NYT::NHttpProxy::NClickHouse {

using namespace NClickHouseServer;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TCachedDiscovery::TCachedDiscovery(
    TOperationId operationId,
    NClickHouseServer::IDiscoveryPtr discovery)
    : TAsyncCacheValueBase(std::move(operationId))
    , Value_(std::move(discovery))
{ }

////////////////////////////////////////////////////////////////////////////////

TDiscoveryCache::TDiscoveryCache(TDiscoveryCacheConfigPtr config, const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase(config->CacheBase, profiler)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
