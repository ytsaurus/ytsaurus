#include "discovery_cache.h"

#include "config.h"

namespace NYT::NHttpProxy::NClickHouse {

using namespace NClickHouseServer;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TCachedDiscovery::TCachedDiscovery(
    TOperationId operationId,
    TDiscoveryV1ConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    const NLogging::TLogger& logger)
    : TDiscovery(
        config,
        client,
        invoker,
        extraAttributes,
        logger)
    , TAsyncCacheValueBase(operationId)
{ }

////////////////////////////////////////////////////////////////////////////////

TDiscoveryCache::TDiscoveryCache(TDiscoveryCacheConfigPtr config, const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase(config->CacheBase, profiler)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
