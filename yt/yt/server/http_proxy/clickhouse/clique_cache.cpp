#include "clique_cache.h"

#include "config.h"

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TCachedDiscovery::TCachedDiscovery(
    TString key,
    TString cliqueId,
    TDiscoveryConfigPtr config,
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
    , TAsyncCacheValueBase(key)
    , CliqueId_(std::move(cliqueId))
{ }

////////////////////////////////////////////////////////////////////////////////

TCliqueCache::TCliqueCache(TCliqueCacheConfigPtr config, const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase(config->CacheBase, profiler)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
