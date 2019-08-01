#include "clique_cache.h"
#include "config.h"

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

TCachedDiscovery::TCachedDiscovery(
    TString key,
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
{ }

////////////////////////////////////////////////////////////////////////////////

TCliqueCache::TCliqueCache(TCliqueCacheConfigPtr config)
    // TODO(dakovalkov): Set up the profiler.
    : TAsyncSlruCacheBase(config->CacheBase)
{ }

i64 TCliqueCache::GetWeight(const TCachedDiscoveryPtr& discovery) const
{
    return discovery->GetWeight();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
