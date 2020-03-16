#pragma once

#include "private.h"

#include <yt/client/misc/discovery.h>

#include <yt/core/misc/async_cache.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TCachedDiscovery
    : public TDiscovery
    , public TAsyncCacheValueBase<TString, TCachedDiscovery>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, CliqueId);

public:
    TCachedDiscovery(
        TString key,
        TString cliqueId,
        TDiscoveryConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        const NLogging::TLogger& logger);
};

DEFINE_REFCOUNTED_TYPE(TCachedDiscovery)

////////////////////////////////////////////////////////////////////////////////

class TCliqueCache
    : public TAsyncSlruCacheBase<TString, TCachedDiscovery>
{
public:
    TCliqueCache(TCliqueCacheConfigPtr config, const NProfiling::TProfiler& profiler = NProfiling::TProfiler());
};

DEFINE_REFCOUNTED_TYPE(TCliqueCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
