#pragma once

#include "public.h"

#include <yt/client/misc/discovery.h>

#include <yt/core/misc/async_cache.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TCachedDiscovery
    : public virtual TDiscovery
    , public virtual TAsyncCacheValueBase<TString, TCachedDiscovery>
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
    TCliqueCache(TCliqueCacheConfigPtr config);
private:
    virtual i64 GetWeight(const TCachedDiscoveryPtr& discovery) const override;
};

DEFINE_REFCOUNTED_TYPE(TCliqueCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
