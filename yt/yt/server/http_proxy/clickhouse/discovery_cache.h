#pragma once

#include "private.h"

#include <yt/yt/library/clickhouse_discovery/discovery_v1.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TCachedDiscovery
    : public NClickHouseServer::TDiscovery
    , public TAsyncCacheValueBase<NScheduler::TOperationId, TCachedDiscovery>
{
public:
    TCachedDiscovery(
        NScheduler::TOperationId operationId,
        NClickHouseServer::TDiscoveryV1ConfigPtr config,
        NApi::IClientPtr client,
        IInvokerPtr invoker,
        std::vector<TString> extraAttributes,
        const NLogging::TLogger& logger);
};

DEFINE_REFCOUNTED_TYPE(TCachedDiscovery)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryCache
    : public TAsyncSlruCacheBase<NScheduler::TOperationId, TCachedDiscovery>
{
public:
    TDiscoveryCache(TDiscoveryCacheConfigPtr config, const NProfiling::TProfiler& profiler = {});
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
