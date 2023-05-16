#pragma once

#include "private.h"

#include <yt/yt/library/clickhouse_discovery/public.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TCachedDiscovery
    : public TAsyncCacheValueBase<NScheduler::TOperationId, TCachedDiscovery>
{
public:
    TCachedDiscovery(
        NScheduler::TOperationId operationId,
        NClickHouseServer::IDiscoveryPtr discovery);

    DEFINE_BYREF_RO_PROPERTY(NClickHouseServer::IDiscoveryPtr, Value);
};

DEFINE_REFCOUNTED_TYPE(TCachedDiscovery)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryCache
    : public TAsyncSlruCacheBase<NScheduler::TOperationId, TCachedDiscovery>
{
public:
    TDiscoveryCache(TDiscoveryCacheConfigPtr config, const NProfiling::TProfiler& profiler = {});
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
