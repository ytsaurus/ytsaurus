#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/chaos_lease_cache.h>
#include <yt/yt/client/chaos_client/config.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

IChaosLeaseCachePtr CreateNativeChaosLeaseCache(
    TChaosLeaseCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
