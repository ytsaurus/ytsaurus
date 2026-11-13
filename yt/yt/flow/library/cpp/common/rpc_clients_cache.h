#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/cache/cache.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Creates cached RPC clients with automatic job-proxy routing when configured.
NClient::NCache::IClientsCachePtr CreateClientsCache(
    const NClient::NCache::TClientsCacheConfigPtr& config,
    const NApi::TClientOptions& clientOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
