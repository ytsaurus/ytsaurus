#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_cache/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateChaosCacheService(
    NChaosCache::TChaosCacheConfigPtr config,
    IInvokerPtr invoker,
    NApi::NNative::IClientPtr client,
    TChaosCachePtr cache,
    NRpc::IAuthenticatorPtr authenticator,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
