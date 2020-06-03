#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/logging/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr masterChannel,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
