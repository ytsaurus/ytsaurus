#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateMasterCacheService(
    TMasterCacheServiceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr masterChannel,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
