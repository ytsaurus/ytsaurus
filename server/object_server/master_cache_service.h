#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateMasterCacheService(
    TMasterCacheServiceConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NRpc::TRealmId& masterCellId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
