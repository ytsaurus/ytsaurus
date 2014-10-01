#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateMasterCacheService(
    TMasterCacheServiceConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NRpc::TRealmId& masterCellGuid);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
