#pragma once
#include "public.h"

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTabletClient::ITableMountCachePtr CreateRpcProxyTableMountCache(
    NTabletClient::TTableMountCacheConfigPtr config,
    NRpc::IChannelPtr channel,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
