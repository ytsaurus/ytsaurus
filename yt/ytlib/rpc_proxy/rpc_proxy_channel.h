#pragma once

#include "public.h"

#include <yt/ytlib/api/connection.h>

#include <yt/core/rpc/channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateRpcProxyChannel(TRpcProxyConnectionPtr connection, const NApi::TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
