#pragma once

#include "private.h"

#include <yt/ytlib/api/connection.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateDiscoveringChannel(
    TConnectionPtr connection,
    const NApi::TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
