#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDistributedChunkSessionService(
    TDistributedChunkSessionServiceConfigPtr config,
    IInvokerPtr invoker,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
