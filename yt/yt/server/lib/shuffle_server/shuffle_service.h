#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateShuffleService(
    IInvokerPtr invoker,
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger,
    TString localServerAddress);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
