#pragma once

#include <yt/yt/core/rpc/service.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateShuffleService(
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    TString localServerAddress);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
