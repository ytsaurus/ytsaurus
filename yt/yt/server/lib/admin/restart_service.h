#pragma once

#include <yt/yt/server/lib/misc/restart_manager.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NAdmin {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateRestartService(
    TRestartManagerPtr restartManager,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
