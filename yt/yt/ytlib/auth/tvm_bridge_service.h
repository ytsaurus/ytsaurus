#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Creates a TVM bridge service, which relays TVM tickets from #tvmService to callers.
NRpc::IServicePtr CreateTvmBridgeService(
    IDynamicTvmServicePtr tvmService,
    IInvokerPtr invoker,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
