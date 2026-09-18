#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTokenService(
    IInvokerPtr invoker,
    ITokenManagerPtr tokenManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
