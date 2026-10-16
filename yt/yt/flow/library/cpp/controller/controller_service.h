#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateControllerService(
    IFlowExecutorPtr flowExecutor,
    NRpc::IAuthenticatorPtr authenticator,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
