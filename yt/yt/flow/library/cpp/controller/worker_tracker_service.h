#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateWorkerTrackerService(
    IControllerPtr controller,
    IWorkerTrackerPtr workerTracker,
    IInvokerPtr invoker,
    NRpc::IChannelFactoryPtr channelFactory,
    IPipelineAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
