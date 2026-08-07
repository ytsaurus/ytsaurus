#pragma once

#include "public.h"

#include "pipeline.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Creates the companion-side implementation of the CompanionService gRPC contract.
NRpc::IServicePtr CreateCompanionService(
    TPipeline pipeline,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
