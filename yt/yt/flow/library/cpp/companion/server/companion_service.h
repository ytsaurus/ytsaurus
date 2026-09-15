#pragma once

#include "public.h"

#include "pipeline.h"
#include "server_context.h"

#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Creates CompanionService; |registry| defaults to the process-wide registry.
NRpc::IServicePtr CreateCompanionService(
    TPipeline pipeline,
    TCompanionServerContextPtr context,
    NProfiling::TSolomonRegistryPtr registry = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
