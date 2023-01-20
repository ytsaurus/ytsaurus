#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////

// COMPAT(pogorelov): Remove when all nodes will be 23.1.
NRpc::IServicePtr CreateOldJobTrackerService(TBootstrap* bootstrap);

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
