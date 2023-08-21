#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
