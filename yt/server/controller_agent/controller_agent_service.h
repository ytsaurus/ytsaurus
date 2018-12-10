#pragma once

#include "public.h"

#include <yt/server/scheduler/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateControllerAgentService(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
