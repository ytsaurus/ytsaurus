#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateControllerAgentService(NCellScheduler::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
