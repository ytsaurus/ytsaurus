#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateControllerAgentTrackerService(NCellScheduler::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

