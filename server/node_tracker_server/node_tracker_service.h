#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateNodeTrackerService(
    TNodeTrackerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
