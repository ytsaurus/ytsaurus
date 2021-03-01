#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletNodeTrackerService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
