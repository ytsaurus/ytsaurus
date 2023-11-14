#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateReplicatedTableTrackerService(
    NCellMaster::TBootstrap* bootstrap,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
