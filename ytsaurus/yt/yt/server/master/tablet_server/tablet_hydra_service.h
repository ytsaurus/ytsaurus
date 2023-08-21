#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletHydraService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
