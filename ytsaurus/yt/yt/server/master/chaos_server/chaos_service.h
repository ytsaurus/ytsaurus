#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateMasterChaosService(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
