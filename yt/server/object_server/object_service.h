#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateObjectService(
    TObjectServiceConfigPtr config,
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
