#pragma once

#include "public.h"

#include <yt/server/hydra/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateTabletTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TTablet>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
