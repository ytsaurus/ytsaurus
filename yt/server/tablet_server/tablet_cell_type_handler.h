#pragma once

#include "public.h"

#include <yt/server/hydra/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateTabletCellTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TTabletCell>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
