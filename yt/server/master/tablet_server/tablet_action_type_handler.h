#pragma once

#include "public.h"

#include <yt/server/lib/hydra/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateTabletActionTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TTabletAction>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
