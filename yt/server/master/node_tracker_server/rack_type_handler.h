#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/lib/hydra/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateRackTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TRack>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
