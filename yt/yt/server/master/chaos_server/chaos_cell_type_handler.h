#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateChaosCellTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
