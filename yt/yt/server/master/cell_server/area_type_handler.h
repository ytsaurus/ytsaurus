#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateAreaTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TArea>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
