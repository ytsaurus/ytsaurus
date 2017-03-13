#pragma once

#include "public.h"

#include <yt/server/hydra/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateTabletActionTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TTabletAction>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
