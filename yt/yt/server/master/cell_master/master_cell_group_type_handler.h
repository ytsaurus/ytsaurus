#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateMasterCellGroupTypeHandler(
    TBootstrap* bootstrap,
    NHydra::TEntityMap<TMasterCellGroup>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
