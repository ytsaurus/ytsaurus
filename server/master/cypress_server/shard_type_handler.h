#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/lib/hydra/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateShardTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NHydra::TEntityMap<TCypressShard>* map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
