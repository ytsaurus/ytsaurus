#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreatePortalEntranceMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreatePortalExitMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
