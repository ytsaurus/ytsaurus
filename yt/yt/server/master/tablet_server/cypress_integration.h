#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTabletMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateTabletActionMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
