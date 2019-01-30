#pragma once

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/object_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTabletCellNodeTypeHandler(
    NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateTabletMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateTabletCellBundleMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateTabletActionMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
