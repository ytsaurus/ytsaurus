#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/object_server/public.h>

namespace NYT {
namespace NTabletServer {

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

} // namespace NTabletServer
} // namespace NYT
