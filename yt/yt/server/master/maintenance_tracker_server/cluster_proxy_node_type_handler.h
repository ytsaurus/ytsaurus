#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateClusterProxyNodeTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTY::NCypressServer
