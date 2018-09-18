#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateClusterNodeNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateClusterNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateRackMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateDataCenterMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
