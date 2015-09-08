#pragma once

#include "public.h"

#include <server/cypress_server/public.h>

#include <server/object_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateClusterNodeNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateClusterNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);
NCypressServer::INodeTypeHandlerPtr CreateRackMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
