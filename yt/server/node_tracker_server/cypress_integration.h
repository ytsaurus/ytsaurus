#pragma once

#include "public.h"

#include <server/cypress_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

INodeAuthorityPtr CreateNodeAuthority(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateNodeTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
