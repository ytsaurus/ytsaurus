#pragma once

#include "public.h"

#include <ytlib/cypress_server/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateLostChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateOverreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateUnderreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateChunkListMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

IHolderAuthorityPtr CreateHolderAuthority(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateHolderTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateHolderMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
