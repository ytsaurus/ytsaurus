#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateChunkMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType type);

NCypressServer::INodeTypeHandlerPtr CreateChunkViewMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateChunkListMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateMediumMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
