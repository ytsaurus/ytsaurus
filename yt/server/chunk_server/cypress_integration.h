#pragma once

#include "public.h"

#include <core/ypath/public.h>

#include <server/cypress_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateChunkMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType type);

NCypressServer::INodeTypeHandlerPtr CreateChunkListMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
