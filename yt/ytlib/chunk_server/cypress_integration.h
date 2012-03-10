#pragma once

#include "holder_authority.h"

#include <ytlib/cypress/type_handler.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateLostChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateOverreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateUnderreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

IHolderAuthority::TPtr CreateHolderAuthority(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateHolderTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateHolderMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
