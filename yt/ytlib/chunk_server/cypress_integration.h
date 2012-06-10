#pragma once

#include "public.h"

#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandlerPtr CreateChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateLostChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateOverreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateUnderreplicatedChunkMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateChunkListMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

IHolderAuthorityPtr CreateHolderAuthority(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateHolderTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateHolderMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
