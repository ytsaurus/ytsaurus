#pragma once

#include <ytlib/cypress/type_handler.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandler::TPtr CreateLockMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
