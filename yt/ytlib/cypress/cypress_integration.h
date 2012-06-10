#pragma once

#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandlerPtr CreateNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

NCypress::INodeTypeHandlerPtr CreateLockMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
