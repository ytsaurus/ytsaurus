#pragma once

#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

NCypressClient::INodeTypeHandlerPtr CreateNodeMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
