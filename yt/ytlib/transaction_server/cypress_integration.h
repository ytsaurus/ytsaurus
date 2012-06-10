#pragma once

#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandlerPtr CreateTransactionMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
