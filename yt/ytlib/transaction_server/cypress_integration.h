#pragma once

#include <ytlib/cypress_server/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTransactionMapTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
