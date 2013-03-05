#pragma once

#include <server/cypress_server/public.h>
#include <server/cell_master/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTransactionMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
