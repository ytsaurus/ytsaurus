#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTopmostTransactionMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

NCypressServer::INodeTypeHandlerPtr CreateTransactionMapTypeHandler(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
