#pragma once

#include "type_handler.h"

#include <server/object_server/public.h>
#include <server/cypress_server/public.h>
#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

typedef
    TCallback< NYTree::IYPathServicePtr(ICypressNode*, NTransactionServer::TTransaction*) >
    TYPathServiceProducer;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    TYPathServiceProducer producer,
    bool requireLeaderStatus = false);

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    NYTree::IYPathServicePtr service,
    bool requireLeaderStatus = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
