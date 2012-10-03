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
    TYPathServiceProducer producer);

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    NYTree::IYPathServicePtr service);

NYTree::IYPathServicePtr CreateLeaderValidatorWrapper(
    NCellMaster::TBootstrap* bootstrap,
    NYTree::IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
