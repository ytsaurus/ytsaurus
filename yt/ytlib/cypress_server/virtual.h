#pragma once

#include "type_handler.h"

#include <ytlib/object_server/public.h>
#include <ytlib/cypress_server/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

typedef
    TCallback< NYTree::IYPathServicePtr(const TNodeId&) >
    TYPathServiceProducer;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::EObjectType objectType,
    TYPathServiceProducer producer,
    bool requireLeaderStatus = false);

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::EObjectType objectType,
    NYTree::IYPathServicePtr service,
    bool requireLeaderStatus = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
