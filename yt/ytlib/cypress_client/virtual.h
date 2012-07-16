#pragma once

#include "type_handler.h"

#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

typedef
    TCallback< NYTree::IYPathServicePtr(const TNodeId&) >
    TYPathServiceProducer;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer);

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    EObjectType objectType,
    NYTree::IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
