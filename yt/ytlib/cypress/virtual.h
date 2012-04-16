#pragma once

#include "type_handler.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

typedef
    TCallback< NYTree::IYPathServicePtr(const TNodeId&) >
    TYPathServiceProducer;

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    EObjectType objectType,
    TYPathServiceProducer producer);

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    EObjectType objectType,
    NYTree::IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
