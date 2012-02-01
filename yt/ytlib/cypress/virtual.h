#pragma once

#include "common.h"
#include "cypress_manager.h"
#include "node.h"
#include "node_proxy.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

typedef
    IParamFunc<
        const TVersionedNodeId&,
        NYTree::IYPathService::TPtr
    >
    TYPathServiceProducer;

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    EObjectType objectType,
    TYPathServiceProducer* producer);

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    EObjectType objectType,
    NYTree::IYPathService* service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
