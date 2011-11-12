#pragma once

#include "common.h"
#include "cypress_manager.h"
#include "node.h"
#include "node_proxy.h"

#include "../ytree/ypath_service.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TVirtualYPathContext
{
    TNodeId NodeId;
    TTransactionId TransactionId;
    Stroka Manifest;
    ICypressNodeProxy::TPtr Fallback; 
};

typedef
    IParamFunc<
        const TVirtualYPathContext&,
        NYTree::IYPathService::TPtr
    >
    TYPathServiceBuilder;

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    TYPathServiceBuilder* serviceBuilder);

INodeTypeHandler::TPtr CreateVirtualTypeHandler(
    TCypressManager* cypressManager,
    ERuntimeNodeType runtypeType,
    const Stroka& typeName,
    NYTree::IYPathService* service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
