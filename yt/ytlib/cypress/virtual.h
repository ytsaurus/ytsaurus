#pragma once

#include "common.h"
#include "cypress_manager.h"
#include "node.h"

#include "../ytree/ypath.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TCreateServiceParam
{
    const ICypressNode* Node;
    TTransactionId TransactionId;
};

// TODO: do we really need this level of customization?
typedef IParamFunc<
        const TCreateServiceParam&,
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
