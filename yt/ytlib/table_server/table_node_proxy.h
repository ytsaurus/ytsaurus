#pragma once

#include "common.h"
#include "table_node.h"

#include "../cypress/node_proxy_detail.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode>
{
public:
    typedef TIntrusivePtr<TTableNodeProxy> TPtr;

    TTableNodeProxy(
        NCypress::INodeTypeHandler* typeHandler,
        NCypress::TCypressManager* cypressManager,
        const NTransaction::TTransactionId& transactionId,
        const NCypress::TNodeId& nodeId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

