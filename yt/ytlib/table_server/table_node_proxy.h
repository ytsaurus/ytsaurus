#pragma once

#include "common.h"
#include "table_node.h"

#include "../cypress/node_proxy_detail.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode>
{
public:
    typedef TIntrusivePtr<TFileNodeProxy> TPtr;

    TFileNodeProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

