#pragma once

#include "common.h"
#include "file_node.h"

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
        NCypress::INodeTypeHandler* typeHandler,
        NCypress::TCypressManager* cypressManager,
        const NTransaction::TTransactionId& transactionId,
        const NCypress::TNodeId& nodeId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

