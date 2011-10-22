#include "stdafx.h"
#include "file_node_proxy.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    TCypressManager::TPtr cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        cypressManager,
        transactionId,
        nodeId)
{ }

NYT::NYTree::ENodeType TFileNodeProxy::GetType() const
{
    return ENodeType::Entity;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

