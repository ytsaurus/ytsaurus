#include "stdafx.h"
#include "file_node_proxy.h"

namespace NYT {
namespace NFileServer {

using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

NYT::NYTree::ENodeType TFileNodeProxy::GetType() const
{
    return ENodeType::Entity;
}

Stroka TFileNodeProxy::GetTypeName() const
{
    return FileTypeName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

