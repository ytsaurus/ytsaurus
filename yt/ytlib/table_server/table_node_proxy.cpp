#include "stdafx.h"
#include "table_node_proxy.h"

namespace NYT {
namespace NTableServer {

using namespace NCypress;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TTableNode>(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

