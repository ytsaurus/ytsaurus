#include "node.h"
#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxy::TPtr TMapNode::GetProxy(
    TIntrusivePtr<TCypressManager> state,
    const TTransactionId& transactionId) const
{
    return ~New<TMapNodeProxy>(state, transactionId, Id.NodeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

