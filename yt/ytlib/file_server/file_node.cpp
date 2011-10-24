#include "stdafx.h"
#include "file_node.h"
#include "file_node_proxy.h"

#include "../cypress/node_proxy.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

TFileNode::TFileNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
{ }

TFileNode::TFileNode(const TBranchedNodeId& id, const TFileNode& other)
    : TCypressNodeBase(id, other)
{ }

ICypressNodeProxy::TPtr TFileNode::GetProxy(
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    return ~New<TFileNodeProxy>(cypressManager, transactionId, Id.NodeId);
}

TAutoPtr<ICypressNode> TFileNode::Branch(
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    TAutoPtr<ICypressNode> branchedNode = new TThis(
        TBranchedNodeId(Id.NodeId, transactionId),
        *this);

    TCypressNodeBase::DoBranch(cypressManager, *branchedNode, transactionId);

    return branchedNode;
}

void TFileNode::Merge(
    TIntrusivePtr<TCypressManager> cypressManager,
    ICypressNode& branchedNode)
{
    TCypressNodeBase::Merge(cypressManager, branchedNode);
}

TAutoPtr<ICypressNode> TFileNode::Clone() const
{
    return new TThis(Id, *this);
}

void TFileNode::Destroy(TIntrusivePtr<TCypressManager> cypressManager)
{
    TCypressNodeBase::Destroy(cypressManager);

    // TODO:
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

