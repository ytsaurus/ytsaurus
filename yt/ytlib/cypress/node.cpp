#include "node.h"
#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TBranchedNodeId::TBranchedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId)
    : NodeId(nodeId)
    , TransactionId(transactionId)
{ }

bool TBranchedNodeId::IsBranched() const
{
    return TransactionId != NullTransactionId;
}

Stroka TBranchedNodeId::ToString() const
{
    return Sprintf("%s:%s",
        ~NodeId.ToString(),
        ~TransactionId.ToString());
}

//! Compares TBranchedNodeId s for equality.
bool operator == (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return lhs.NodeId == rhs.NodeId &&
           lhs.TransactionId == rhs.TransactionId;
}

//! Compares TBranchedNodeId s for inequality.
bool operator != (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs)
{
    return !(lhs == rhs);
}

} // namespace NCypress
} // namespace NYT

i32 hash<NYT::NCypress::TBranchedNodeId>::operator()(const NYT::NCypress::TBranchedNodeId& id) const
{
    return static_cast<i32>(THash<NYT::TGuid>()(id.NodeId)) * 497 +
        static_cast<i32>(THash<NYT::TGuid>()(id.TransactionId));
}

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TBranchedNodeId& id)
    : ParentId_(NullNodeId)
    , State_(ENodeState::Uncommitted)
    , Id(id)
{ }

NYT::NCypress::TBranchedNodeId TCypressNodeBase::GetId() const
{
    return Id;
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
{ }

TMapNode::TMapNode(const TBranchedNodeId& id, const TMapNode& other)
    : TCypressNodeBase(id)
{
        NameToChild() = other.NameToChild();
        ChildToName() = other.ChildToName();
}

ICypressNodeProxy::TPtr TMapNode::GetProxy(
    TIntrusivePtr<TCypressManager> state,
    const TTransactionId& transactionId) const
{
    return ~New<TMapNodeProxy>(state, transactionId, Id.NodeId);
}

TAutoPtr<ICypressNode> TMapNode::Branch(const TTransactionId& transactionId) const
{
    YASSERT(!Id.IsBranched());
    return new TThis(
        TBranchedNodeId(Id.NodeId, transactionId),
        *this);
}

void TMapNode::Merge(ICypressNode& branchedNode)
{
    auto& typedBranchedNode = dynamic_cast<TThis&>(branchedNode);
    NameToChild().swap(typedBranchedNode.NameToChild());
    ChildToName().swap(typedBranchedNode.ChildToName());
}

TAutoPtr<ICypressNode> TMapNode::Clone() const
{
    return new TThis(Id, *this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

