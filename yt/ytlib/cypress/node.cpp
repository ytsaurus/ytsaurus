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
    , RefCounter(0)
    , Id(id)
{ }

TCypressNodeBase::TCypressNodeBase(const TBranchedNodeId& id, const TCypressNodeBase& other)
    : ParentId_(other.ParentId_)
    , State_(other.State_)
    , RefCounter(0)
    , Id(id)
{ }

NYT::NCypress::TBranchedNodeId TCypressNodeBase::GetId() const
{
    return Id;
}

void TCypressNodeBase::Ref()
{
    ++RefCounter;
}

void TCypressNodeBase::Unref()
{
    --RefCounter;
}

int TCypressNodeBase::GetRefCounter() const
{
    return RefCounter;
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
{ }

TMapNode::TMapNode(const TBranchedNodeId& id, const TMapNode& other)
    : TCypressNodeBase(id, other)
{
    NameToChild_ = other.NameToChild_;
    ChildToName_ = other.ChildToName_;
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
    NameToChild_.swap(typedBranchedNode.NameToChild_);
    ChildToName_.swap(typedBranchedNode.ChildToName_);
}

TAutoPtr<ICypressNode> TMapNode::Clone() const
{
    return new TThis(Id, *this);
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TBranchedNodeId& id)
    : TCypressNodeBase(id)
{ }

TListNode::TListNode(const TBranchedNodeId& id, const TListNode& other)
    : TCypressNodeBase(id, other)
{
    IndexToChild_ = other.IndexToChild_;
    ChildToIndex_ = other.ChildToIndex_;
}

ICypressNodeProxy::TPtr TListNode::GetProxy(
    TIntrusivePtr<TCypressManager> state,
    const TTransactionId& transactionId) const
{
    return ~New<TListNodeProxy>(state, transactionId, Id.NodeId);
}

TAutoPtr<ICypressNode> TListNode::Branch(const TTransactionId& transactionId) const
{
    YASSERT(!Id.IsBranched());
    return new TThis(
        TBranchedNodeId(Id.NodeId, transactionId),
        *this);
}

void TListNode::Merge(ICypressNode& branchedNode)
{
    auto& typedBranchedNode = dynamic_cast<TThis&>(branchedNode);
    IndexToChild_.swap(typedBranchedNode.IndexToChild_);
    ChildToIndex_.swap(typedBranchedNode.ChildToIndex_);
}

TAutoPtr<ICypressNode> TListNode::Clone() const
{
    return new TThis(Id, *this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

