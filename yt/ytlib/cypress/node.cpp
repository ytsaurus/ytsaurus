#include "stdafx.h"
#include "node.h"
#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TBranchedNodeId::TBranchedNodeId()
{ }

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
    , AttributesId_(NullNodeId)
    , State_(ENodeState::Uncommitted)
    , Id(id)
    , RefCounter(0)
{ }

TCypressNodeBase::TCypressNodeBase(const TBranchedNodeId& id, const TCypressNodeBase& other)
    : ParentId_(other.ParentId_)
    , AttributesId_(other.AttributesId_)
    , State_(other.State_)
    , Id(id)
    , RefCounter(0)
{ }

NYT::NCypress::TBranchedNodeId TCypressNodeBase::GetId() const
{
    return Id;
}

int TCypressNodeBase::Ref()
{
    YASSERT(State_ == ENodeState::Committed || State_ == ENodeState::Uncommitted);
    return ++RefCounter;
}

int TCypressNodeBase::Unref()
{
    YASSERT(State_ == ENodeState::Committed || State_ == ENodeState::Uncommitted);
    return --RefCounter;
}

void TCypressNodeBase::Destroy(TIntrusivePtr<TCypressManager> cypressManager)
{
    UNUSED(cypressManager);
    YASSERT(Id.NodeId != RootNodeId);
    YASSERT(State_ == ENodeState::Committed || State_ == ENodeState::Uncommitted);
}

void TCypressNodeBase::DoBranch(
    TIntrusivePtr<TCypressManager> cypressManager,
    ICypressNode& branchedNode,
    const TTransactionId& transactionId) const
{
    UNUSED(branchedNode);
    UNUSED(transactionId);
    YASSERT(State_ == ENodeState::Committed);

    // Add a reference to the attributes, if any.
    if (AttributesId_ != NullNodeId) {
        auto& attrImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(AttributesId_, NullTransactionId));
        cypressManager->RefNode(attrImpl);
    }
}

void TCypressNodeBase::Merge(
    TIntrusivePtr<TCypressManager> cypressManager,
    ICypressNode& branchedNode)
{
   YASSERT(State_ == ENodeState::Committed);

    // Drop the reference to attributes, if any.
    if (AttributesId_ != NullNodeId) {
        auto& attrImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(AttributesId_, NullTransactionId));
        cypressManager->UnrefNode(attrImpl);
    }

    // Replace the attributes with the branched copy.
    AttributesId_ = branchedNode.GetAttributesId();
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
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    return ~New<TMapNodeProxy>(cypressManager, transactionId, Id.NodeId);
}

TAutoPtr<ICypressNode> TMapNode::Branch(
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    TAutoPtr<ICypressNode> branchedNode = new TThis(
        TBranchedNodeId(Id.NodeId, transactionId),
        *this);

    TCypressNodeBase::DoBranch(cypressManager, *branchedNode, transactionId);

    // Reference all children.
    FOREACH (const auto& pair, NameToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(pair.Second(), NullTransactionId));
        cypressManager->RefNode(childImpl);
    }

    return branchedNode;
}

void TMapNode::Merge(
    TIntrusivePtr<TCypressManager> cypressManager,
    ICypressNode& branchedNode)
{
    TCypressNodeBase::Merge(cypressManager, branchedNode);

    // Drop all references held by the originator.
    FOREACH (const auto& pair, NameToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(pair.Second(), NullTransactionId));
        cypressManager->UnrefNode(childImpl);
    }

    // Replace the child list with the branched copy.
    auto& typedBranchedNode = dynamic_cast<TThis&>(branchedNode);
    NameToChild_.swap(typedBranchedNode.NameToChild_);
    ChildToName_.swap(typedBranchedNode.ChildToName_);
}

TAutoPtr<ICypressNode> TMapNode::Clone() const
{
    return new TThis(Id, *this);
}

void TMapNode::Destroy(TIntrusivePtr<TCypressManager> cypressManager)
{
    TCypressNodeBase::Destroy(cypressManager);

    // Drop references to the children.
    FOREACH (const auto& pair, NameToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(pair.Second(), NullTransactionId));
        cypressManager->UnrefNode(childImpl);
    }
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
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    return ~New<TListNodeProxy>(cypressManager, transactionId, Id.NodeId);
}

TAutoPtr<ICypressNode> TListNode::Branch(
    TIntrusivePtr<TCypressManager> cypressManager,
    const TTransactionId& transactionId) const
{
    TAutoPtr<ICypressNode> branchedNode = new TThis(
        TBranchedNodeId(Id.NodeId, transactionId),
        *this);

    TCypressNodeBase::DoBranch(cypressManager, *branchedNode, transactionId);

    // Reference all children.
    FOREACH (const auto& nodeId, IndexToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        cypressManager->RefNode(childImpl);
    }

    return branchedNode;
}

void TListNode::Merge(
    TIntrusivePtr<TCypressManager> cypressManager,
    ICypressNode& branchedNode)
{
    TCypressNodeBase::Merge(cypressManager, branchedNode);

    // Drop all references held by the originator.
    FOREACH (const auto& nodeId, IndexToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        cypressManager->UnrefNode(childImpl);
    }

    // Replace the child list with the branched copy.
    auto& typedBranchedNode = dynamic_cast<TThis&>(branchedNode);
    IndexToChild_.swap(typedBranchedNode.IndexToChild_);
    ChildToIndex_.swap(typedBranchedNode.ChildToIndex_);
}

TAutoPtr<ICypressNode> TListNode::Clone() const
{
    return new TThis(Id, *this);
}

void TListNode::Destroy(TIntrusivePtr<TCypressManager> cypressManager)
{
    TCypressNodeBase::Destroy(cypressManager);

    // Drop references to the children.
    FOREACH (auto& nodeId, IndexToChild_) {
        auto& childImpl = cypressManager->GetNodeForUpdate(TBranchedNodeId(nodeId, NullTransactionId));
        cypressManager->UnrefNode(childImpl);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

