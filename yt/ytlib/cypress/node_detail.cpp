#include "stdafx.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const EObjectType::EDomain TCypressScalarTypeTraits<Stroka>::ObjectType = EObjectType::StringNode;
const EObjectType::EDomain TCypressScalarTypeTraits<i64>::ObjectType = EObjectType::Int64Node;
const EObjectType::EDomain TCypressScalarTypeTraits<double>::ObjectType = EObjectType::DoubleNode;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

i32 hash<NYT::NCypress::TVersionedNodeId>::operator()(const NYT::NCypress::TVersionedNodeId& id) const
{
    return
        static_cast<i32>(THash<NYT::TGuid>()(id.ObjectId)) * 497 +
        static_cast<i32>(THash<NYT::TGuid>()(id.TransactionId));
}

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id)
    : ParentId_(NullObjectId)
    , LockMode_(ELockMode::None)
    , Id(id)
{ }

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id, const TCypressNodeBase& other)
    : LockIds_(other.LockIds_)
    , SubtreeLockIds_(other.SubtreeLockIds_)
    , ParentId_(other.ParentId_)
    , LockMode_(other.LockMode_)
    , Id(id)
{ }

EObjectType TCypressNodeBase::GetObjectType() const
{
    return TypeFromId(Id.ObjectId);
}

TVersionedNodeId TCypressNodeBase::GetId() const
{
    return Id;
}

i32 TCypressNodeBase::RefObject()
{
    YASSERT(!Id.IsBranched());
    return TObjectBase::RefObject();
}

i32 TCypressNodeBase::UnrefObject()
{
    YASSERT(!Id.IsBranched());
    return TObjectBase::UnrefObject();
}

i32 TCypressNodeBase::GetObjectRefCounter() const
{
    return TObjectBase::GetObjectRefCounter();
}

void TCypressNodeBase::Save(TOutputStream* output) const
{
    TObjectBase::Save(output);
    ::Save(output, RefCounter);
    SaveSet(output, LockIds_);
    SaveSet(output, SubtreeLockIds_);
    ::Save(output, ParentId_);
    ::Save(output, LockMode_);
}

void TCypressNodeBase::Load(TInputStream* input)
{
    TObjectBase::Load(input);
    ::Load(input, RefCounter);
    LoadSet(input, LockIds_);
    LoadSet(input, SubtreeLockIds_);
    ::Load(input, ParentId_);
    ::Load(input, LockMode_);
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

TMapNode::TMapNode(const TVersionedNodeId& id, const TMapNode& other)
    : TCypressNodeBase(id, other)
{
    KeyToChild_ = other.KeyToChild_;
    ChildToKey_ = other.ChildToKey_;
}

TAutoPtr<ICypressNode> TMapNode::Clone() const
{
    return new TMapNode(Id, *this);
}

void TMapNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    SaveMap(output, ChildToKey());
}

void TMapNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    LoadMap(input, ChildToKey());
    FOREACH(const auto& pair, ChildToKey()) {
        KeyToChild().insert(MakePair(pair.second, pair.first));
    }
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeTypeHandler::TMapNodeTypeHandler(TCypressManager* cypressManager)
    : TCypressNodeTypeHandlerBase<TMapNode>(cypressManager)
{ }

EObjectType TMapNodeTypeHandler::GetObjectType()
{
    return EObjectType::MapNode;
}

ENodeType TMapNodeTypeHandler::GetNodeType()
{
    return ENodeType::Map;
}

void TMapNodeTypeHandler::DoDestroy(TMapNode& node)
{
    // Drop references to the children.
    FOREACH (const auto& pair, node.KeyToChild()) {
        CypressManager->GetObjectManager()->UnrefObject(pair.second);
    }
}

void TMapNodeTypeHandler::DoBranch(
    const TMapNode& originatingNode,
    TMapNode& branchedNode)
{
    UNUSED(branchedNode);

    // Reference all children.
    FOREACH (const auto& pair, originatingNode.KeyToChild()) {
        CypressManager->GetObjectManager()->RefObject(pair.second);
    }
}

void TMapNodeTypeHandler::DoMerge(
    TMapNode& originatingNode,
    TMapNode& branchedNode)
{
    // Drop all references held by the originator.
    FOREACH (const auto& pair, originatingNode.KeyToChild()) {
        CypressManager->GetObjectManager()->UnrefObject(pair.second);
    }

    // Replace the child list with the branched copy.
    originatingNode.KeyToChild().swap(branchedNode.KeyToChild());
    originatingNode.ChildToKey().swap(branchedNode.ChildToKey());
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

TListNode::TListNode(const TVersionedNodeId& id, const TListNode& other)
    : TCypressNodeBase(id, other)
{
    IndexToChild_ = other.IndexToChild_;
    ChildToIndex_ = other.ChildToIndex_;
}

TAutoPtr<ICypressNode> TListNode::Clone() const
{
    return new TListNode(Id, *this);
}

ICypressNodeProxy::TPtr TMapNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TMapNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().ObjectId);
}

void TListNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, IndexToChild());
}

void TListNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, IndexToChild());
    for (int i = 0; i < IndexToChild().ysize(); ++i) {
        ChildToIndex()[IndexToChild()[i]] = i;
    }
}

////////////////////////////////////////////////////////////////////////////////

TListNodeTypeHandler::TListNodeTypeHandler(TCypressManager* cypressManager)
    : TCypressNodeTypeHandlerBase<TListNode>(cypressManager)
{ }

EObjectType TListNodeTypeHandler::GetObjectType()
{
    return EObjectType::ListNode;
}

ENodeType TListNodeTypeHandler::GetNodeType()
{
    return ENodeType::List;
}

ICypressNodeProxy::TPtr TListNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TListNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().ObjectId);
}

void TListNodeTypeHandler::DoDestroy(TListNode& node)
{
    // Drop references to the children.
    FOREACH (auto& nodeId, node.IndexToChild()) {
        CypressManager->GetObjectManager()->UnrefObject(nodeId);
    }
}

void TListNodeTypeHandler::DoBranch(
    const TListNode& originatingNode,
    TListNode& branchedNode)
{
    UNUSED(branchedNode);

    // Reference all children.
    FOREACH (const auto& nodeId, originatingNode.IndexToChild()) {
        CypressManager->GetObjectManager()->RefObject(nodeId);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode& originatingNode,
    TListNode& branchedNode)
{
    // Drop all references held by the originator.
    FOREACH (const auto& nodeId, originatingNode.IndexToChild()) {
        CypressManager->GetObjectManager()->UnrefObject(nodeId);
    }

    // Replace the child list with the branched copy.
    originatingNode.IndexToChild().swap(branchedNode.IndexToChild());
    originatingNode.ChildToIndex().swap(branchedNode.ChildToIndex());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

