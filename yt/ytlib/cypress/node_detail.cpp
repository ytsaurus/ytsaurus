#include "stdafx.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include "../ytree/fluent.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const ERuntimeNodeType::EDomain TCypressScalarTypeTraits<Stroka>::RuntimeType = ERuntimeNodeType::String;
const char* TCypressScalarTypeTraits<Stroka>::TypeName = "string";

const ERuntimeNodeType::EDomain TCypressScalarTypeTraits<i64>::RuntimeType = ERuntimeNodeType::Int64;
const char* TCypressScalarTypeTraits<i64>::TypeName = "int64";

const ERuntimeNodeType::EDomain TCypressScalarTypeTraits<double>::RuntimeType = ERuntimeNodeType::Double;
const char* TCypressScalarTypeTraits<double>::TypeName = "double";

}

////////////////////////////////////////////////////////////////////////////////

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

TCypressNodeBase::TCypressNodeBase(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : ParentId_(NullNodeId)
    , AttributesId_(NullNodeId)
    , State_(ENodeState::Uncommitted)
    , Id(id)
    , RuntimeType(runtimeType)
    , RefCounter(0)
{ }

TCypressNodeBase::TCypressNodeBase(const TBranchedNodeId& id, const TCypressNodeBase& other)
    : ParentId_(other.ParentId_)
    , AttributesId_(other.AttributesId_)
    , State_(other.State_)
    , Id(id)
    , RuntimeType(other.RuntimeType)
    , RefCounter(0)
{ }

ERuntimeNodeType TCypressNodeBase::GetRuntimeType() const
{
    return RuntimeType;
}

TBranchedNodeId TCypressNodeBase::GetId() const
{
    return Id;
}

i32 TCypressNodeBase::Ref()
{
    YASSERT(State_ != ENodeState::Branched);
    return ++RefCounter;
}

i32 TCypressNodeBase::Unref()
{
    YASSERT(State_ != ENodeState::Branched);
    return --RefCounter;
}

i32 TCypressNodeBase::GetRefCounter() const
{
    return RefCounter;
}

void TCypressNodeBase::Save(TOutputStream* output) const
{
    ::Save(output, RefCounter);
    SaveSet(output, LockIds_);
    ::Save(output, ParentId_);
    ::Save(output, AttributesId_);
    ::Save(output, State_);
}

void TCypressNodeBase::Load(TInputStream* input)
{
    ::Load(input, RefCounter);
    ::Load(input, LockIds_);
    ::Load(input, ParentId_);
    ::Load(input, AttributesId_);
    ::Load(input, State_);
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : TCypressNodeBase(id, runtimeType)
{ }

TMapNode::TMapNode(const TBranchedNodeId& id, const TMapNode& other)
    : TCypressNodeBase(id, other)
{
    NameToChild_ = other.NameToChild_;
    ChildToName_ = other.ChildToName_;
}

TAutoPtr<ICypressNode> TMapNode::Clone() const
{
    return new TMapNode(Id, *this);
}

void TMapNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    SaveMap(output, ChildToName());
}

void TMapNode::Load(TInputStream* input)
{
    TCypressNodeBase::Load(input);
    ::Load(input, ChildToName());
    FOREACH(const auto& pair, ChildToName()) {
        NameToChild().insert(MakePair(pair.Second(), pair.First()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeTypeHandler::TMapNodeTypeHandler(TCypressManager* cypressManager)
    : TCypressNodeTypeHandlerBase<TMapNode>(cypressManager)
{
    RegisterGetter("size", FromMethod(&TThis::GetSize));
}

ERuntimeNodeType TMapNodeTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::Map;
}

ENodeType TMapNodeTypeHandler::GetNodeType()
{
    return ENodeType::Map;
}

Stroka TMapNodeTypeHandler::GetTypeName()
{
    return "map";
}

void TMapNodeTypeHandler::DoDestroy(TMapNode& node)
{
    // Drop references to the children.
    FOREACH (const auto& pair, node.NameToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            pair.Second(),
            NullTransactionId));
        CypressManager->UnrefNode(childImpl);
    }
}

void TMapNodeTypeHandler::DoBranch(
    const TMapNode& committedNode,
    TMapNode& branchedNode)
{
    UNUSED(branchedNode);

    // Reference all children.
    FOREACH (const auto& pair, committedNode.NameToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            pair.Second(),
            NullTransactionId));
        CypressManager->RefNode(childImpl);
    }
}

void TMapNodeTypeHandler::DoMerge(
    TMapNode& committedNode,
    TMapNode& branchedNode )
{
    // Drop all references held by the originator.
    FOREACH (const auto& pair, committedNode.NameToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            pair.Second(),
            NullTransactionId));
        CypressManager->UnrefNode(childImpl);
    }

    // Replace the child list with the branched copy.
    committedNode.NameToChild().swap(branchedNode.NameToChild());
    committedNode.ChildToName().swap(branchedNode.ChildToName());
}

void TMapNodeTypeHandler::GetSize(const TGetAttributeParam& param)
{
    BuildYsonFluently(param.Consumer)
        .Scalar(param.Node->NameToChild().ysize());
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TBranchedNodeId& id, ERuntimeNodeType runtimeType)
    : TCypressNodeBase(id, runtimeType)
{ }

TListNode::TListNode(const TBranchedNodeId& id, const TListNode& other)
    : TCypressNodeBase(id, other)
{
    IndexToChild_ = other.IndexToChild_;
    ChildToIndex_ = other.ChildToIndex_;
}

TAutoPtr<ICypressNode> TListNode::Clone() const
{
    return new TListNode(Id, *this);
}

TIntrusivePtr<ICypressNodeProxy> TMapNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TMapNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().NodeId);
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
{
    RegisterGetter("size", FromMethod(&TThis::GetSize));
}

ERuntimeNodeType TListNodeTypeHandler::GetRuntimeType()
{
    return ERuntimeNodeType::List;
}

ENodeType TListNodeTypeHandler::GetNodeType()
{
    return ENodeType::List;
}

Stroka TListNodeTypeHandler::GetTypeName()
{
    return "list";
}

TIntrusivePtr<ICypressNodeProxy> TListNodeTypeHandler::GetProxy(
    const ICypressNode& node,
    const TTransactionId& transactionId)
{
    return New<TListNodeProxy>(
        this,
        ~CypressManager,
        transactionId,
        node.GetId().NodeId);
}

void TListNodeTypeHandler::DoDestroy(TListNode& node)
{
    // Drop references to the children.
    FOREACH (auto& nodeId, node.IndexToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            nodeId,
            NullTransactionId));
        CypressManager->UnrefNode(childImpl);
    }
}

void TListNodeTypeHandler::DoBranch(
    const TListNode& committedNode,
    TListNode& branchedNode)
{
    UNUSED(branchedNode);

    // Reference all children.
    FOREACH (const auto& nodeId, committedNode.IndexToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            nodeId,
            NullTransactionId));
        CypressManager->RefNode(childImpl);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode& committedNode,
    TListNode& branchedNode )
{
    // Drop all references held by the originator.
    FOREACH (const auto& nodeId, committedNode.IndexToChild()) {
        auto& childImpl = CypressManager->GetNodeForUpdate(TBranchedNodeId(
            nodeId,
            NullTransactionId));
        CypressManager->UnrefNode(childImpl);
    }

    // Replace the child list with the branched copy.
    committedNode.IndexToChild().swap(branchedNode.IndexToChild());
    committedNode.ChildToIndex().swap(branchedNode.ChildToIndex());
}

void TListNodeTypeHandler::GetSize(const TGetAttributeParam& param)
{
    BuildYsonFluently(param.Consumer)
        .Scalar(param.Node->IndexToChild().ysize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

