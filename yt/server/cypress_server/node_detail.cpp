#include "stdafx.h"
#include "node_detail.h"
#include "node_proxy_detail.h"

#include <ytlib/ytree/fluent.h>
#include <server/cell_master/load_context.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const EObjectType::EDomain TCypressScalarTypeTraits<Stroka>::ObjectType = EObjectType::StringNode;
const EObjectType::EDomain TCypressScalarTypeTraits<i64>::ObjectType = EObjectType::IntegerNode;
const EObjectType::EDomain TCypressScalarTypeTraits<double>::ObjectType = EObjectType::DoubleNode;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TCypressNodeBase::TCypressNodeBase(const TVersionedNodeId& id)
    : ParentId_(NullObjectId)
    , LockMode_(ELockMode::None)
    , TrunkNode_(NULL)
    , CreationTime_(0)
    , ModificationTime_(0)
    , Id(id)
{ }

EObjectType TCypressNodeBase::GetObjectType() const
{
    return TypeFromId(Id.ObjectId);
}

const TVersionedNodeId& TCypressNodeBase::GetId() const
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
    SaveObjectRefs(output, Locks_);
    ::Save(output, ParentId_);
    ::Save(output, LockMode_);
    ::Save(output, CreationTime_);
}

void TCypressNodeBase::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);

    TObjectBase::Load(input);
    LoadObjectRefs(input, Locks_, context);
    ::Load(input, ParentId_);
    ::Load(input, LockMode_);
    ::Load(input, CreationTime_);

    TrunkNode_ = Id.IsBranched() ? context.Get<ICypressNode>(TVersionedObjectId(Id.ObjectId)) : this;
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChildCountDelta_(0)
{ }

void TMapNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, ChildCountDelta_);
    SaveMap(output, KeyToChild());
}

void TMapNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
    ::Load(input, ChildCountDelta_);
    LoadMap(input, KeyToChild());
    FOREACH (const auto& pair, KeyToChild()) {
        if (pair.second != NullObjectId) {
            ChildToKey().insert(MakePair(pair.second, pair.first));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeTypeHandler::TMapNodeTypeHandler(TBootstrap* bootstrap)
    : TCypressNodeTypeHandlerBase<TMapNode>(bootstrap)
{ }

EObjectType TMapNodeTypeHandler::GetObjectType()
{
    return EObjectType::MapNode;
}

ENodeType TMapNodeTypeHandler::GetNodeType()
{
    return ENodeType::Map;
}

void TMapNodeTypeHandler::DoDestroy(TMapNode* node)
{
    // Drop references to the children.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& pair, node->KeyToChild()) {
        if (pair.second != NullObjectId) {
            objectManager->UnrefObject(pair.second);
        }
    }
}

void TMapNodeTypeHandler::DoBranch(
    const TMapNode* originatingNode,
    TMapNode* branchedNode)
{
    UNUSED(originatingNode);
    UNUSED(branchedNode);
}

void TMapNodeTypeHandler::DoMerge(
    TMapNode* originatingNode,
    TMapNode* branchedNode)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto cypressManager = Bootstrap->GetCypressManager();
    const auto& originatingId = originatingNode->GetId();
    FOREACH (const auto& pair, branchedNode->KeyToChild()) {
        auto it = originatingNode->KeyToChild().find(pair.first);
        if (it == originatingNode->KeyToChild().end()) {
            YCHECK(originatingNode->KeyToChild().insert(pair).second);
        } else {
            if (it->second != NullObjectId) {
                objectManager->UnrefObject(it->second);
                YCHECK(originatingNode->ChildToKey().erase(it->second) == 1);
            }
            it->second = pair.second;
            
            if (pair.second == NullObjectId) {
                auto originatingTransaction =
                    originatingId.TransactionId == NullTransactionId
                    ? NULL
                    : transactionManager->GetTransaction(originatingId.TransactionId);
                auto transactions = transactionManager->GetTransactionPath(originatingTransaction);
                bool contains = false;
                FOREACH (const auto* currentTransaction, transactions) {
                    if (currentTransaction == originatingTransaction) {
                        continue;
                    }
                    const auto* node = cypressManager->GetVersionedNode(originatingId.ObjectId, currentTransaction);
                    const auto& map = static_cast<const TMapNode*>(node)->KeyToChild();
                    auto innerIt = map.find(pair.first);
                    if (innerIt != map.end()) {
                        if (innerIt->second != NullObjectId) {
                            contains = true;
                        }
                        break;
                    }
                }
                if (!contains) {
                    originatingNode->KeyToChild().erase(it);
                }
            }

        }
        if (pair.second != NullObjectId) {
            YCHECK(originatingNode->ChildToKey().insert(MakePair(pair.second, pair.first)).second);
        }
    }
    originatingNode->ChildCountDelta() += branchedNode->ChildCountDelta();
}

ICypressNodeProxyPtr TMapNodeTypeHandler::GetProxy(
    ICypressNode* trunkNode,
    TTransaction* transaction)
{
    return New<TMapNodeProxy>(
        this,
        Bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

void TListNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    ::Save(output, IndexToChild());
}

void TListNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
    ::Load(input, IndexToChild());
    for (int i = 0; i < IndexToChild().size(); ++i) {
        ChildToIndex()[IndexToChild()[i]] = i;
    }
}

////////////////////////////////////////////////////////////////////////////////

TListNodeTypeHandler::TListNodeTypeHandler(TBootstrap* bootstrap)
    : TCypressNodeTypeHandlerBase<TListNode>(bootstrap)
{ }

EObjectType TListNodeTypeHandler::GetObjectType()
{
    return EObjectType::ListNode;
}

ENodeType TListNodeTypeHandler::GetNodeType()
{
    return ENodeType::List;
}

ICypressNodeProxyPtr TListNodeTypeHandler::GetProxy(
    ICypressNode* trunkNode,
    TTransaction* transaction)
{
    return New<TListNodeProxy>(
        this,
        Bootstrap,
        transaction,
        trunkNode);
}

void TListNodeTypeHandler::DoDestroy(TListNode* node)
{
    // Drop references to the children.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, node->IndexToChild()) {
        objectManager->UnrefObject(nodeId);
    }
}

void TListNodeTypeHandler::DoBranch(
    const TListNode* originatingNode,
    TListNode* branchedNode)
{
    branchedNode->IndexToChild() = originatingNode->IndexToChild();
    branchedNode->ChildToIndex() = originatingNode->ChildToIndex();

    // Reference all children.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, originatingNode->IndexToChild()) {
        objectManager->RefObject(nodeId);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode* originatingNode,
    TListNode* branchedNode)
{
    // Drop all references held by the originator.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, originatingNode->IndexToChild()) {
        objectManager->UnrefObject(nodeId);
    }

    // Replace the child list with the branched copy.
    originatingNode->IndexToChild().swap(branchedNode->IndexToChild());
    originatingNode->ChildToIndex().swap(branchedNode->ChildToIndex());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

