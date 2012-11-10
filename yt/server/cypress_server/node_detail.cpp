#include "stdafx.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "helpers.h"

#include <ytlib/ytree/fluent.h>

#include <server/cell_master/serialization_context.h>
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

bool TCypressNodeBase::IsAlive() const 
{
    return TObjectBase::IsAlive();
}

int TCypressNodeBase::GetOwningReplicationFactor() const 
{
    YUNREACHABLE();
}

void TCypressNodeBase::Save(const NCellMaster::TSaveContext& context) const
{
    TObjectBase::Save(context);

    auto* output = context.GetOutput();
    SaveObjectRefs(output, Locks_);
    ::Save(output, ParentId_);
    ::Save(output, LockMode_);
    ::Save(output, CreationTime_);
    ::Save(output, ModificationTime_);
}

void TCypressNodeBase::Load(const TLoadContext& context)
{
    TObjectBase::Load(context);

    auto* input = context.GetInput();
    LoadObjectRefs(input, Locks_, context);
    ::Load(input, ParentId_);
    ::Load(input, LockMode_);
    ::Load(input, CreationTime_);
    ::Load(input, ModificationTime_);

    TrunkNode_ = Id.IsBranched() ? context.Get<ICypressNode>(TVersionedObjectId(Id.ObjectId)) : this;
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChildCountDelta_(0)
{ }

void TMapNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, ChildCountDelta_);
    SaveMap(output, KeyToChild());
}

void TMapNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
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
    : TBase(bootstrap)
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
    TBase::DoDestroy(node);

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
    TBase::DoBranch(originatingNode, branchedNode);
}

void TMapNodeTypeHandler::DoMerge(
    TMapNode* originatingNode,
    TMapNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

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
    YASSERT(!trunkNode->GetId().IsBranched());
    YASSERT(trunkNode->GetTrunkNode() == trunkNode);
    return New<TMapNodeProxy>(
        this,
        Bootstrap,
        transaction,
        trunkNode);
}

void TMapNodeTypeHandler::DoClone(
    TMapNode* sourceNode,
    TMapNode* clonedNode,
    TTransaction* transaction)
{
    TBase::DoClone(sourceNode, clonedNode, transaction);

    auto keyToChildMap = GetMapNodeChildren(Bootstrap, sourceNode->GetId().ObjectId, transaction);
    std::vector< std::pair<Stroka, TNodeId> > keyToChildList(keyToChildMap.begin(), keyToChildMap.end());

    // Sort children by key to ensure deterministic ids generation.
    std::sort(
        keyToChildList.begin(),
        keyToChildList.end(),
        [] (const std::pair<Stroka, TNodeId>& lhs, const std::pair<Stroka, TNodeId>& rhs) {
            return lhs.first < rhs.first;
        });

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    FOREACH (const auto& pair, keyToChildList) {
        const auto& key = pair.first;
        const auto& childId = pair.second;
        
        auto* childNode = cypressManager->GetVersionedNode(childId, transaction);
        
        auto* clonedChildNode = cypressManager->CloneNode(childNode, transaction);
        const auto& clonedChildId = clonedChildNode->GetId().ObjectId;

        YCHECK(clonedNode->KeyToChild().insert(std::make_pair(key, clonedChildId)).second);
        YCHECK(clonedNode->ChildToKey().insert(std::make_pair(clonedChildId, key)).second);
        
        clonedChildNode->SetParentId(clonedNode->GetId().ObjectId);
        objectManager->RefObject(clonedChildNode);
    }

    YASSERT(clonedNode->ChildCountDelta() == 0);
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

void TListNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    ::Save(output, IndexToChild());
}

void TListNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
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
    TBase::DoDestroy(node);

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
    TBase::DoBranch(originatingNode, branchedNode);

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
    TBase::DoMerge(originatingNode, branchedNode);

    // Drop all references held by the originator.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, originatingNode->IndexToChild()) {
        objectManager->UnrefObject(nodeId);
    }

    // Replace the child list with the branched copy.
    originatingNode->IndexToChild().swap(branchedNode->IndexToChild());
    originatingNode->ChildToIndex().swap(branchedNode->ChildToIndex());
}

void TListNodeTypeHandler::DoClone(
    TListNode* sourceNode,
    TListNode* clonedNode,
    TTransaction* transaction)
{
    TBase::DoClone(sourceNode, clonedNode, transaction);

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    const auto& indexToChild = sourceNode->IndexToChild();
    for (int index = 0; index < indexToChild.size(); ++index) {
        const auto& childId = indexToChild[index];
        auto* childNode = cypressManager->GetVersionedNode(childId, transaction);

        auto* clonedChildNode = cypressManager->CloneNode(childNode, transaction);
        const auto& clonedChildId = clonedChildNode->GetId().ObjectId;

        clonedNode->IndexToChild().push_back(clonedChildId);
        YCHECK(clonedNode->ChildToIndex().insert(std::make_pair(clonedChildId, index)).second);

        clonedChildNode->SetParentId(clonedNode->GetId().ObjectId);
        objectManager->RefObject(clonedChildNode);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

