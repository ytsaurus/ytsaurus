#include "stdafx.h"
#include "node_detail.h"
#include "node_proxy_detail.h"
#include "helpers.h"

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;

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

TNontemplateCypressNodeTypeHandlerBase::TNontemplateCypressNodeTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{ }

bool TNontemplateCypressNodeTypeHandlerBase::IsRecovery() const
{
    return Bootstrap->GetMetaStateFacade()->GetManager()->IsRecovery();
}

void TNontemplateCypressNodeTypeHandlerBase::DestroyCore(TCypressNodeBase* node)
{
    auto objectManager = Bootstrap->GetObjectManager();

    // Remove user attributes, if any.
    auto id = node->GetVersionedId();
    if (objectManager->FindAttributes(id)) {
        objectManager->RemoveAttributes(id);
    }

    // Reset parent links from immediate ancestors.
    FOREACH (auto* ancestor, node->ImmediateAncestors()) {
        ancestor->ResetParent();
    }
    node->ImmediateAncestors().clear();

    // Remove self from immediate ancestors.
    node->SetParent(nullptr);
}

void TNontemplateCypressNodeTypeHandlerBase::BranchCore(
    const TCypressNodeBase* originatingNode,
    TCypressNodeBase* branchedNode,
    TTransaction* transaction,
    ELockMode mode)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    // Copy basic properties.
    branchedNode->SetParent(originatingNode->GetParent());
    branchedNode->SetCreationTime(originatingNode->GetCreationTime());
    branchedNode->SetModificationTime(originatingNode->GetModificationTime());
    branchedNode->SetLockMode(mode);
    branchedNode->SetTrunkNode(originatingNode->GetTrunkNode());
    branchedNode->SetTransaction(transaction);

    // Branch user attributes.
    objectManager->BranchAttributes(originatingNode->GetVersionedId(), branchedNode->GetVersionedId());
}

void TNontemplateCypressNodeTypeHandlerBase::MergeCore(
    TCypressNodeBase* originatingNode,
    TCypressNodeBase* branchedNode)
{
    auto objectManager = Bootstrap->GetObjectManager();

    auto originatingId = originatingNode->GetVersionedId();
    auto branchedId = branchedNode->GetVersionedId();
    YCHECK(branchedId.IsBranched());

    // Merge user attributes.
    objectManager->MergeAttributes(originatingId, branchedId);

    // Merge parent.
    originatingNode->SetParent(branchedNode->GetParent());
    branchedNode->SetParent(nullptr);

    // Merge modification time.
    if (branchedNode->GetModificationTime() > originatingNode->GetModificationTime()) {
        originatingNode->SetModificationTime(branchedNode->GetModificationTime());
    }
}

TAutoPtr<TCypressNodeBase> TNontemplateCypressNodeTypeHandlerBase::CloneCorePrologue(
    TCypressNodeBase* sourceNode,
    const TCloneContext& context)
{
    UNUSED(context);

    auto objectManager = Bootstrap->GetObjectManager();

    auto type = GetObjectType();
    auto clonedId = objectManager->GenerateId(type);

    auto clonedNode = Instantiate(TVersionedNodeId(clonedId));
    clonedNode->SetTrunkNode(~clonedNode);

    return clonedNode;
}

void TNontemplateCypressNodeTypeHandlerBase::CloneCoreEpilogue(
    TCypressNodeBase* sourceNode,
    TCypressNodeBase* clonedNode,
    const TCloneContext& context)
{
    UNUSED(sourceNode);

    // Copy attributes directly to suppress validation.
    auto objectManager = Bootstrap->GetObjectManager();
    auto keyToAttribute = GetNodeAttributes(Bootstrap, sourceNode->GetTrunkNode(), context.Transaction);
    if (!keyToAttribute.empty()) {
        auto* clonedAttributes = objectManager->CreateAttributes(clonedNode->GetVersionedId());
        FOREACH (const auto& pair, keyToAttribute) {
            YCHECK(clonedAttributes->Attributes().insert(pair).second);
        }
    }

    auto securityManager = Bootstrap->GetSecurityManager();

    // Set account.
    YCHECK(context.Account);
    securityManager->SetAccount(clonedNode, context.Account);

    // Set owner.
    YCHECK(context.Owner);
    auto* acd = securityManager->GetAcd(clonedNode);
    acd->SetOwner(context.Owner);
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
    // TODO(babenko): refactor when new serialization API is ready
    auto keyIts = GetSortedIterators(KeyToChild_);
    SaveSize(output, keyIts.size());
    FOREACH (auto it, keyIts) {
        const auto& key = it->first;
        NYT::Save(output, key);
        const auto* node = it->second;
        auto id = node ? node->GetId() : NullObjectId;
        NYT::Save(output, id);
    }
}

void TMapNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    ::Load(input, ChildCountDelta_);
    // TODO(babenko): refactor when new serialization API is ready
    size_t count = LoadSize(input);
    for (size_t index = 0; index != count; ++index) {
        Stroka key;
        ::Load(input, key);
        TNodeId id;
        NYT::Load(input, id);
        auto* node = id == NullObjectId ? nullptr : context.Get<TCypressNodeBase>(id);
        YCHECK(KeyToChild_.insert(std::make_pair(key, node)).second);
        YCHECK(ChildToKey_.insert(std::make_pair(node, key)).second);
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
        auto* node = pair.second;
        if (node) {
            objectManager->UnrefObject(node);
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

    bool isOriginatingNodeBranched = originatingNode->GetTransaction();

    auto& keyToChild = originatingNode->KeyToChild();
    auto& childToKey = originatingNode->ChildToKey();

    FOREACH (const auto& pair, branchedNode->KeyToChild()) {
        const auto& key = pair.first;
        auto* childTrunkNode = pair.second;

        auto it = keyToChild.find(key);
        if (childTrunkNode) {
            if (it == keyToChild.end()) {
                // Originating: missing
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
                YCHECK(keyToChild.insert(std::make_pair(key, childTrunkNode)).second);
            } else if (it->second) {
                // Originating: present
                objectManager->UnrefObject(it->second);
                YCHECK(childToKey.erase(it->second) == 1);
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
                it->second = childTrunkNode;;
            } else {
                // Originating: tombstone
                it->second = childTrunkNode;
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
            }
        } else {
            // Branched: tombstone
            if (it == keyToChild.end()) {
                // Originating: missing
                if (isOriginatingNodeBranched) {
                    // TODO(babenko): remove cast when GCC supports native nullptr
                    YCHECK(keyToChild.insert(std::make_pair(key, (TCypressNodeBase*) nullptr)).second);
                }
            } else if (it->second) {
                // Originating: present
                objectManager->UnrefObject(it->second);
                YCHECK(childToKey.erase(it->second) == 1);
                if (isOriginatingNodeBranched) {
                    it->second = nullptr;
                } else {
                    keyToChild.erase(it);
                }
            } else {
                // Originating: tombstone
            }
        }
    }

    originatingNode->ChildCountDelta() += branchedNode->ChildCountDelta();
}

ICypressNodeProxyPtr TMapNodeTypeHandler::DoGetProxy(
    TMapNode* trunkNode,
    TTransaction* transaction)
{
    return New<TMapNodeProxy>(
        this,
        Bootstrap,
        transaction,
        trunkNode);
}

void TMapNodeTypeHandler::DoClone(
    TMapNode* sourceNode,
    TMapNode* clonedNode,
    const TCloneContext& context)
{
    TBase::DoClone(sourceNode, clonedNode, context);

    auto keyToChildMap = GetMapNodeChildren(Bootstrap, sourceNode->GetTrunkNode(), context.Transaction);
    std::vector< std::pair<Stroka, TCypressNodeBase*> > keyToChildList(keyToChildMap.begin(), keyToChildMap.end());

    // Sort children by key to ensure deterministic ids generation.
    std::sort(
        keyToChildList.begin(),
        keyToChildList.end(),
        [] (const std::pair<Stroka, TCypressNodeBase*>& lhs, const std::pair<Stroka, TCypressNodeBase*>& rhs) {
            return lhs.first < rhs.first;
        });

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    FOREACH (const auto& pair, keyToChildList) {
        const auto& key = pair.first;
        auto* childTrunkNode = pair.second;

        auto* childNode = cypressManager->GetVersionedNode(childTrunkNode, context.Transaction);

        auto* clonedChildNode = cypressManager->CloneNode(childNode, context);
        auto* clonedTrunkChildNode = clonedChildNode->GetTrunkNode();

        YCHECK(clonedNode->KeyToChild().insert(std::make_pair(key, clonedTrunkChildNode)).second);
        YCHECK(clonedNode->ChildToKey().insert(std::make_pair(clonedTrunkChildNode, key)).second);

        clonedChildNode->SetParent(clonedNode->GetTrunkNode());
        objectManager->RefObject(clonedTrunkChildNode);
        ++clonedNode->ChildCountDelta();
    }
}

////////////////////////////////////////////////////////////////////////////////

TListNode::TListNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
{ }

void TListNode::Save(const NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    auto* output = context.GetOutput();
    // TODO(babenko): refactor when new serialization API is ready
    SaveSize(output, IndexToChild_.size());
    FOREACH (auto* node, IndexToChild_) {
        NYT::Save(output, node->GetId());
    }
}

void TListNode::Load(const NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    auto* input = context.GetInput();
    // TODO(babenko): refactor when new serialization API is ready
    size_t count = LoadSize(input);
    IndexToChild_.resize(count);
    for (size_t index = 0; index != count; ++index) {
        TNodeId id;
        NYT::Load(input, id);
        auto* node = context.Get<TCypressNodeBase>(id);
        IndexToChild_[index] = node;
        YCHECK(ChildToIndex_.insert(std::make_pair(node, index)).second);
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

ICypressNodeProxyPtr TListNodeTypeHandler::DoGetProxy(
    TListNode* trunkNode,
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
    FOREACH (auto* node, node->IndexToChild()) {
        objectManager->UnrefObject(node);
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
    FOREACH (auto* node, originatingNode->IndexToChild()) {
        objectManager->RefObject(node);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode* originatingNode,
    TListNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    // Drop all references held by the originator.
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* node, originatingNode->IndexToChild()) {
        objectManager->UnrefObject(node);
    }

    // Replace the child list with the branched copy.
    originatingNode->IndexToChild().swap(branchedNode->IndexToChild());
    originatingNode->ChildToIndex().swap(branchedNode->ChildToIndex());
}

void TListNodeTypeHandler::DoClone(
    TListNode* sourceNode,
    TListNode* clonedNode,
    const TCloneContext& context)
{
    TBase::DoClone(sourceNode, clonedNode, context);

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    const auto& indexToChild = sourceNode->IndexToChild();
    for (int index = 0; index < indexToChild.size(); ++index) {
        auto* childNode = indexToChild[index];
        auto* clonedChildNode = cypressManager->CloneNode(childNode, context);
        auto* clonedChildTrunkNode = clonedChildNode->GetTrunkNode();

        clonedNode->IndexToChild().push_back(clonedChildTrunkNode);
        YCHECK(clonedNode->ChildToIndex().insert(std::make_pair(clonedChildTrunkNode, index)).second);

        clonedChildNode->SetParent(clonedNode->GetTrunkNode());
        objectManager->RefObject(clonedChildTrunkNode);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

