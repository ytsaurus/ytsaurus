#include "stdafx.h"
#include "node_proxy_detail.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TNodeFactory::TNodeFactory(
    TBootstrap* bootstrap,
    TTransaction* transaction)
    : Bootstrap(bootstrap)
    , Transaction(transaction)
{
    YASSERT(bootstrap);
}

TNodeFactory::~TNodeFactory()
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, CreatedNodeIds) {
        objectManager->UnrefObject(nodeId);
    }
}

ICypressNodeProxyPtr TNodeFactory::DoCreate(EObjectType type)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto objectManager = Bootstrap->GetObjectManager();
   
    auto handler = cypressManager->GetHandler(type);
  
    auto node = handler->Create(Transaction, NULL, NULL);
    auto node_ = ~node;
    cypressManager->RegisterNode(Transaction, node);
    
    auto nodeId = node_->GetId().ObjectId;
    objectManager->RefObject(node_);
    CreatedNodeIds.push_back(nodeId);

    return cypressManager->GetVersionedNodeProxy(nodeId, Transaction);
}

IStringNodePtr TNodeFactory::CreateString()
{
    return DoCreate(EObjectType::StringNode)->AsString();
}

IIntegerNodePtr TNodeFactory::CreateInteger()
{
    return DoCreate(EObjectType::IntegerNode)->AsInteger();
}

IDoubleNodePtr TNodeFactory::CreateDouble()
{
    return DoCreate(EObjectType::DoubleNode)->AsDouble();
}

IMapNodePtr TNodeFactory::CreateMap()
{
    return DoCreate(EObjectType::MapNode)->AsMap();
}

IListNodePtr TNodeFactory::CreateList()
{
    return DoCreate(EObjectType::ListNode)->AsList();
}

IEntityNodePtr TNodeFactory::CreateEntity()
{
    ythrow yexception() << "Entity nodes cannot be created inside Cypress";
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    const TNodeId& nodeId)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        nodeId)
{ }

void TMapNodeProxy::Clear()
{
    auto* impl = GetTypedImplForUpdate();

    // Validate locks before applying any mutations.
    std::vector<ICypressNode*> storedChildren;
    FOREACH (const auto& pair, impl->KeyToChild()) {
        storedChildren.push_back(GetImplForUpdate(pair.second));
    }

    FOREACH (auto* child, storedChildren) {
        DetachChild(child);
    }

    impl->KeyToChild().clear();
    impl->ChildToKey().clear();
    impl->ChildCountDelta() = 0;

    auto children = DoGetChildren();
    FOREACH (const auto& pair, children) {
        YCHECK(impl->KeyToChild().insert(MakePair(pair.first, NullObjectId)).second);
        --impl->ChildCountDelta();
    }
}

int TMapNodeProxy::GetChildCount() const
{
    return GetTypedImpl()->ChildCountDelta();
}

std::vector< TPair<Stroka, INodePtr> > TMapNodeProxy::GetChildren() const
{
    auto children = DoGetChildren();
    return std::vector< TPair<Stroka, INodePtr> >(children.begin(), children.end());
}

std::vector<Stroka> TMapNodeProxy::GetKeys() const
{
    std::vector<Stroka> result;
    auto children = DoGetChildren();
    FOREACH (const auto& pair, children) {
        result.push_back(pair.first);
    }
    return result;
}

INodePtr TMapNodeProxy::FindChild(const TStringBuf& key) const
{
    return DoFindChild(key, false);
}

bool TMapNodeProxy::AddChild(INodePtr child, const TStringBuf& key)
{
    YASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto* impl = GetTypedImplForUpdate();

    auto childProxy = ToProxy(child);
    auto* childImpl = childProxy->GetImplForUpdate();

    auto childId = childProxy->GetId();
    YASSERT(childId != NullObjectId);

    YCHECK(impl->KeyToChild().insert(MakePair(key, childId)).second);
    YCHECK(impl->ChildToKey().insert(MakePair(childId, key)).second);
    ++impl->ChildCountDelta();

    AttachChild(childImpl);

    return true;
}

bool TMapNodeProxy::RemoveChild(const TStringBuf& key)
{
    auto* impl = GetTypedImplForUpdate();

    auto it = impl->KeyToChild().find(Stroka(key));
    if (it != impl->KeyToChild().end()) {
        // NB: don't use const auto& here, it becomes invalid!
        auto childId = it->second;
        if (childId == NullObjectId) {
            return false;
        }
        
        auto childProxy = GetProxy(childId);
        auto* childImpl = childProxy->GetImplForUpdate();

        if (DoFindChild(key, true)) {
            it->second = NullObjectId;
        } else {
            impl->KeyToChild().erase(it);
        }

        YCHECK(impl->ChildToKey().erase(childId) == 1);
        DetachChild(childImpl);
    } else {
        if (!DoFindChild(key, true)) {
            return false;
        }
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
    }
    
    --impl->ChildCountDelta();
    return true;
}

void TMapNodeProxy::RemoveChild(INodePtr child)
{
    auto* impl = GetTypedImplForUpdate();
    
    auto childProxy = ToProxy(child);
    auto* childImpl = childProxy->GetImplForUpdate();

    auto it = impl->ChildToKey().find(childProxy->GetId());
    if (it != impl->ChildToKey().end()) {
        const auto& key = it->second;
        if (DoFindChild(key, true)) {
            impl->KeyToChild().find(key)->second = NullObjectId;
        } else {
            YCHECK(impl->KeyToChild().erase(key) == 1);
        }
        impl->ChildToKey().erase(it);
        DetachChild(childImpl);    
    } else {
        const auto& key = GetChildKey(child);
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
    }
    --impl->ChildCountDelta();
}

void TMapNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto* impl = GetTypedImplForUpdate();

    auto oldChildProxy = ToProxy(oldChild);
    auto* oldChildImpl = oldChildProxy->GetImplForUpdate();

    auto newChildProxy = ToProxy(newChild);
    auto* newChildImpl = newChildProxy->GetImplForUpdate();

    Stroka key;

    auto it = impl->ChildToKey().find(oldChildProxy->GetId());
    if (it != impl->ChildToKey().end()) {
        // NB: don't use const auto& here, it becomes invalid!
        key = it->second;
        impl->ChildToKey().erase(it);
        DetachChild(oldChildImpl);
    } else {
        key = GetChildKey(oldChild);
        oldChildImpl->SetParentId(NullObjectId);
    }
    impl->KeyToChild()[key] = newChildProxy->GetId();
    YCHECK(impl->ChildToKey().insert(MakePair(newChildProxy->GetId(), key)).second);    

    AttachChild(newChildImpl);
}

Stroka TMapNodeProxy::GetChildKey(IConstNodePtr child)
{
    auto childProxy = ToProxy(child);

    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    
    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto& map = static_cast<const TMapNode*>(node)->ChildToKey();
        auto it = map.find(childProxy->GetId());
        if (it != map.end()) {
            return it->second;
        }
    }

    YUNREACHABLE();
}

yhash_map<Stroka, ICypressNodeProxyPtr> TMapNodeProxy::DoGetChildren() const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    std::reverse(transactions.begin(), transactions.end());

    yhash_map<Stroka, ICypressNodeProxyPtr> result;
    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto& map = static_cast<const TMapNode*>(node)->KeyToChild();
        FOREACH (const auto& pair, map) {
            if (pair.second == NullTransactionId) {
                YCHECK(result.erase(pair.first) == 1);
            } else {
                result[pair.first] = GetProxy(pair.second);
            }
        }
    }
    return result;
}

INodePtr TMapNodeProxy::DoFindChild(const TStringBuf& key, bool skipCurrentTransaction) const
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    Stroka keyString(key);

    auto transactions = transactionManager->GetTransactionPath(Transaction);

    FOREACH (const auto* transaction, transactions) {
        if (skipCurrentTransaction && transaction == Transaction) {
            continue;
        }
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto& map = static_cast<const TMapNode*>(node)->KeyToChild();
        auto it = map.find(keyString);
        if (it != map.end()) {
            return it->second == NullObjectId ? NULL : GetProxy(it->second);
        }
    }

    return NULL;
}

void TMapNodeProxy::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TBase::DoInvoke(context);
}

void TMapNodeProxy::SetRecursive(const TYPath& path, INodePtr value)
{
    TMapNodeMixin::SetRecursive(path, value);
}

IYPathService::TResolveResult TMapNodeProxy::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TMapNodeMixin::ResolveRecursive(path, verb);
}

void TMapNodeProxy::DoCloneTo(TMapNode* clonedNode)
{
    TBase::DoCloneTo(clonedNode);

    auto objectManager = Bootstrap->GetObjectManager();

    auto children = DoGetChildren();
    FOREACH (const auto& pair, children) {
        auto key = pair.first;
        auto childId = pair.second->GetId();
        YCHECK(clonedNode->KeyToChild().insert(std::make_pair(key, childId)).second);
        YCHECK(clonedNode->ChildToKey().insert(std::make_pair(childId, key)).second);
        objectManager->RefObject(childId);
    }

    YASSERT(clonedNode->ChildCountDelta() == 0);
}

////////////////////////////////////////////////////////////////////////////////

TListNodeProxy::TListNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    const TNodeId& nodeId)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        nodeId)
{ }

void TListNodeProxy::Clear()
{
    auto* impl = GetTypedImplForUpdate();

    // Validate locks and obtain impls first;
    std::vector<ICypressNode*> children;
    FOREACH (auto& nodeId, impl->IndexToChild()) {
        children.push_back(GetImplForUpdate(nodeId));
    }

    FOREACH (auto* child, children) {
        DetachChild(child);
    }

    impl->IndexToChild().clear();
    impl->ChildToIndex().clear();
}

int TListNodeProxy::GetChildCount() const
{
    return GetTypedImpl()->IndexToChild().size();
}

std::vector<INodePtr> TListNodeProxy::GetChildren() const
{
    auto children = DoGetChildren();
    return std::vector<INodePtr>(children.begin(), children.end());
}

INodePtr TListNodeProxy::FindChild(int index) const
{
    const auto& list = GetTypedImpl()->IndexToChild();
    return index >= 0 && index < list.size() ? GetProxy(list[index]) : NULL;
}

void TListNodeProxy::AddChild(INodePtr child, int beforeIndex /*= -1*/)
{
    auto* impl = GetTypedImplForUpdate();
    auto& list = impl->IndexToChild();

    auto childProxy = ToProxy(child);
    auto childId = childProxy->GetId();
    auto* childImpl = childProxy->GetImplForUpdate();

    if (beforeIndex < 0) {
        YCHECK(impl->ChildToIndex().insert(MakePair(childId, list.size())).second);
        list.push_back(childId);
    } else {
        // Update the indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl->ChildToIndex()[*it];
        }

        // Insert the new child.
        YCHECK(impl->ChildToIndex().insert(MakePair(childId, beforeIndex)).second);
        list.insert(list.begin() + beforeIndex, childId);
    }

    AttachChild(childImpl);
}

bool TListNodeProxy::RemoveChild(int index)
{
    auto* impl = GetTypedImplForUpdate();
    auto& list = impl->IndexToChild();

    if (index < 0 || index >= list.size()) {
        return false;
    }

    auto childProxy = GetProxy(list[index]);
    auto* childImpl = childProxy->GetImplForUpdate();

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl->ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YCHECK(impl->ChildToIndex().erase(childProxy->GetId()));
    DetachChild(childImpl);

    return true;
}

void TListNodeProxy::RemoveChild(INodePtr child)
{
    int index = GetChildIndex(child);
    YCHECK(RemoveChild(index));
}

void TListNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto* impl = GetTypedImplForUpdate();

    auto oldChildProxy = ToProxy(oldChild);
    auto* oldChildImpl = oldChildProxy->GetImplForUpdate();

    auto newChildProxy = ToProxy(newChild);
    auto* newChildImpl = newChildProxy->GetImplForUpdate();

    auto it = impl->ChildToIndex().find(oldChildProxy->GetId());
    YASSERT(it != impl->ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl);

    impl->IndexToChild()[index] = newChildProxy->GetId();
    impl->ChildToIndex().erase(it);
    YCHECK(impl->ChildToIndex().insert(MakePair(newChildProxy->GetId(), index)).second);
    AttachChild(newChildImpl);
}

int TListNodeProxy::GetChildIndex(IConstNodePtr child)
{
    auto* impl = GetTypedImpl();

    auto childProxy = ToProxy(child);

    auto it = impl->ChildToIndex().find(childProxy->GetId());
    YASSERT(it != impl->ChildToIndex().end());

    return it->second;
}

void TListNodeProxy::SetRecursive(const TYPath& path, INodePtr value)
{
    TListNodeMixin::SetRecursive(path, value);
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TListNodeMixin::ResolveRecursive(path, verb);
}

std::vector<ICypressNodeProxyPtr> TListNodeProxy::DoGetChildren() const
{
    std::vector<ICypressNodeProxyPtr> result;
    const auto& list = GetTypedImpl()->IndexToChild();
    result.reserve(list.size());
    FOREACH (const auto& nodeId, list) {
        result.push_back(GetProxy(nodeId));
    }
    return result;
}

void TListNodeProxy::DoCloneTo(TListNode* clonedNode)
{
    TBase::DoCloneTo(clonedNode);

    auto objectManager = Bootstrap->GetObjectManager();

    auto children = DoGetChildren();
    for (int index = 0; index < static_cast<int>(children.size()); ++index) {
        auto child = children[index];
        auto childId = child->GetId();
        clonedNode->IndexToChild()[index] = childId;
        YCHECK(clonedNode->ChildToIndex().insert(std::make_pair(childId, index)).second);
        objectManager->RefObject(childId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

