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
    YCHECK(bootstrap);
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
    // Take shared lock for the node itself.
    auto* impl = LockThisTypedImpl(ELockMode::Shared);

    // Construct children list.
    yhash_map<Stroka, TNodeId> keyToChild;
    DoListChildren(&keyToChild);

    // Take exclusive locks for children.
    std::vector< std::pair<Stroka, ICypressNode*> > children;
    FOREACH (const auto& pair, keyToChild) {
        LockThisImpl(TLockRequest::SharedChild(pair.first));
        auto* child = LockImpl(pair.second);
        children.push_back(std::make_pair(pair.first, child));
    }

    // Detach children.
    // Insert tombstones.
    FOREACH (const auto& pair, children) {
        const auto& key = pair.first;
        auto* child = pair.second;
        const auto& childId = child->GetId().ObjectId;
        if (impl->KeyToChild().find(key) != impl->KeyToChild().end()) {
            YCHECK(impl->KeyToChild().erase(key) == 1);
            YCHECK(impl->ChildToKey().erase(childId) == 1);
            DetachChild(child, true);
        } else {
            YCHECK(impl->KeyToChild().insert(std::make_pair(key, NullObjectId)).second);
            DetachChild(child, false);
        }
        --impl->ChildCountDelta();
    }
}

int TMapNodeProxy::GetChildCount() const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);

    int result = 0;
    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto* mapNode = static_cast<const TMapNode*>(node);
        result += mapNode->ChildCountDelta();
    }
    return result;
}

std::vector< TPair<Stroka, INodePtr> > TMapNodeProxy::GetChildren() const
{
    yhash_map<Stroka, TNodeId> keyToChild;
    DoListChildren(&keyToChild);

    std::vector< TPair<Stroka, INodePtr> > result;
    result.reserve(keyToChild.size());
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(std::make_pair(pair.first, GetProxy(pair.second)));
    }
    return result;
}

std::vector<Stroka> TMapNodeProxy::GetKeys() const
{
    yhash_map<Stroka, TNodeId> keyToChild;
    DoListChildren(&keyToChild);

    std::vector<Stroka> result;
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(pair.first);
    }
    return result;
}

INodePtr TMapNodeProxy::FindChild(const TStringBuf& key) const
{
    auto versionedChildId = DoFindChild(Stroka(key));
    return versionedChildId.ObjectId == NullObjectId ? NULL : GetProxy(versionedChildId.ObjectId);
}

bool TMapNodeProxy::AddChild(INodePtr child, const TStringBuf& key)
{
    YASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    Stroka keyString(key);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(keyString));

    auto childId = GetNodeId(child);
    auto* childImpl = LockImpl(childId);

    YCHECK(impl->KeyToChild().insert(MakePair(keyString, childId)).second);
    YCHECK(impl->ChildToKey().insert(MakePair(childId, keyString)).second);
    ++impl->ChildCountDelta();

    AttachChild(childImpl);

    return true;
}

bool TMapNodeProxy::RemoveChild(const TStringBuf& key)
{
    Stroka keyString(key);

    auto versionedChildId = DoFindChild(keyString);
    if (versionedChildId.ObjectId == NullObjectId) {
        return false;
    }

    const auto& childId = versionedChildId.ObjectId;
    auto* childImpl = LockImpl(childId, ELockMode::Exclusive, true);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(keyString));

    if (versionedChildId.TransactionId == GetObjectId(Transaction)) {
        YCHECK(impl->KeyToChild().erase(keyString) == 1);
        YCHECK(impl->ChildToKey().erase(childId) == 1);
        DetachChild(childImpl, true);
    } else {
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
        DetachChild(childImpl, false);
    }

    --impl->ChildCountDelta();

    return true;
}

void TMapNodeProxy::RemoveChild(INodePtr child)
{
    auto key = GetChildKey(child);
    auto childId = GetNodeId(child);

    auto* childImpl = LockImpl(childId, ELockMode::Exclusive, true);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));

    auto it = impl->ChildToKey().find(childId);
    if (it != impl->ChildToKey().end()) {
        YCHECK(impl->KeyToChild().erase(key) == 1);
        YCHECK(impl->ChildToKey().erase(childId) == 1);
        DetachChild(childImpl, true);
    } else {
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
        DetachChild(childImpl, false);
    }

    --impl->ChildCountDelta();
}

void TMapNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto key = GetChildKey(oldChild);

    auto oldChildId = GetNodeId(oldChild);
    auto* oldChildImpl = LockImpl(oldChildId, ELockMode::Exclusive, true);

    auto newChildId = GetNodeId(newChild);
    auto* newChildImpl = LockImpl(newChildId);

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    impl->KeyToChild()[key] = newChildId;
    bool ownsOldChild = impl->KeyToChild().find(key) != impl->KeyToChild().end();
    DetachChild(oldChildImpl, ownsOldChild);
    YCHECK(impl->ChildToKey().insert(MakePair(newChildId, key)).second);    
    AttachChild(newChildImpl);
}

Stroka TMapNodeProxy::GetChildKey(IConstNodePtr child)
{
    auto childId = GetNodeId(child);

    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    
    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto& map = static_cast<const TMapNode*>(node)->ChildToKey();
        auto it = map.find(childId);
        if (it != map.end()) {
            return it->second;
        }
    }

    YUNREACHABLE();
}

void TMapNodeProxy::DoListChildren(yhash_map<Stroka, TNodeId>* keyToChild) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    std::reverse(transactions.begin(), transactions.end());

    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto* mapNode = static_cast<const TMapNode*>(node);
        FOREACH (const auto& pair, mapNode->KeyToChild()) {
            if (pair.second == NullObjectId) {
                YCHECK(keyToChild->erase(pair.first) == 1);
            } else {
                (*keyToChild)[pair.first] = pair.second;
            }
        }
    }
}

TVersionedNodeId TMapNodeProxy::DoFindChild(const Stroka& key) const
{
    auto transactionManager = Bootstrap->GetTransactionManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);

    FOREACH (const auto* transaction, transactions) {
        const auto* node = cypressManager->GetVersionedNode(NodeId, transaction);
        const auto& map = static_cast<const TMapNode*>(node)->KeyToChild();
        auto it = map.find(key);
        if (it != map.end()) {
            return TVersionedNodeId(it->second, GetObjectId(transaction));
        }
    }

    return TVersionedNodeId(NullObjectId, NullTransactionId);
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

    yhash_map<Stroka, TNodeId> keyToChild;
    DoListChildren(&keyToChild);

    FOREACH (const auto& pair, keyToChild) {
        auto key = pair.first;
        auto childId = pair.second;
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
    auto* impl = LockThisTypedImpl();

    // Validate locks and obtain impls first.
    std::vector<ICypressNode*> children;
    FOREACH (const auto& nodeId, impl->IndexToChild()) {
        children.push_back(LockImpl(nodeId));
    }

    FOREACH (auto* child, children) {
        DetachChild(child, true);
    }

    impl->IndexToChild().clear();
    impl->ChildToIndex().clear();
}

int TListNodeProxy::GetChildCount() const
{
    const auto* impl = GetThisTypedImpl();
    return impl->IndexToChild().size();
}

std::vector<INodePtr> TListNodeProxy::GetChildren() const
{
    std::vector<INodePtr> result;
    const auto* impl = GetThisTypedImpl();
    const auto& indexToChild = impl->IndexToChild();
    result.reserve(indexToChild.size());
    FOREACH (const auto& nodeId, indexToChild) {
        result.push_back(GetProxy(nodeId));
    }
    return result;
}

INodePtr TListNodeProxy::FindChild(int index) const
{
    const auto* impl = GetThisTypedImpl();
    const auto& indexToChild = impl->IndexToChild();
    return index >= 0 && index < indexToChild.size() ? GetProxy(indexToChild[index]) : NULL;
}

void TListNodeProxy::AddChild(INodePtr child, int beforeIndex /*= -1*/)
{
    auto* impl = LockThisTypedImpl();
    auto& list = impl->IndexToChild();

    auto childId = GetNodeId(child);
    auto* childImpl = LockImpl(childId);

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
    auto* impl = LockThisTypedImpl(ELockMode::Exclusive, true);
    auto& list = impl->IndexToChild();

    if (index < 0 || index >= list.size()) {
        return false;
    }

    auto childProxy = GetProxy(list[index]);
    auto* childImpl = LockImpl(childProxy->GetId());

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl->ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YCHECK(impl->ChildToIndex().erase(childProxy->GetId()));
    DetachChild(childImpl, true);

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

    auto* impl = LockThisTypedImpl();

    auto oldChildId = GetNodeId(oldChild);
    auto* oldChildImpl = LockImpl(oldChildId);

    auto newChildId = GetNodeId(newChild);
    auto* newChildImpl = LockImpl(newChildId);

    auto it = impl->ChildToIndex().find(oldChildId);
    YASSERT(it != impl->ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl, true);

    impl->IndexToChild()[index] = newChildId;
    impl->ChildToIndex().erase(it);
    YCHECK(impl->ChildToIndex().insert(MakePair(newChildId, index)).second);
    AttachChild(newChildImpl);
}

int TListNodeProxy::GetChildIndex(IConstNodePtr child)
{
    const auto* impl = GetThisTypedImpl();

    auto childId = GetNodeId(child);

    auto it = impl->ChildToIndex().find(childId);
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

void TListNodeProxy::DoCloneTo(TListNode* clonedNode)
{
    TBase::DoCloneTo(clonedNode);

    auto objectManager = Bootstrap->GetObjectManager();
    const auto* impl = GetThisTypedImpl();
    const auto& indexToChild = impl->IndexToChild();
    for (int index = 0; index < indexToChild.size(); ++index) {
        const auto& childId = indexToChild[index];
        clonedNode->IndexToChild().push_back(childId);
        YCHECK(clonedNode->ChildToIndex().insert(std::make_pair(childId, index)).second);
        objectManager->RefObject(childId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

