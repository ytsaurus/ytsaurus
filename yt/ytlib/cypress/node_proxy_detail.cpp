#include "stdafx.h"
#include "node_proxy_detail.h"
#include "cypress_ypath_proxy.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TNodeFactory::TNodeFactory(
    TCypressManager* cypressManager,
    const TTransactionId& transactionId)
    : CypressManager(cypressManager)
    , TransactionId(transactionId)
{
    YASSERT(cypressManager);
}

TNodeFactory::~TNodeFactory()
{
    FOREACH (const auto& nodeId, CreatedNodeIds) {
        CypressManager->GetObjectManager()->UnrefObject(nodeId);
    }
}

ICypressNodeProxy::TPtr TNodeFactory::DoCreate(EObjectType type)
{
    auto id = CypressManager->CreateNode(type, TransactionId);
    CypressManager->GetObjectManager()->RefObject(id);
    CreatedNodeIds.push_back(id);
    return CypressManager->GetVersionedNodeProxy(id, NullTransactionId);
}

TStringNodePtr TNodeFactory::CreateString()
{
    return DoCreate(EObjectType::StringNode)->AsString();
}

TInt64NodePtr TNodeFactory::CreateInt64()
{
    return DoCreate(EObjectType::Int64Node)->AsInt64();
}

TDoubleNodePtr TNodeFactory::CreateDouble()
{
    return DoCreate(EObjectType::DoubleNode)->AsDouble();
}

TMapNodePtr TNodeFactory::CreateMap()
{
    return DoCreate(EObjectType::MapNode)->AsMap();
}

TListNodePtr TNodeFactory::CreateList()
{
    return DoCreate(EObjectType::ListNode)->AsList();
}

TEntityNodePtr TNodeFactory::CreateEntity()
{
    ythrow yexception() << "Entity nodes cannot be created inside Cypress";
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TBase(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
    , TransactionManager(cypressManager->GetTransactionManager())
{ }

void TMapNodeProxy::Clear()
{
    auto& impl = GetTypedImpl();

    FOREACH (const auto& pair, impl.ChildToKey()) {
        auto& childImpl = GetImpl(pair.first);
        DetachChild(childImpl);
    }

    impl.KeyToChild().clear();
    impl.ChildToKey().clear();
    impl.ChildCountDelta() = 0;

    const auto& children = DoGetChildren();
    FOREACH (const auto& pair, children) {
        YVERIFY(impl.KeyToChild().insert(MakePair(pair.first, NullObjectId)).second);
        --impl.ChildCountDelta();
    }
}

int TMapNodeProxy::GetChildCount() const
{
    return GetTypedImpl().ChildCountDelta();
}

yvector< TPair<Stroka, TNodePtr> > TMapNodeProxy::GetChildren() const
{
    const auto& children = DoGetChildren();
    return yvector< TPair<Stroka, TNodePtr> >(children.begin(), children.end());
}

yvector<Stroka> TMapNodeProxy::GetKeys() const
{
    yvector<Stroka> result;
    const auto& children = DoGetChildren();
    FOREACH (const auto& pair, children) {
        result.push_back(pair.first);
    }
    return result;
}

TNodePtr TMapNodeProxy::FindChild(const Stroka& key) const
{
    return DoFindChild(key, false);
}

bool TMapNodeProxy::AddChild(INode* child, const Stroka& key)
{
    YASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto& impl = GetTypedImpl();

    auto* childProxy = ToProxy(child);
    auto childId = childProxy->GetId();
    YASSERT(childId != NullObjectId);

    YVERIFY(impl.KeyToChild().insert(MakePair(key, childId)).second);
    YVERIFY(impl.ChildToKey().insert(MakePair(childId, key)).second);
    ++impl.ChildCountDelta();

    auto& childImpl = childProxy->GetImpl();
    AttachChild(childImpl);

    return true;
}

bool TMapNodeProxy::RemoveChild(const Stroka& key)
{
    auto& impl = GetTypedImpl();

    auto it = impl.KeyToChild().find(key);
    if (it != impl.KeyToChild().end()) {
        // NB: don't use const auto& here, it becomes invalid!
        auto childId = it->second;
        if (childId == NullObjectId) {
            return false;
        }
        
        if (DoFindChild(key, true)) {
            it->second = NullObjectId;
        } else {
            impl.KeyToChild().erase(it);
        }
        auto childProxy = GetProxy(childId);
        auto& childImpl = childProxy->GetImpl();

        YVERIFY(impl.ChildToKey().erase(childId) > 0);
        DetachChild(childImpl);
    } else {
        if (!DoFindChild(key, true)) {
            return false;
        }
        YVERIFY(impl.KeyToChild().insert(MakePair(key, NullObjectId)).second);
    }
    
    --impl.ChildCountDelta();
    return true;
}

void TMapNodeProxy::RemoveChild(INode* child)
{
    auto& impl = GetTypedImpl();
    
    auto* childProxy = ToProxy(child);

    auto it = impl.ChildToKey().find(childProxy->GetId());
    if (it != impl.ChildToKey().end()) {
        const auto& key = it->second;
        if (DoFindChild(key, true)) {
            impl.KeyToChild().find(key)->second = NullObjectId;
        } else {
            YVERIFY(impl.KeyToChild().erase(key) > 0);
        }
        
        impl.ChildToKey().erase(it);
        
        auto& childImpl = childProxy->GetImpl();
        DetachChild(childImpl);    
    } else {
        const auto& key = GetChildKey(child);
        YVERIFY(impl.KeyToChild().insert(MakePair(key, NullObjectId)).second);
    }
    --impl.ChildCountDelta();
}

void TMapNodeProxy::ReplaceChild(INode* oldChild, INode* newChild)
{
    if (oldChild == newChild)
        return;

    auto& impl = GetTypedImpl();

    auto* oldChildProxy = ToProxy(oldChild);
    auto& oldChildImpl = oldChildProxy->GetImpl();
    auto* newChildProxy = ToProxy(newChild);
    auto& newChildImpl = newChildProxy->GetImpl();

    Stroka key;

    auto it = impl.ChildToKey().find(oldChildProxy->GetId());
    if (it != impl.ChildToKey().end()) {
        // NB: don't use const auto& here, it becomes invalid!
        key = it->second;
        impl.ChildToKey().erase(it);
        DetachChild(oldChildImpl);
    } else {
        key = GetChildKey(oldChild);
        oldChildImpl.SetParentId(NullObjectId);
    }
    impl.KeyToChild()[key] = newChildProxy->GetId();
    YVERIFY(impl.ChildToKey().insert(MakePair(newChildProxy->GetId(), key)).second);    

    AttachChild(newChildImpl);
}

Stroka TMapNodeProxy::GetChildKey(const INode* child)
{
    auto* childProxy = ToProxy(child);

    auto transactionIds = TransactionManager->GetTransactionPath(TransactionId);
    FOREACH (const auto& transactionId, transactionIds) {
        const auto& node = CypressManager->GetVersionedNode(NodeId, transactionId);
        const auto& map = static_cast<const TMapNode&>(node).ChildToKey();
        auto it = map.find(childProxy->GetId());
        if (it != map.end()) {
            return it->second;
        }
    }

    YUNREACHABLE();
}

yhash_map<Stroka, TNodePtr> TMapNodeProxy::DoGetChildren() const
{
    yhash_map<Stroka, TNodePtr> result;
    auto transactionIds = TransactionManager->GetTransactionPath(TransactionId);
    for (auto it = transactionIds.rbegin(); it != transactionIds.rend(); ++it) {
        const auto& transactionId = *it;
        const auto& node = CypressManager->GetVersionedNode(NodeId, transactionId);
        const auto& map = static_cast<const TMapNode&>(node).KeyToChild();
        FOREACH (const auto& pair, map) {
            if (pair.second == NullTransactionId) {
                YVERIFY(result.erase(pair.first) > 0);
            } else {
                result[pair.first] = GetProxy(pair.second);
            }
        }
    }
    return result;
}

TNodePtr TMapNodeProxy::DoFindChild(const Stroka& key, bool skipCurrentTransaction) const
{
    auto transactionIds = TransactionManager->GetTransactionPath(TransactionId);
    FOREACH (const auto& transactionId, transactionIds) {
        if (skipCurrentTransaction && transactionId == TransactionId) {
            continue;
        }
        const auto& node = CypressManager->GetVersionedNode(NodeId, transactionId);
        const auto& map = static_cast<const TMapNode&>(node).KeyToChild();
        auto it = map.find(key);
        if (it != map.end()) {
            if (it->second == NullObjectId) {
                break;
            } else {
                return GetProxy(it->second);
            }
        }
    }
    return NULL;
}


void TMapNodeProxy::DoInvoke(NRpc::IServiceContext* context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TBase::DoInvoke(context);
}

void TMapNodeProxy::CreateRecursive(const TYPath& path, INode* value)
{
    auto factory = CreateFactory();
    TMapNodeMixin::SetRecursive(~factory, path, value);
}

IYPathService::TResolveResult TMapNodeProxy::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TMapNodeMixin::ResolveRecursive(path, verb);
}

void TMapNodeProxy::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    TMapNodeMixin::SetRecursive(~factory, path, request);
    context->Reply();
}

void TMapNodeProxy::SetNodeRecursive(
    const TYPath& path,
    TReqSetNode* request,
    TRspSetNode* response,
    TCtxSetNode* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    auto value = reinterpret_cast<INode*>(request->value());
    TMapNodeMixin::SetRecursive(~factory, path, value);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

TListNodeProxy::TListNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TBase(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

void TListNodeProxy::Clear()
{
    auto& impl = GetTypedImpl();

    FOREACH(auto& nodeId, impl.IndexToChild()) {
        auto& childImpl = GetImpl(nodeId);
        DetachChild(childImpl);
    }

    impl.IndexToChild().clear();
    impl.ChildToIndex().clear();
}

int TListNodeProxy::GetChildCount() const
{
    return GetTypedImpl().IndexToChild().ysize();
}

yvector<TNodePtr> TListNodeProxy::GetChildren() const
{
    yvector<TNodePtr> result;
    const auto& list = GetTypedImpl().IndexToChild();
    result.reserve(list.ysize());
    FOREACH (const auto& nodeId, list) {
        result.push_back(GetProxy(nodeId));
    }
    return result;
}

TNodePtr TListNodeProxy::FindChild(int index) const
{
    const auto& list = GetTypedImpl().IndexToChild();
    return index >= 0 && index < list.ysize() ? GetProxy(list[index]) : NULL;
}

void TListNodeProxy::AddChild(INode* child, int beforeIndex /*= -1*/)
{
    auto& impl = GetTypedImpl();
    auto& list = impl.IndexToChild();

    auto* childProxy = ToProxy(child);
    auto childId = childProxy->GetId();
    auto& childImpl = childProxy->GetImpl();

    if (beforeIndex < 0) {
        YVERIFY(impl.ChildToIndex().insert(MakePair(childId, list.ysize())).second);
        list.push_back(childId);
    } else {
        // Update the indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl.ChildToIndex()[*it];
        }

        // Insert the new child.
        YVERIFY(impl.ChildToIndex().insert(MakePair(childId, beforeIndex)).second);
        list.insert(list.begin() + beforeIndex, childId);
    }

    AttachChild(childImpl);
}

bool TListNodeProxy::RemoveChild(int index)
{
    auto& impl = GetTypedImpl();
    auto& list = impl.IndexToChild();

    if (index < 0 || index >= list.ysize())
        return false;

    auto childProxy = GetProxy(list[index]);
    auto& childImpl = childProxy->GetImpl();

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl.ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YVERIFY(impl.ChildToIndex().erase(childProxy->GetId()));
    DetachChild(childImpl);

    return true;
}

void TListNodeProxy::RemoveChild(INode* child)
{
    int index = GetChildIndex(child);
    YVERIFY(RemoveChild(index));
}

void TListNodeProxy::ReplaceChild(INode* oldChild, INode* newChild)
{
    if (oldChild == newChild)
        return;

    auto& impl = GetTypedImpl();

    auto* oldChildProxy = ToProxy(oldChild);
    auto& oldChildImpl = oldChildProxy->GetImpl();
    auto* newChildProxy = ToProxy(newChild);
    auto& newChildImpl = newChildProxy->GetImpl();

    auto it = impl.ChildToIndex().find(oldChildProxy->GetId());
    YASSERT(it != impl.ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl);

    impl.IndexToChild()[index] = newChildProxy->GetId();
    impl.ChildToIndex().erase(it);
    YVERIFY(impl.ChildToIndex().insert(MakePair(newChildProxy->GetId(), index)).second);
    AttachChild(newChildImpl);
}

int TListNodeProxy::GetChildIndex(const INode* child)
{
    auto& impl = GetTypedImpl();

    auto childProxy = ToProxy(child);

    auto it = impl.ChildToIndex().find(childProxy->GetId());
    YASSERT(it != impl.ChildToIndex().end());

    return it->second;
}

void TListNodeProxy::CreateRecursive(const TYPath& path, INode* value)
{
    auto factory = CreateFactory();
    TListNodeMixin::SetRecursive(~factory, path, value);
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    return TListNodeMixin::ResolveRecursive(path, verb);
}

void TListNodeProxy::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    TListNodeMixin::SetRecursive(~factory, path, request);
    context->Reply();
}

void TListNodeProxy::SetNodeRecursive(
    const TYPath& path,
    TReqSetNode* request,
    TRspSetNode* response,
    TCtxSetNode* context)
{
    UNUSED(response);

    auto factory = CreateFactory();
    auto value = reinterpret_cast<INode*>(request->value());
    TListNodeMixin::SetRecursive(~factory, path, value);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

