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
    auto node = CypressManager->CreateNode(type, TransactionId);
    auto nodeId = node->GetNodeId();
    CypressManager->GetObjectManager()->RefObject(nodeId);
    CreatedNodeIds.push_back(nodeId);
    return node;
}

IStringNode::TPtr TNodeFactory::CreateString()
{
    return DoCreate(EObjectType::StringNode)->AsString();
}

IInt64Node::TPtr TNodeFactory::CreateInt64()
{
    return DoCreate(EObjectType::Int64Node)->AsInt64();
}

IDoubleNode::TPtr TNodeFactory::CreateDouble()
{
    return DoCreate(EObjectType::DoubleNode)->AsDouble();
}

IMapNode::TPtr TNodeFactory::CreateMap()
{
    return DoCreate(EObjectType::MapNode)->AsMap();
}

IListNode::TPtr TNodeFactory::CreateList()
{
    return DoCreate(EObjectType::ListNode)->AsList();
}

IEntityNode::TPtr TNodeFactory::CreateEntity()
{
    ythrow yexception() << "Entity nodes cannot be created inside Cypress";
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCompositeNodeProxyBase(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

void TMapNodeProxy::Clear()
{
    LockIfNeeded();
    
    auto& impl = GetTypedImplForUpdate();

    FOREACH(const auto& pair, impl.NameToChild()) {
        auto& childImpl = GetImplForUpdate(pair.second);
        DetachChild(childImpl);
    }

    impl.NameToChild().clear();
    impl.ChildToName().clear();
}

int TMapNodeProxy::GetChildCount() const
{
    return GetTypedImpl().NameToChild().ysize();
}

yvector< TPair<Stroka, INode::TPtr> > TMapNodeProxy::GetChildren() const
{
    yvector< TPair<Stroka, INode::TPtr> > result;
    const auto& map = GetTypedImpl().NameToChild();
    result.reserve(map.ysize());
    FOREACH (const auto& pair, map) {
        result.push_back(MakePair(pair.first, GetProxy(pair.second)));
    }
    return result;
}

INode::TPtr TMapNodeProxy::FindChild(const Stroka& name) const
{
    const auto& map = GetTypedImpl().NameToChild();
    auto it = map.find(name);
    return it == map.end() ? NULL : GetProxy(it->second);
}

bool TMapNodeProxy::AddChild(INode* child, const Stroka& name)
{
    YASSERT(!name.empty());

    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    auto* childProxy = ToProxy(child);
    auto childId = childProxy->GetNodeId();

    if (!impl.NameToChild().insert(MakePair(name, childId)).second)
        return false;

    auto& childImpl = childProxy->GetImplForUpdate();
    YVERIFY(impl.ChildToName().insert(MakePair(childId, name)).second);
    AttachChild(childImpl);

    return true;
}

bool TMapNodeProxy::RemoveChild(const Stroka& name)
{
    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    auto it = impl.NameToChild().find(name);
    if (it == impl.NameToChild().end())
        return false;

    const auto& childId = it->second;
    auto childProxy = GetProxy(childId);
    auto& childImpl = childProxy->GetImplForUpdate();
    
    impl.NameToChild().erase(it);
    YVERIFY(impl.ChildToName().erase(childId) == 1);

    DetachChild(childImpl);
    
    return true;
}

void TMapNodeProxy::RemoveChild(INode* child)
{
    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();
    
    auto* childProxy = ToProxy(child);
    auto& childImpl = childProxy->GetImplForUpdate();

    auto it = impl.ChildToName().find(childProxy->GetNodeId());
    YASSERT(it != impl.ChildToName().end());

    Stroka name = it->second;
    impl.ChildToName().erase(it);
    YVERIFY(impl.NameToChild().erase(name) == 1);

    DetachChild(childImpl);
}

void TMapNodeProxy::ReplaceChild(INode* oldChild, INode* newChild)
{
    if (oldChild == newChild)
        return;

    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    auto* oldChildProxy = ToProxy(oldChild);
    auto& oldChildImpl = oldChildProxy->GetImplForUpdate();
    auto* newChildProxy = ToProxy(newChild);
    auto& newChildImpl = newChildProxy->GetImplForUpdate();

    auto it = impl.ChildToName().find(oldChildProxy->GetNodeId());
    YASSERT(it != impl.ChildToName().end());

    Stroka name = it->second;

    impl.ChildToName().erase(it);
    DetachChild(oldChildImpl);

    impl.NameToChild()[name] = newChildProxy->GetNodeId();
    YVERIFY(impl.ChildToName().insert(MakePair(newChildProxy->GetNodeId(), name)).second);
    AttachChild(newChildImpl);
}

Stroka TMapNodeProxy::GetChildKey(INode* child)
{
    auto& impl = GetTypedImpl();
    
    auto* childProxy = ToProxy(child);

    auto it = impl.ChildToName().find(childProxy->GetNodeId());
    YASSERT(it != impl.ChildToName().end());

    return it->second;
}

void TMapNodeProxy::DoInvoke(NRpc::IServiceContext* context)
{
    if (TMapNodeMixin::DoInvoke(context)) {
        // The verb is already handled.
    } else {
        TCompositeNodeProxyBase::DoInvoke(context);
    }
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
    : TCompositeNodeProxyBase(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

void TListNodeProxy::Clear()
{
    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    FOREACH(auto& nodeId, impl.IndexToChild()) {
        auto& childImpl = GetImplForUpdate(nodeId);
        DetachChild(childImpl);
    }

    impl.IndexToChild().clear();
    impl.ChildToIndex().clear();
}

int TListNodeProxy::GetChildCount() const
{
    return GetTypedImpl().IndexToChild().ysize();
}

yvector<INode::TPtr> TListNodeProxy::GetChildren() const
{
    yvector<INode::TPtr> result;
    const auto& list = GetTypedImpl().IndexToChild();
    result.reserve(list.ysize());
    FOREACH (const auto& nodeId, list) {
        result.push_back(GetProxy(nodeId));
    }
    return result;
}

INode::TPtr TListNodeProxy::FindChild(int index) const
{
    const auto& list = GetTypedImpl().IndexToChild();
    return index >= 0 && index < list.ysize() ? GetProxy(list[index]) : NULL;
}

void TListNodeProxy::AddChild(INode* child, int beforeIndex /*= -1*/)
{
    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();
    auto& list = impl.IndexToChild();

    auto* childProxy = ToProxy(child);
    auto childId = childProxy->GetNodeId();
    auto& childImpl = childProxy->GetImplForUpdate();

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
    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();
    auto& list = impl.IndexToChild();

    if (index < 0 || index >= list.ysize())
        return false;

    auto childProxy = GetProxy(list[index]);
    auto& childImpl = childProxy->GetImplForUpdate();

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl.ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YVERIFY(impl.ChildToIndex().erase(childProxy->GetNodeId()));
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

    LockIfNeeded();

    auto& impl = GetTypedImplForUpdate();

    auto* oldChildProxy = ToProxy(oldChild);
    auto& oldChildImpl = oldChildProxy->GetImplForUpdate();
    auto* newChildProxy = ToProxy(newChild);
    auto& newChildImpl = newChildProxy->GetImplForUpdate();

    auto it = impl.ChildToIndex().find(oldChildProxy->GetNodeId());
    YASSERT(it != impl.ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl);

    impl.IndexToChild()[index] = newChildProxy->GetNodeId();
    impl.ChildToIndex().erase(it);
    YVERIFY(impl.ChildToIndex().insert(MakePair(newChildProxy->GetNodeId(), index)).second);
    AttachChild(newChildImpl);
}

int TListNodeProxy::GetChildIndex(INode* child)
{
    auto& impl = GetTypedImplForUpdate();

    auto childProxy = ToProxy(child);

    auto it = impl.ChildToIndex().find(childProxy->GetNodeId());
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

TRootNodeProxy::TRootNodeProxy(
    INodeTypeHandler* typeHandler,
    TCypressManager* cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TMapNodeProxy(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
{ }

IYPathService::TResolveResult TRootNodeProxy::ResolveRecursive(
    const TYPath& path,
    const Stroka& verb)
{
    if (!path.empty() && path[0] == NodeIdMarker) {
        TYPath suffixPath;
        Stroka prefix;
        ChopYPathToken(path.substr(1), &prefix, &suffixPath);

        TNodeId nodeId;
        if (!TNodeId::FromString(prefix, &nodeId)) {
            ythrow yexception() << Sprintf("Error parsing node id %s", ~prefix.Quote());
        }

        auto node = GetProxy(nodeId);
        return TResolveResult::There(~IYPathService::FromNode(~node), suffixPath);
    } else {
        return TMapNodeProxy::ResolveRecursive(path, verb);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

