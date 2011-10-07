#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    TCypressState::TPtr state,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCompositeNodeProxyBase(
        state,
        transactionId,
        nodeId)
{ }

void TMapNodeProxy::Clear()
{
    // TODO: refcount
    auto& impl = GetMutableTypedImpl();
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
        result.push_back(MakePair(
            pair.First(),
            GetProxy<INode>(pair.Second())));
    }
    return result;
}

INode::TPtr TMapNodeProxy::FindChild(const Stroka& name) const
{
    const auto& map = GetTypedImpl().NameToChild();
    auto it = map.find(name);
    return it == map.end() ? NULL : GetProxy<INode>(it->Second());
}

bool TMapNodeProxy::AddChild(INode::TPtr child, const Stroka& name)
{
    // TODO: refcount
    auto& impl = GetMutableTypedImpl();

    auto childProxy = ToProxy(child);
    auto childId = childProxy->GetNodeId();

    if (impl.NameToChild().insert(MakePair(name, childId)).Second()) {
        YVERIFY(impl.ChildToName().insert(MakePair(childId, name)).Second());
        childProxy->GetMutableImpl().ParentId() = NodeId;
        return true;
    } else {
        return false;
    }
}

bool TMapNodeProxy::RemoveChild(const Stroka& name)
{
    // TODO: refcount

    auto& impl = GetMutableTypedImpl();

    auto it = impl.NameToChild().find(name);
    if (it == impl.NameToChild().end())
        return false;

    const auto& childId = it->Second();
    auto childProxy = GetProxy<ICypressNodeProxy>(childId);
    auto& childImpl = childProxy->GetMutableImpl();
    childImpl.ParentId() = NullNodeId;
    impl.NameToChild().erase(it);
    YVERIFY(impl.ChildToName().erase(childId) == 1);
    return true;
}

void TMapNodeProxy::RemoveChild(INode::TPtr child)
{
    // TODO: refcount

    auto& impl = GetMutableTypedImpl();
    
    auto childProxy = ToProxy(child);
    childProxy->GetMutableImpl().ParentId() = NullNodeId;

    auto it = impl.ChildToName().find(childProxy->GetNodeId());
    YASSERT(it != impl.ChildToName().end());

    Stroka name = it->Second();
    impl.ChildToName().erase(it);
    YVERIFY(impl.NameToChild().erase(name) == 1);
}

void TMapNodeProxy::ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
{
    // TODO: refcount

    auto& impl = GetMutableTypedImpl();

    if (oldChild == newChild)
        return;

    auto oldChildProxy = ToProxy(oldChild);
    auto newChildProxy = ToProxy(newChild);

    auto it = impl.ChildToName().find(oldChildProxy->GetNodeId());
    YASSERT(it != impl.ChildToName().end());

    Stroka name = it->Second();

    oldChildProxy->GetMutableImpl().ParentId() = NullNodeId;
    impl.ChildToName().erase(it);

    impl.NameToChild()[name] = newChildProxy->GetNodeId();
    newChildProxy->GetMutableImpl().ParentId() = NodeId;
    YVERIFY(impl.ChildToName().insert(MakePair(newChildProxy->GetNodeId(), name)).Second());
}

// TODO: extract base 
IYPathService::TNavigateResult TMapNodeProxy::Navigate(TYPath path)
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(this);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        return TNavigateResult::CreateError(Sprintf("Child %s it not found",
            ~prefix.Quote()));
    } else {
        return TNavigateResult::CreateRecurse(AsYPath(child), tailPath);
    }
}

IYPathService::TSetResult TMapNodeProxy::Set(
    TYPath path,
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        INode::TPtr newChild = ~GetFactory()->CreateMap();
        AddChild(~newChild, prefix);
        return TSetResult::CreateRecurse(AsYPath(newChild), tailPath);
    } else {
        return TSetResult::CreateRecurse(AsYPath(child), tailPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

