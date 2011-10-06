#include "node_proxy.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    TCypressState::TPtr state,
    const TTransactionId& transactionId,
    const TNodeId& nodeId,
    bool isMutable)
    : TCompositeNodeProxyBase(
        state,
        transactionId,
        nodeId,
        isMutable)
{ }

void TMapNodeProxy::Clear()
{
    // TODO: refcount
    YASSERT(IsMutable);

    Impl->NameToChild().clear();
    Impl->ChildToName().clear();
}

int TMapNodeProxy::GetChildCount() const
{
    return Impl->NameToChild().ysize();
}

yvector< TPair<Stroka, INode::TConstPtr> > TMapNodeProxy::GetChildren() const
{
    yvector< TPair<Stroka, INode::TConstPtr> > result;
    const auto& map = Impl->NameToChild();
    result.reserve(map.ysize());
    FOREACH (const auto& pair, map) {
        result.push_back(MakePair(
            pair.First(),
            GetProxy<INode>(pair.Second())));
    }
    return result;
}

INode::TConstPtr TMapNodeProxy::FindChild(const Stroka& name) const
{
    const auto& map = Impl->NameToChild();
    auto it = map.find(name);
    return it == map.end() ? NULL : GetProxy<INode>(it->Second());
}

bool TMapNodeProxy::AddChild(INode::TPtr child, const Stroka& name)
{
    // TODO: refcount
    YASSERT(IsMutable);

    auto childProxy = ToProxy(child);
    auto childId = childProxy->GetNodeId();
    if (Impl->NameToChild().insert(MakePair(name, childId)).Second()) {
        YVERIFY(Impl->ChildToName().insert(MakePair(childId, name)).Second());
        childProxy->SetParentId(NodeId);
        return true;
    } else {
        return false;
    }
}

bool TMapNodeProxy::RemoveChild(const Stroka& name)
{
    // TODO: refcount
    YASSERT(IsMutable);

    auto it = Impl->NameToChild().find(name);
    if (it == Impl->NameToChild().end())
        return false;

    auto childId = it->Second(); 
    auto& childImpl = State->GetNodeForUpdate(TBranchedNodeId(childId, TransactionId));
    childImpl.ParentId() = NullNodeId;
    Impl->NameToChild().erase(it);
    YVERIFY(Impl->ChildToName().erase(childId) == 1);
    return true;
}

void TMapNodeProxy::RemoveChild(INode::TPtr child)
{
    // TODO: refcount
    YASSERT(IsMutable);

    auto childProxy = ToProxy(child);
    childProxy->SetParentId(NullNodeId);

    auto it = Impl->ChildToName().find(childProxy->GetNodeId());
    YASSERT(it != Impl->ChildToName().end());

    Stroka name = it->Second();
    Impl->ChildToName().erase(it);
    YVERIFY(Impl->NameToChild().erase(name) == 1);
}

void TMapNodeProxy::ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
{
    // TODO: refcount
    YASSERT(IsMutable);

    if (oldChild == newChild)
        return;

    auto oldChildProxy = ToProxy(oldChild);
    auto newChildProxy = ToProxy(newChild);

    auto it = Impl->ChildToName().find(oldChildProxy->GetNodeId());
    YASSERT(it != Impl->ChildToName().end());

    Stroka name = it->Second();

    oldChildProxy->SetParentId(NullNodeId);
    Impl->ChildToName().erase(it);

    Impl->NameToChild()[name] = newChildProxy->GetNodeId();
    newChildProxy->SetParentId(NodeId);
    YVERIFY(Impl->ChildToName().insert(MakePair(newChildProxy->GetNodeId(), name)).Second());
}

// TODO: extract base 
IYPathService::TNavigateResult TMapNodeProxy::Navigate(TYPath path)
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
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

