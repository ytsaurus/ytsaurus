#include "common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//void TMapNode::Clear()
//{
//    // TODO:
//    YASSERT(false);
//}
//
//int TMapNode::GetChildCount() const
//{
//    return NameToChild.ysize();
//}
//
//yvector< TPair<Stroka, INode::TConstPtr> > TMapNode::GetChildren() const
//{
//    yvector< TPair<Stroka, INode::TConstPtr> > result;
//    result.reserve(NameToChild.ysize());
//    FOREACH (const auto& pair, NameToChild) {
//        const auto& child = CypressService->GetNode(pair.Second());
//    // TODO: const_cast is due to stupid intrusiveptr
//        result.push_back(MakePair(
//            pair.First(),
//            const_cast<ICypressNode*>(&child)));
//    }
//    return result;
//}
//
//
//INode::TConstPtr TMapNode::FindChild(const Stroka& name) const
//{
//    auto it = NameToChild.find(name);
//    // TODO: const_cast is due to stupid intrusiveptr
//    return
//        it == NameToChild.end()
//        ? NULL
//        : const_cast<ICypressNode*>(&CypressService->GetNode(it->Second()));
//}
//
//bool TMapNode::AddChild(INode::TPtr child, const Stroka& name)
//{
//    auto* registryChild = dynamic_cast<ICypressNode*>(~child);
//    YASSERT(registryChild != NULL);
//    auto childId = registryChild->GetId();
//    if (NameToChild.insert(MakePair(name, childId)).Second()) {
//        YVERIFY(ChildToName.insert(MakePair(childId, name)).Second());
//        child->SetParent(this);
//        return true;
//    } else {
//        return false;
//    }
//}
/*
bool TMapNode::RemoveChild(const Stroka& name)
{
    auto it = NameToChild.find(name);
    if (it == NameToChild.end())
        return false;

    auto child = it->Second(); 
    child->AsMutable()->SetParent(NULL);
    NameToChild.erase(it);
    YVERIFY(ChildToName.erase(child) == 1);

    return true;
}

void TMapNode::RemoveChild( INode::TPtr child )
{
    auto it = ChildToName.find(child);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();
    ChildToName.erase(it);
    YVERIFY(NameToChild.erase(name) == 1);
}

void TMapNode::ReplaceChild( INode::TPtr oldChild, INode::TPtr newChild )
{
    if (oldChild == newChild)
        return;

    auto it = ChildToName.find(oldChild);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();

    oldChild->SetParent(NULL);
    ChildToName.erase(it);

    NameToChild[name] = newChild;
    newChild->SetParent(this);
    YVERIFY(ChildToName.insert(MakePair(newChild, name)).Second());
}

IYPathService::TNavigateResult TMapNode::Navigate(
    const TYPath& path) const
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
        return TNavigateResult::CreateRecurse(child, tailPath);
    }
}

IYPathService::TSetResult TMapNode::Set(
    const TYPath& path,
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
        auto newChild = GetFactory()->CreateMap();
        AddChild(~newChild, prefix);
        return TSetResult::CreateRecurse(~newChild, tailPath);
    } else {
        return TSetResult::CreateRecurse(child, tailPath);
    }
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

