#include "ephemeral.h"
#include "ypath.h"

#include "../misc/assert.h"

#include <util/generic/algorithm.h>

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

INodeFactory* TEphemeralNodeBase::GetFactory() const
{
    return TNodeFactory::Get();
}

////////////////////////////////////////////////////////////////////////////////

void TMapNode::Clear()
{
    FOREACH(const auto& pair, NameToChild) {
        pair.Second()->AsMutable()->SetParent(NULL);
    }
    NameToChild.clear();
    ChildToName.clear();
}

int TMapNode::GetChildCount() const
{
    return NameToChild.ysize();
}

yvector< TPair<Stroka, INode::TConstPtr> > TMapNode::GetChildren() const
{
    return yvector< TPair<Stroka, INode::TConstPtr> >(NameToChild.begin(), NameToChild.end());
}


INode::TConstPtr TMapNode::FindChild(const Stroka& name) const
{
    auto it = NameToChild.find(name);
    return it == NameToChild.end() ? NULL : it->Second();
}

bool TMapNode::AddChild(INode::TPtr child, const Stroka& name)
{
    if (NameToChild.insert(MakePair(name, child)).Second()) {
        YVERIFY(ChildToName.insert(MakePair(child, name)).Second());
        child->SetParent(this);
        return true;
    } else {
        return false;
    }
}

bool TMapNode::RemoveChild( const Stroka& name )
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
    TYPath path)
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

IYPathService::TSetResult TMapNode::Set(
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

void TListNode::Clear()
{
    FOREACH(const auto& node, List) {
        node->AsMutable()->SetParent(NULL);
    }
    List.clear();
}

int TListNode::GetChildCount() const
{
    return List.ysize();
}

yvector<INode::TConstPtr> TListNode::GetChildren() const
{
    return List;
}

INode::TConstPtr TListNode::FindChild(int index) const
{
    return index >= 0 && index < List.ysize() ? List[index] : NULL;
}

void TListNode::AddChild( INode::TPtr child, int beforeIndex /*= -1*/ )
{
    if (beforeIndex < 0) {
        List.push_back(child); 
    } else {
        List.insert(List.begin() + beforeIndex, child);
    }
    child->SetParent(this);
}

bool TListNode::RemoveChild(int index)
{
    if (index < 0 || index >= List.ysize())
        return false;

    List[index]->AsMutable()->SetParent(NULL);
    List.erase(List.begin() + index);
    return true;
}

void TListNode::ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
{
    auto it = Find(List.begin(), List.end(), ~oldChild);
    YASSERT(it != List.end());

    oldChild->SetParent(NULL);
    *it = newChild;
    newChild->SetParent(this);
}

void TListNode::RemoveChild(INode::TPtr child)
{
    auto it = Find(List.begin(), List.end(), ~child);
    YASSERT(it != List.end());
    List.erase(it);
}

IYPathService::TNavigateResult TListNode::Navigate(
    TYPath path)
{
    if (path.empty()) {
        return TNavigateResult::CreateDone(AsImmutable());
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    int index;
    try {
        index = FromString<int>(prefix);
    } catch (...) {
        return TNavigateResult::CreateError(Sprintf("Failed to parse child index %s",
            ~prefix.Quote()));
    }

    return GetYPathChild(index, tailPath);
}

IYPathService::TSetResult TListNode::Set(
    TYPath path,
    TYsonProducer::TPtr producer)
{
    if (path.empty()) {
        return SetSelf(producer);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    if (prefix.empty()) {
        return TSetResult::CreateError("Empty child index");
    }

    if (prefix == "+") {
        return CreateYPathChild(GetChildCount(), tailPath);
    } else if (prefix == "-") {
        return CreateYPathChild(0, tailPath);
    }
    
    TStringBuf indexString =
        prefix[0] == '+' || prefix[0] == '-'
        ? TStringBuf(prefix.begin() + 1, prefix.end())
        : prefix;

    int index;
    try {
        index = FromString<int>(indexString);
    } catch (...) {
        return TGetResult::CreateError(Sprintf("Failed to parse child index %s",
            ~Stroka(indexString).Quote()));
    }

    if (prefix[0] == '+') {
        return CreateYPathChild(index + 1, tailPath);
    } else if (prefix[0] == '-') {
        return CreateYPathChild(index + 1, tailPath);
    } else {
        auto navigateResult = GetYPathChild(index, tailPath);
        YASSERT(navigateResult.Code == IYPathService::ECode::Recurse);
        return TSetResult::CreateRecurse(navigateResult.RecurseService, navigateResult.RecursePath);
    }
}

IYPathService::TSetResult TListNode::CreateYPathChild(
    int beforeIndex,
    TYPath tailPath)
{
    auto newChild = GetFactory()->CreateMap();
    AddChild(~newChild, beforeIndex);
    return TSetResult::CreateRecurse(AsYPath(newChild->AsMutable()), tailPath);
}

IYPathService::TNavigateResult TListNode::GetYPathChild(
    int index,
    TYPath tailPath) const
{
    int count = GetChildCount();
    if (count == 0) {
        return TNavigateResult::CreateError("List is empty");
    }

    if (index < 0 || index >= count) {
        return TNavigateResult::CreateError(Sprintf("Invalid child index %d, expecting value in range 0..%d",
            index,
            count - 1));
    }

    auto child = FindChild(index);
    return TNavigateResult::CreateRecurse(AsYPath(child->AsMutable()), tailPath);
}

////////////////////////////////////////////////////////////////////////////////

INodeFactory* TNodeFactory::Get()
{
    return Singleton<TNodeFactory>();
}

IStringNode::TPtr TNodeFactory::CreateString(const Stroka& value /*= Stroka()*/)
{
    IStringNode::TPtr node = ~New<TStringNode>();
    node->SetValue(value);
    return node;
}

IInt64Node::TPtr TNodeFactory::CreateInt64(i64 value /*= 0*/)
{
    IInt64Node::TPtr node = ~New<TInt64Node>();
    node->SetValue(value);
    return node;
}

IDoubleNode::TPtr TNodeFactory::CreateDouble(double value /*= 0*/)
{
    IDoubleNode::TPtr node = ~New<TDoubleNode>();
    node->SetValue(value);
    return node;
}

IMapNode::TPtr TNodeFactory::CreateMap()
{
    return ~New<TMapNode>();
}

IListNode::TPtr TNodeFactory::CreateList()
{
    return ~New<TListNode>();
}

IEntityNode::TPtr TNodeFactory::CreateEntity()
{
    return ~New<TEntityNode>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT

