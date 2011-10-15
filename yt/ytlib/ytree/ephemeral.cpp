#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"

#include "../misc/hash.h"
#include "../misc/assert.h"

#include <util/generic/algorithm.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeBase
    : public ::NYT::NYTree::TNodeBase
{
public:
    TEphemeralNodeBase();

    virtual INodeFactory* GetFactory() const;

    virtual ICompositeNode::TPtr GetParent() const;
    virtual void SetParent(ICompositeNode::TPtr parent);

    virtual IMapNode::TPtr GetAttributes() const;
    virtual void SetAttributes(IMapNode::TPtr attributes);

private:
    ICompositeNode* Parent;
    IMapNode::TPtr Attributes;

};

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
class TScalarNode
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
    TScalarNode()
        : Value()
    { }

    virtual TValue GetValue() const
    {
        return Value;
    }

    virtual void SetValue(const TValue& value)
    {
        Value = value;
    }

private:
    TValue Value;

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_TYPE_OVERRIDES(name) \
public: \
    virtual ENodeType GetType() const \
    { \
        return ENodeType::name; \
    } \
    \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const \
    { \
        return const_cast<T ## name ## Node*>(this); \
    } \
    \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() \
    { \
        return this; \
    } \
    \
    virtual TSetResult SetSelf(TYsonProducer::TPtr producer) \
    { \
        SetNodeFromProducer(TIntrusivePtr<I##name##Node>(this), producer); \
        return TSetResult::CreateDone(); \
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
        DECLARE_TYPE_OVERRIDES(name) \
    };


DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase>
class TCompositeNodeBase
    : public TEphemeralNodeBase
    , public virtual IBase
{
public:
    virtual TIntrusivePtr<ICompositeNode> AsComposite()
    {
        return this;
    }

    virtual TIntrusiveConstPtr<ICompositeNode> AsComposite() const
    {
        return const_cast< TCompositeNodeBase<IBase>* >(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TCompositeNodeBase<IMapNode>
{
    DECLARE_TYPE_OVERRIDES(Map)

public:
    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const;
    virtual INode::TPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(INode::TPtr child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

    virtual TNavigateResult Navigate(
        TYPath path);
    virtual TSetResult Set(
        TYPath path,
        TYsonProducer::TPtr producer);

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase<IListNode>
{
    DECLARE_TYPE_OVERRIDES(List)

public:
    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<INode::TPtr> GetChildren() const;
    virtual INode::TPtr FindChild(int index) const;
    virtual void AddChild(INode::TPtr child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

    virtual TNavigateResult Navigate(
        TYPath path);
    virtual TSetResult Set(
        TYPath path,
        TYsonProducer::TPtr producer);

private:
    yvector<INode::TPtr> List;

    TNavigateResult GetYPathChild(int index, TYPath tailPath) const;
    TSetResult CreateYPathChild(int beforeIndex, TYPath tailPath);

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TEphemeralNodeBase
    , public virtual IEntityNode
{
    DECLARE_TYPE_OVERRIDES(Entity)
};

#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

TEphemeralNodeBase::TEphemeralNodeBase()
    : Parent(NULL)
{ }

INodeFactory* TEphemeralNodeBase::GetFactory() const
{
    return TEphemeralNodeFactory::Get();
}

ICompositeNode::TPtr TEphemeralNodeBase::GetParent() const
{
    return Parent;
}

void TEphemeralNodeBase::SetParent(ICompositeNode::TPtr parent)
{
    YASSERT(~parent == NULL || Parent == NULL);
    Parent = ~parent;
}

IMapNode::TPtr TEphemeralNodeBase::GetAttributes() const
{
    return Attributes;
}

void TEphemeralNodeBase::SetAttributes(IMapNode::TPtr attributes)
{
    if (~Attributes != NULL) {
        Attributes->SetParent(NULL);
        Attributes = NULL;
    }
    Attributes = attributes;
}

////////////////////////////////////////////////////////////////////////////////

void TMapNode::Clear()
{
    FOREACH(const auto& pair, NameToChild) {
        pair.Second()->SetParent(NULL);
    }
    NameToChild.clear();
    ChildToName.clear();
}

int TMapNode::GetChildCount() const
{
    return NameToChild.ysize();
}

yvector< TPair<Stroka, INode::TPtr> > TMapNode::GetChildren() const
{
    return yvector< TPair<Stroka, INode::TPtr> >(NameToChild.begin(), NameToChild.end());
}

INode::TPtr TMapNode::FindChild(const Stroka& name) const
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

bool TMapNode::RemoveChild(const Stroka& name)
{
    auto it = NameToChild.find(name);
    if (it == NameToChild.end())
        return false;

    auto child = it->Second(); 
    child->SetParent(NULL);
    NameToChild.erase(it);
    YVERIFY(ChildToName.erase(child) == 1);

    return true;
}

void TMapNode::RemoveChild(INode::TPtr child)
{
    child->SetParent(NULL);

    auto it = ChildToName.find(child);
    YASSERT(it != ChildToName.end());

    Stroka name = it->Second();
    ChildToName.erase(it);
    YVERIFY(NameToChild.erase(name) == 1);
}

void TMapNode::ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild)
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
        return TNavigateResult::CreateDone(this);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    auto child = FindChild(prefix);
    if (~child == NULL) {
        throw TYPathException() << Sprintf("Child %s it not found",
            ~prefix.Quote());
    }

    return TNavigateResult::CreateRecurse(AsYPath(child), tailPath);
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
        node->SetParent(NULL);
    }
    List.clear();
}

int TListNode::GetChildCount() const
{
    return List.ysize();
}

yvector<INode::TPtr> TListNode::GetChildren() const
{
    return List;
}

INode::TPtr TListNode::FindChild(int index) const
{
    return index >= 0 && index < List.ysize() ? List[index] : NULL;
}

void TListNode::AddChild(INode::TPtr child, int beforeIndex /*= -1*/)
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

    List[index]->SetParent(NULL);
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
        return TNavigateResult::CreateDone(this);
    }

    Stroka prefix;
    TYPath tailPath;
    ChopYPathPrefix(path, &prefix, &tailPath);

    int index;
    try {
        index = FromString<int>(prefix);
    } catch (...) {
        throw TYPathException() << Sprintf("Failed to parse child index %s",
            ~prefix.Quote());
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
        throw TYPathException() << "Empty child index";
    }

    if (prefix == "+") {
        return CreateYPathChild(GetChildCount(), tailPath);
    } else if (prefix == "-") {
        return CreateYPathChild(0, tailPath);
    }
    
    char lastPrefixCh = prefix[prefix.length() - 1];
    TStringBuf indexString =
        lastPrefixCh == '+' || lastPrefixCh == '-'
        ? TStringBuf(prefix.begin() + 1, prefix.end())
        : prefix;

    int index;
    try {
        index = FromString<int>(indexString);
    } catch (...) {
        throw TYPathException() << Sprintf("Failed to parse child index %s",
            ~Stroka(indexString).Quote());
    }

    if (lastPrefixCh == '+') {
        return CreateYPathChild(index + 1, tailPath);
    } else if (lastPrefixCh == '-') {
        return CreateYPathChild(index, tailPath);
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
    return TSetResult::CreateRecurse(AsYPath(~newChild), tailPath);
}

IYPathService::TNavigateResult TListNode::GetYPathChild(
    int index,
    TYPath tailPath) const
{
    int count = GetChildCount();
    if (count == 0) {
        throw TYPathException() << "List is empty";
    }

    if (index < 0 || index >= count) {
        throw TYPathException() << Sprintf("Invalid child index %d, expecting value in range 0..%d",
            index,
            count - 1);
    }

    auto child = FindChild(index);
    return TNavigateResult::CreateRecurse(AsYPath(child), tailPath);
}

////////////////////////////////////////////////////////////////////////////////

INodeFactory* TEphemeralNodeFactory::Get()
{
    return Singleton<TEphemeralNodeFactory>();
}

IStringNode::TPtr TEphemeralNodeFactory::CreateString()
{
    return ~New<TStringNode>();
}

IInt64Node::TPtr TEphemeralNodeFactory::CreateInt64()
{
    return ~New<TInt64Node>();
}

IDoubleNode::TPtr TEphemeralNodeFactory::CreateDouble()
{
    return ~New<TDoubleNode>();
}

IMapNode::TPtr TEphemeralNodeFactory::CreateMap()
{
    return ~New<TMapNode>();
}

IListNode::TPtr TEphemeralNodeFactory::CreateList()
{
    return ~New<TListNode>();
}

IEntityNode::TPtr TEphemeralNodeFactory::CreateEntity()
{
    return ~New<TEntityNode>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

