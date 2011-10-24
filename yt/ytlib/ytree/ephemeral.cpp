#include "stdafx.h"
#include "ephemeral.h"
#include "node_detail.h"
#include "ypath_detail.h"

#include "../misc/hash.h"
#include "../misc/assert.h"

#include <algorithm>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TEphemeralNodeBase
    : public virtual ::NYT::NYTree::TNodeBase
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
    , public TMapNodeMixin
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

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

    virtual TNavigateResult NavigateRecursive(TYPath path);
    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TCompositeNodeBase<IListNode>
    , public TListNodeMixin
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

private:
    yvector<INode::TPtr> List;

    virtual TNavigateResult NavigateRecursive(TYPath path);
    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);

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

IYPathService::TNavigateResult TMapNode::NavigateRecursive(TYPath path)
{
    return TMapNodeMixin::NavigateRecursive(path);
}

IYPathService::TSetResult TMapNode::SetRecursive(TYPath path, TYsonProducer::TPtr producer)
{
    return TMapNodeMixin::SetRecursive(
        path,
        producer,
        TTreeBuilder::CreateYsonBuilder(GetFactory()));
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
    auto it = std::find(List.begin(), List.end(), ~oldChild);
    YASSERT(it != List.end());

    oldChild->SetParent(NULL);
    *it = newChild;
    newChild->SetParent(this);
}

void TListNode::RemoveChild(INode::TPtr child)
{
    auto it = std::find(List.begin(), List.end(), ~child);
    YASSERT(it != List.end());
    List.erase(it);
}

IYPathService::TNavigateResult TListNode::NavigateRecursive(TYPath path)
{
    return TListNodeMixin::NavigateRecursive(path);
}

IYPathService::TSetResult TListNode::SetRecursive(TYPath path, TYsonProducer::TPtr producer)
{
    return TListNodeMixin::SetRecursive(
        path,
        producer,
        TTreeBuilder::CreateYsonBuilder(GetFactory()));
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

