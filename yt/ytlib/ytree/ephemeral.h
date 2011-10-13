#pragma once

#include "common.h"
#include "node.h"
#include "ypath_detail.h"

#include "../misc/hash.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

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
    virtual void DoSetSelf(TYsonProducer::TPtr producer) \
    { \
        SetNodeFromProducer(TIntrusivePtr<I##name##Node>(this), producer); \
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

class TNodeFactory
    : public INodeFactory
{
public:
    static INodeFactory* Get();

    virtual IStringNode::TPtr CreateString();
    virtual IInt64Node::TPtr CreateInt64();
    virtual IDoubleNode::TPtr CreateDouble();
    virtual IMapNode::TPtr CreateMap();
    virtual IListNode::TPtr CreateList();
    virtual IEntityNode::TPtr CreateEntity();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT

