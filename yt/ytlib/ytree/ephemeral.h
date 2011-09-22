#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath.h"

#include "../misc/hash.h"

namespace NYT {
namespace NYTree {
namespace NEphemeral {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public ::NYT::NYTree::TNodeBase
{
public:
    virtual INodeFactory* GetFactory() const;

};

////////////////////////////////////////////////////////////////////////////////

template<class TValue, class IBase>
class TScalarNode
    : public TNodeBase
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
    virtual ENodeType GetType() const \
    { \
        return ENodeType::name; \
    } \
    \
    virtual I ## name ## Node::TConstPtr As ## name() const \
    { \
        return const_cast<T ## name ## Node*>(this); \
    } \
    \
    virtual I ## name ## Node::TPtr As ## name() \
    { \
        return this; \
    }

#define DECLARE_SCALAR_TYPE(name, type) \
    class T ## name ## Node \
        : public TScalarNode<type, I ## name ## Node> \
    { \
    public: \
        DECLARE_TYPE_OVERRIDES(name) \
    \
    };

////////////////////////////////////////////////////////////////////////////////

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

////////////////////////////////////////////////////////////////////////////////

class TMapNode
    : public TNodeBase
    , public virtual IMapNode
{
public:
    DECLARE_TYPE_OVERRIDES(Map)

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const;
    virtual INode::TConstPtr FindChild(const Stroka& name) const;
    virtual bool AddChild(INode::TPtr child, const Stroka& name);
    virtual bool RemoveChild(const Stroka& name);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

    virtual TNavigateResult YPathNavigate(
        const TYPath& path) const;
    virtual TSetResult YPathSet(
        const TYPath& path,
        TYsonProducer::TPtr producer);

private:
    yhash_map<Stroka, INode::TPtr> NameToChild;
    yhash_map<INode::TPtr, Stroka> ChildToName;

};

////////////////////////////////////////////////////////////////////////////////

class TListNode
    : public TNodeBase
    , public virtual IListNode
{
public:
    DECLARE_TYPE_OVERRIDES(List)

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<INode::TConstPtr> GetChildren() const;
    virtual INode::TConstPtr FindChild(int index) const;
    virtual void AddChild(INode::TPtr child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild);
    virtual void RemoveChild(INode::TPtr child);

    virtual TNavigateResult YPathNavigate(
        const TYPath& path) const;
    virtual TSetResult YPathSet(
        const TYPath& path,
        TYsonProducer::TPtr producer);

private:
    yvector<INode::TConstPtr> List;

    TNavigateResult GetYPathChild(int index, const TYPath& tailPath) const;
    TSetResult CreateYPathChild(int beforeIndex, const TYPath& tailPath);

};

////////////////////////////////////////////////////////////////////////////////

class TEntityNode
    : public TNodeBase
    , public virtual IEntityNode
{
public:
    DECLARE_TYPE_OVERRIDES(Entity)

};

#undef DECLARE_SCALAR_TYPE
#undef DECLARE_TYPE_OVERRIDES

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public INodeFactory
{
public:
    static INodeFactory* Get();

    virtual IStringNode::TPtr CreateString(const Stroka& value = Stroka());
    virtual IInt64Node::TPtr CreateInt64(i64 value = 0);
    virtual IDoubleNode::TPtr CreateDouble(double value = 0);
    virtual IMapNode::TPtr CreateMap();
    virtual IListNode::TPtr CreateList();
    virtual IEntityNode::TPtr CreateEntity();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NEphemeral
} // namespace NYTree
} // namespace NYT

