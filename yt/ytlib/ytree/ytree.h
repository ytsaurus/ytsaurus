#pragma once

#include "common.h"
#include "ytree_fwd.h"
#include "yson_events.h"

#include "../misc/enum.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct TScalarTypeTraits
{ };

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ENodeType,
    (String)
    (Int64)
    (Double)
    (Map)
    (List)
    (Entity)
);
    
struct INode
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<INode> TPtr;

    virtual ENodeType GetType() const = 0;
    
    virtual INodeFactory* GetFactory() const = 0;

#define DECLARE_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() = 0; \
    virtual TIntrusivePtr<const I##name##Node> As##name() const = 0;

    DECLARE_AS_METHODS(Composite)
    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)

#undef DECLARE_AS_METHODS

    virtual TIntrusivePtr<IMapNode> GetAttributes() const = 0;
    virtual void SetAttributes(TIntrusivePtr<IMapNode> attributes) = 0;

    virtual TIntrusivePtr<ICompositeNode> GetParent() const = 0;
    virtual void SetParent(TIntrusivePtr<ICompositeNode> parent) = 0;

    template<class T>
    T GetValue() const
    {
        return TScalarTypeTraits<T>::GetValue(this);
    }

    template<class T>
    void SetValue(typename TScalarTypeTraits<T>::TParamType value)
    {
        TScalarTypeTraits<T>::SetValue(this, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct IScalarNode
    : virtual INode
{
    typedef T TValue;
    typedef TIntrusivePtr< IScalarNode<T> > TPtr;

    virtual TValue GetValue() const = 0;
    virtual void SetValue(const TValue& value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICompositeNode
    : virtual INode
{
    typedef TIntrusivePtr<ICompositeNode> TPtr;

    virtual void Clear() = 0;
    virtual int GetChildCount() const = 0;
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild) = 0;
    virtual void RemoveChild(INode::TPtr child) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IListNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IListNode> TPtr;

    using ICompositeNode::RemoveChild;

    virtual yvector<INode::TPtr> GetChildren() const = 0;
    virtual INode::TPtr FindChild(int index) const = 0;
    virtual void AddChild(INode::TPtr child, int beforeIndex = -1) = 0;
    virtual bool RemoveChild(int index) = 0;

    INode::TPtr GetChild(int index) const
    {
        auto child = FindChild(index);
        YASSERT(~child != NULL);
        return child;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IMapNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IMapNode> TPtr;

    using ICompositeNode::RemoveChild;

    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const = 0;
    virtual INode::TPtr FindChild(const Stroka& name) const = 0;
    virtual bool AddChild(INode::TPtr child, const Stroka& name) = 0;
    virtual bool RemoveChild(const Stroka& name) = 0;

    INode::TPtr GetChild(const Stroka& name) const
    {
        auto child = FindChild(name);
        YASSERT(~child != NULL);
        return child;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IEntityNode
    : virtual INode
{
    typedef TIntrusivePtr<IEntityNode> TPtr;
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(name, type, paramType) \
    struct I##name##Node \
        : IScalarNode<type> \
    { \
        typedef TIntrusivePtr<I##name##Node> TPtr; \
    }; \
    \
    template<> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I##name##Node TNode; \
        typedef paramType TParamType; \
        \
        static type GetValue(const INode* node) \
        { \
            return node->As##name()->GetValue(); \
        } \
        \
        static void SetValue(INode* node, TParamType value) \
        { \
            node->As##name()->SetValue(value); \
        } \
    };

DECLARE_SCALAR_TYPE(String, Stroka, const Stroka&)
DECLARE_SCALAR_TYPE(Int64, i64, i64)
DECLARE_SCALAR_TYPE(Double, double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

struct INodeFactory
{
    virtual ~INodeFactory()
    { }

    virtual IStringNode::TPtr CreateString() = 0;
    virtual IInt64Node::TPtr CreateInt64() = 0;
    virtual IDoubleNode::TPtr CreateDouble() = 0;
    virtual IMapNode::TPtr CreateMap() = 0;
    virtual IListNode::TPtr CreateList() = 0;
    virtual IEntityNode::TPtr CreateEntity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

