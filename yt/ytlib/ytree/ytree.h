#pragma once

#include "common.h"
#include "ytree_fwd.h"
#include "yson_events.h"

//#include "../actions/action.h"
#include "../misc/enum.h"
//#include "../misc/ptr.h"

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
    typedef TIntrusiveConstPtr<INode> TConstPtr;

    virtual ENodeType GetType() const = 0;
    
    virtual INodeFactory* GetFactory() const = 0;

    virtual INode::TPtr AsMutable() const = 0;
    virtual INode::TConstPtr AsImmutable() const = 0;

#define DECLARE_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const = 0; \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() = 0;

    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(Entity)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)

#undef DECLARE_AS_METHODS

    virtual TIntrusiveConstPtr<IMapNode> GetAttributes() const = 0;
    virtual void SetAttributes(TIntrusivePtr<IMapNode> attributes) = 0;

    virtual TIntrusiveConstPtr<ICompositeNode> GetParent() const = 0;
    virtual void SetParent(TIntrusivePtr<ICompositeNode> parent) = 0;

    template<class T>
    T GetValue() const
    {
        return TScalarTypeTraits<T>::GetValue(this);
    }

    template<class T>
    void SetValue(const T& value)
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

    virtual TValue GetValue() const = 0;
    virtual void SetValue(const TValue& value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICompositeNode
    : virtual INode
{
    typedef TIntrusivePtr<ICompositeNode> TPtr;
    typedef TIntrusiveConstPtr<ICompositeNode> TConstPtr;

    virtual void Clear() = 0;
    virtual int GetChildCount() const = 0;
    virtual void ReplaceChild(INode::TPtr oldChild, INode::TPtr newChild) = 0;
    virtual void RemoveChild(INode::TPtr child) = 0;
    // TODO: iterators?
};

////////////////////////////////////////////////////////////////////////////////

struct IListNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IListNode> TPtr;
    typedef TIntrusiveConstPtr<IListNode> TConstPtr;

    virtual yvector<INode::TConstPtr> GetChildren() const = 0;
    virtual INode::TConstPtr FindChild(int index) const = 0;
    virtual void AddChild(INode::TPtr child, int beforeIndex = -1) = 0;
    virtual bool RemoveChild(int index) = 0;

    INode::TConstPtr GetChild(int index) const
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
    typedef TIntrusiveConstPtr<IMapNode> TConstPtr;

    virtual yvector< TPair<Stroka, INode::TConstPtr> > GetChildren() const = 0;
    virtual INode::TConstPtr FindChild(const Stroka& name) const = 0;
    virtual bool AddChild(INode::TPtr child, const Stroka& name) = 0;
    virtual bool RemoveChild(const Stroka& name) = 0;

    INode::TConstPtr GetChild(const Stroka& name) const
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
    typedef TIntrusiveConstPtr<IEntityNode> TConstPtr;
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(name, type) \
    struct I ## name ## Node \
        : IScalarNode<type> \
    { \
        typedef TIntrusivePtr<I ## name ## Node> TPtr; \
        typedef TIntrusiveConstPtr<I ## name ## Node> TConstPtr; \
    }; \
    \
    template<> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I ## name ## Node TNode; \
        \
        static type GetValue(const INode* tailNode) \
        { \
            return tailNode->As ## name()->GetValue(); \
        } \
        \
        static void SetValue(INode* tailNode, const type& value) \
        { \
            tailNode->As ## name()->SetValue(value); \
        } \
    };

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

struct INodeFactory
{
    virtual ~INodeFactory()
    { }

    virtual IStringNode::TPtr CreateString(const Stroka& value = Stroka()) = 0;
    virtual IInt64Node::TPtr CreateInt64(i64 value = 0) = 0;
    virtual IDoubleNode::TPtr CreateDouble(double value = 0) = 0;
    virtual IMapNode::TPtr CreateMap() = 0;
    virtual IListNode::TPtr CreateList() = 0;
    virtual IEntityNode::TPtr CreateEntity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

