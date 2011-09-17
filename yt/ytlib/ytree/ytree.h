#pragma once

#include "common.h"
#include "ytree_fwd.h"

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../misc/ptr.h"

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

    virtual ~INode()
    { }

    virtual ENodeType GetType() const = 0;
    
    virtual INode::TPtr AsMutable() const = 0;

    virtual TIntrusiveConstPtr<INode> AsNode() const = 0;
    virtual TIntrusivePtr<INode> AsNode() = 0;

#define DECLARE_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const = 0; \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() = 0;

    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)

#undef DECLARE_AS_METHODS

    virtual TIntrusiveConstPtr<IMapNode> GetAttributes() const = 0;
    virtual void SetAttributes(TIntrusiveConstPtr<IMapNode> attributes) = 0;

    virtual INode::TConstPtr GetParent() const = 0;
    virtual void SetParent(INode::TConstPtr parent) = 0;

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

    // YPath stuff.
    virtual bool NavigateYPath(
        const TYPath& path,
        INode::TConstPtr* tailNode,
        TYPath* tailPath) const = 0;

    virtual void SetYPath(
        INodeFactory* factory,
        const TYPath& path,
        INode::TPtr value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct IScalarNode
    : INode
{
    typedef T TValue;

    virtual TValue GetValue() const = 0;
    virtual void SetValue(const TValue& value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ICompositeNode
    : INode
{
    virtual int GetChildCount() const = 0;
    // TODO: iterators?
};

////////////////////////////////////////////////////////////////////////////////

struct IListNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IListNode> TPtr;
    typedef TIntrusiveConstPtr<IListNode> TConstPtr;

    virtual INode::TConstPtr FindChild(int index) const = 0;
    virtual void AddChild(INode::TPtr node, int beforeIndex = -1) = 0;
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

    virtual yvector<Stroka> GetChildNames() const = 0;
    virtual INode::TConstPtr FindChild(const Stroka& name) const = 0;
    virtual bool AddChild(INode::TPtr node, const Stroka& name) = 0;
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
    : INode
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

template<class IBase>
class TNodeBase
    : public IBase
{
public:
    virtual ENodeType GetType() const = 0;
    
    virtual INode::TPtr AsMutable() const
    {
        return const_cast<TNodeBase*>(this);
    }

    virtual INode::TConstPtr AsNode() const
    {
        return const_cast<TNodeBase*>(this);
    }

    virtual INode::TPtr AsNode()
    {
        return this;
    }

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const \
    { \
        YASSERT(false); \
        return NULL; \
    } \
    \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() \
    { \
        YASSERT(false); \
        return NULL; \
    }

    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)

#undef IMPLEMENT_AS_METHODS

    virtual INode::TConstPtr GetParent() const
    {
        return Parent;
    }

    virtual void SetParent(INode::TConstPtr parent)
    {
        if (~parent == NULL) {
            Parent = NULL;
        } else {
            YASSERT(~Parent == NULL);
            Parent = parent;
        }
    }

    virtual IMapNode::TConstPtr GetAttributes() const
    {
        return Attributes;
    }

    virtual void SetAttributes(IMapNode::TConstPtr attributes)
    {
        if (~Attributes != NULL) {
            Attributes->AsMutable()->SetParent(NULL);
            Attributes = NULL;
        }
        Attributes = attributes;
    }

    virtual bool NavigateYPath(
        const TYPath& path,
        INode::TConstPtr* node,
        TYPath* tailPath) const
    {
        if (!path.empty() && path[0] == '@' && ~Attributes != NULL) {
            *node = Attributes->AsNode();
            *tailPath = TYPath(path.begin() + 1, path.end());
            return true;
        } else {
            *node = NULL;
            *tailPath = TYPath();
            return false;
        }
    }

    virtual void SetYPath(
        INodeFactory* factory,
        const TYPath& path,
        INode::TPtr value)
    {
        UNUSED(factory);
        UNUSED(path);
        UNUSED(value);
        ythrow yexception() << "Cannot create child";
    }

private:
    INode::TConstPtr Parent;
    IMapNode::TConstPtr Attributes;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

