#pragma once

#include "common.h"
#include "ytree_fwd.h"
#include "ypath_service.h"
#include "yson_consumer.h"

#include <ytlib/misc/enum.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template<class T>
struct TScalarTypeTraits
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! A static node type.
DECLARE_ENUM(ENodeType,
    // Node contains a string (Stroka).
    (String)
    // Node contains an integer number (i64).
    (Int64)
    // Node contains an FP number (double).
    (Double)
    // Node contains a map from strings to other nodes.
    (Map)
    // Node contains a list (vector) of other nodes.
    (List)
    // Node is atomic, i.e. has no visible properties (aside from attributes).
    (Entity)
);
    
//! A base DOM-like interface representing a node.
struct INode
    : public virtual IYPathService
{
    typedef TIntrusivePtr<INode> TPtr;

    //! Returns the static type of the node.
    virtual ENodeType GetType() const = 0;
    
    //! Returns a factory for creating new nodes.
    /*!
     *  Every YTree implementation provides its own set of
     *  node implementations. E.g., for an ephemeral implementation
     *  this factory creates ephemeral nodes while for
     *  a persistent implementation (see Cypress) this factory
     *  creates persistent nodes.
     *  
     *  Note that each call may produce a new factory instance.
     *  This is used in Cypress where the factory instance acts as a container holding
     *  temporary referencing to newly created nodes.
     *  Each created node must be somehow attached to the tree before
     *  the factory dies. Otherwise the node also gets disposed.
     */
    virtual TIntrusivePtr<INodeFactory> CreateFactory() const = 0;

    // A bunch of "AsSomething" methods that return a pointer
    // to the same node but typed as "Something".
    // These methods throw an exception on type mismatch.
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

    //! Returns the parent of the node.
    //! NULL indicates that the current node is the root.
    virtual TIntrusivePtr<ICompositeNode> GetParent() const = 0;
    //! Sets the parent of the node.
    /*!
     *  This method is called automatically when one subtree (possibly)
     *  consisting of a single node is attached to another.
     *  
     *  This method must not be called explicitly.
     */
    virtual void SetParent(ICompositeNode* parent) = 0;

    //! A helper method for retrieving a scalar value from a node.
    //! Invokes an appropriate "AsSomething" call followed by "GetValue".
    template<class T>
    T GetValue() const
    {
        return NDetail::TScalarTypeTraits<T>::GetValue(this);
    }

    //! A helper method for assigning a scalar value to a node.
    //! Invokes an appropriate "AsSomething" call followed by "SetValue".
    template<class T>
    void SetValue(typename NDetail::TScalarTypeTraits<T>::TParamType value)
    {
        NDetail::TScalarTypeTraits<T>::SetValue(this, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all scalar nodes, i.e. nodes containing a single atomic value.
template<class T>
struct IScalarNode
    : virtual INode
{
    typedef T TValue;
    typedef TIntrusivePtr< IScalarNode<T> > TPtr;

    //! Gets the values.
    virtual TValue GetValue() const = 0;
    //! Sets the value.
    virtual void SetValue(const TValue& value) = 0;
};


// Define the actual scalar node types: IStringNode, IInt64Node, IDoubleNode.
#define DECLARE_SCALAR_TYPE(name, type, paramType) \
    struct I##name##Node \
        : IScalarNode<type> \
    { \
        typedef TIntrusivePtr<I##name##Node> TPtr; \
    }; \
    \
    namespace NDetail { \
    \
    template<> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I##name##Node TNode; \
        typedef paramType TParamType; \
        \
        static const ENodeType::EDomain NodeType = ENodeType::name; \
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
    }; \
    \
    }

DECLARE_SCALAR_TYPE(String, Stroka, const Stroka&)
DECLARE_SCALAR_TYPE(Int64, i64, i64)
DECLARE_SCALAR_TYPE(Double, double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all composite nodes, i.e. nodes containing other nodes.
struct ICompositeNode
    : public virtual INode
{
    typedef TIntrusivePtr<ICompositeNode> TPtr;

    //! Removes all child nodes.
    virtual void Clear() = 0;
    //! Returns the number of child nodes.
    virtual int GetChildCount() const = 0;
    //! Replaces one child by the other.
    //! #newChild must be a root.
    virtual void ReplaceChild(INode* oldChild, INode* newChild) = 0;
    //! Removes a child.
    //! The removed child becomes a root.
    virtual void RemoveChild(INode* child) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! A map node, which keeps a dictionary mapping strings (Stroka) to child nodes.
struct IMapNode
    : public ICompositeNode
{
    typedef TIntrusivePtr<IMapNode> TPtr;

    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the map.
    /*!
     *  Map items are returned in unspecified order.
     */
    virtual yvector< TPair<Stroka, INode::TPtr> > GetChildren() const = 0;
    //! Gets a child by its key.
    /*!
     *  \param key A key.
     *  \return A child with the given key or NULL if the index is not valid.
     */
    virtual INode::TPtr FindChild(const Stroka& key) const = 0;
    //! Adds a new child with a given key.
    /*!
     *  \param child A child.
     *  \param key A key.
     *  \return True iff the key was not in the map already and thus the child is inserted.
     *  
     *  \note
     *  #child must be a root.
     */
    virtual bool AddChild(INode* child, const Stroka& key) = 0;
    //! Removes a child by its key.
    /*!
     *  \param key A key.
     *  \return True iff there was a child with the given key.
     */
    virtual bool RemoveChild(const Stroka& key) = 0;

    //! Similar to #FindChild but fails if no child is found.
    INode::TPtr GetChild(const Stroka& key) const
    {
        auto child = FindChild(key);
        YASSERT(child);
        return child;
    }

    //! Returns the key for a given child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's key.
     */
    virtual Stroka GetChildKey(INode* child) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! A list node, which keeps a list (vector) of children.
struct IListNode
    : ICompositeNode
{
    typedef TIntrusivePtr<IListNode> TPtr;

    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the list.
    virtual yvector<INode::TPtr> GetChildren() const = 0;
    //! Gets a child by its index.
    /*!
     *  \param index An index.
     *  \return A child with the given index or NULL if the index is not valid.
     */
    virtual INode::TPtr FindChild(int index) const = 0;
    //! Adds a new child at a given position.
    /*!
     *  \param child A child.
     *  \param beforeIndex A position before which the insertion must happen.
     *  -1 indicates the end of the list.
     *  
     *  \note
     *  #child must be a root.
     */
    virtual void AddChild(INode* child, int beforeIndex = -1) = 0;
    //! Removes a child by its index.
    /*!
     *  \param index An index.
     *  \return True iff the index is valid and thus the child is removed.
     */
    virtual bool RemoveChild(int index) = 0;

    //! Similar to #FindChild but fails if the index is not valid.
    INode::TPtr GetChild(int index) const
    {
        auto child = FindChild(index);
        YASSERT(child);
        return child;
    }

    //! Returns the index for a given child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's index.
     */
    virtual int GetChildIndex(INode* child) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! An structureless entity node.
struct IEntityNode
    : virtual INode
{
    typedef TIntrusivePtr<IEntityNode> TPtr;
};

////////////////////////////////////////////////////////////////////////////////

//! A factory for creating nodes.
/*!
 *  All freshly created nodes are roots, i.e. have no parent.
 */
struct INodeFactory
    : virtual TRefCounted
{
    typedef TIntrusivePtr<INodeFactory> TPtr;

    //! Creates a string node.
    virtual IStringNode::TPtr CreateString() = 0;
    //! Creates an integer node.
    virtual IInt64Node::TPtr CreateInt64() = 0;
    //! Creates an FP number node.
    virtual IDoubleNode::TPtr CreateDouble() = 0;
    //! Creates a map node.
    virtual IMapNode::TPtr CreateMap() = 0;
    //! Creates a list node.
    virtual IListNode::TPtr CreateList() = 0;
    //! Creates an entity node.
    virtual IEntityNode::TPtr CreateEntity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

