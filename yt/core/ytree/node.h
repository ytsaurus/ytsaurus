#pragma once

#include "public.h"
#include "attribute_owner.h"
#include "attribute_provider.h"
#include "ypath_service.h"

#include <core/yson/public.h>

#include <core/misc/mpl.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
struct TScalarTypeTraits
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Resolves YPaths into nodes and vice versa.
struct INodeResolver
    : public virtual TRefCounted
{
    //! Returns a node corresponding to a given path.
    //! Throws if resolution fails.
    virtual INodePtr ResolvePath(const TYPath& path) = 0;

    //! Returns a path for a given node.
    virtual TYPath GetPath(INodePtr node) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeResolver)

////////////////////////////////////////////////////////////////////////////////

//! A base DOM-like interface representing a node.
struct INode
    : public virtual IYPathService
    , public virtual IAttributeOwner
{
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
     *  temporary references to newly created nodes.
     *  \see INodeFactory::Commit
     */
    virtual INodeFactoryPtr CreateFactory() const = 0;

    //! Returns the resolver associated with the tree.
    virtual INodeResolverPtr GetResolver() const = 0;

    // A bunch of "AsSomething" methods that return a pointer
    // to the same node but typed as "Something".
    // These methods throw an exception on type mismatch.
#define DECLARE_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() = 0; \
    virtual TIntrusivePtr<const I##name##Node> As##name() const = 0;

    DECLARE_AS_METHODS(Entity)
    DECLARE_AS_METHODS(Composite)
    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Uint64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(Boolean)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)
#undef DECLARE_AS_METHODS

    //! Returns the parent of the node.
    //! |nullptr| indicates that the current node is the root.
    virtual ICompositeNodePtr GetParent() const = 0;
    
    //! Sets the parent of the node.
    /*!
     *  This method is called automatically when one subtree (possibly)
     *  consisting of a single node is attached to another.
     *
     *  This method must not be called explicitly.
     */
    virtual void SetParent(ICompositeNodePtr parent) = 0;

    //! A helper method for retrieving a scalar value from a node.
    //! Invokes the appropriate |AsSomething| followed by |GetValue|.
    template <class T>
    typename NMpl::TCallTraits<T>::TType GetValue() const
    {
        return NDetail::TScalarTypeTraits<T>::GetValue(this);
    }

    //! A helper method for assigning a scalar value to a node.
    //! Invokes the appropriate |AsSomething| followed by |SetValue|.
    template <class T>
    void SetValue(typename NMpl::TCallTraits<T>::TType value)
    {
        NDetail::TScalarTypeTraits<T>::SetValue(this, value);
    }

    //! A shortcut for |node->GetResolver()->GetPath(node)|.
    TYPath GetPath() const;
};

DEFINE_REFCOUNTED_TYPE(INode)

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all scalar nodes, i.e. nodes containing a single atomic value.
template <class T>
struct IScalarNode
    : public virtual INode
{
    typedef T TValue;

    //! Gets the value.
    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const = 0;

    //! Sets the value.
    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) = 0;
};

// Define the actual scalar node types: IStringNode, IInt64Node, IUint64Node, IDoubleNode, IBooleanNode.
#define DECLARE_SCALAR_TYPE(name, type) \
    struct I##name##Node \
        : IScalarNode<type> \
    { }; \
    \
    DEFINE_REFCOUNTED_TYPE(I##name##Node) \
    \
    namespace NDetail { \
    \
    template <> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I##name##Node TNode; \
        typedef type TType; \
        typedef NMpl::TConditional< \
            NMpl::TIsSame<type, Stroka>::Value, \
            /* if-true  */ const TStringBuf&, \
            /* if-false */ type \
        >::TType TConsumerType; \
        \
        static const ENodeType NodeType; \
        \
        static NMpl::TCallTraits<type>::TType GetValue(const IConstNodePtr& node) \
        { \
            return node->As##name()->GetValue(); \
        } \
        \
        static void SetValue(const INodePtr& node, NMpl::TCallTraits<type>::TType value) \
        { \
            node->As##name()->SetValue(value); \
        } \
    }; \
    \
    }

// Don't forget to define a #TScalarTypeTraits<>::NodeType constant in "node.cpp".
DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Uint64, ui64)
DECLARE_SCALAR_TYPE(Double, double)
DECLARE_SCALAR_TYPE(Boolean, bool)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all composite nodes, i.e. nodes containing other nodes.
struct ICompositeNode
    : public virtual INode
{
    //! Removes all child nodes.
    virtual void Clear() = 0;

    //! Returns the number of child nodes.
    virtual int GetChildCount() const = 0;

    //! Replaces one child by the other.
    //! #newChild must be a root.
    virtual void ReplaceChild(INodePtr oldChild, INodePtr newChild) = 0;

    //! Removes a child.
    //! The removed child becomes a root.
    virtual void RemoveChild(INodePtr child) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompositeNode)

////////////////////////////////////////////////////////////////////////////////

//! A map node, which keeps a dictionary mapping strings (Stroka) to child nodes.
struct IMapNode
    : public virtual ICompositeNode
{
    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the map.
    /*!
     *  Map items are returned in unspecified order.
     */
    virtual std::vector< std::pair<Stroka, INodePtr> > GetChildren() const = 0;

    //! Returns map keys.
    /*!
     *  Keys are returned in unspecified order.
     */
    virtual std::vector<Stroka> GetKeys() const = 0;

    //! Gets a child by its key.
    /*!
     *  \param key A key.
     *  \return A child with the given key or NULL if the index is not valid.
     */
    virtual INodePtr FindChild(const Stroka& key) const = 0;

    //! Adds a new child with a given key.
    /*!
     *  \param child A child.
     *  \param key A key.
     *  \return True iff the key was not in the map already and thus the child is inserted.
     *
     *  \note
     *  #child must be a root.
     */
    virtual bool AddChild(INodePtr child, const Stroka& key) = 0;

    //! Removes a child by its key.
    /*!
     *  \param key A key.
     *  \return True iff there was a child with the given key.
     */
    virtual bool RemoveChild(const Stroka& key) = 0;

    //! Similar to #FindChild but throws if no child is found.
    INodePtr GetChild(const Stroka& key) const;

    //! Returns the key for a given child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's key.
     */
    virtual Stroka GetChildKey(IConstNodePtr child) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMapNode)

////////////////////////////////////////////////////////////////////////////////

//! A list node, which keeps a list (vector) of children.
struct IListNode
    : public virtual ICompositeNode
{
    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the list.
    virtual std::vector<INodePtr> GetChildren() const = 0;

    //! Gets a child by its index.
    /*!
     *  \param index An index.
     *  \return A child with the given index or NULL if the index is not valid.
     */
    virtual INodePtr FindChild(int index) const = 0;

    //! Adds a new child at a given position.
    /*!
     *  \param child A child.
     *  \param beforeIndex A position before which the insertion must happen.
     *  -1 indicates the end of the list.
     *
     *  \note
     *  #child must be a root.
     */

    virtual void AddChild(INodePtr child, int beforeIndex = -1) = 0;

    //! Removes a child by its index.
    /*!
     *  \param index An index.
     *  \return True iff the index is valid and thus the child is removed.
     */
    virtual bool RemoveChild(int index) = 0;

    //! Similar to #FindChild but fails if the index is not valid.
    INodePtr GetChild(int index) const;

    //! Returns the index for a given child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's index.
     */
    virtual int GetChildIndex(IConstNodePtr child) = 0;

    //! Normalizes negative indexes (by adding child count).
    //! Throws if the index is invalid.
    /*!
     *  \param index Original (possibly negative) index.
     *  \returns Adjusted (valid non-negative) index.
     */
    int AdjustChildIndex(int index) const;

};

DEFINE_REFCOUNTED_TYPE(IListNode)

////////////////////////////////////////////////////////////////////////////////

//! An structureless entity node.
struct IEntityNode
    : public virtual INode
{ };

DEFINE_REFCOUNTED_TYPE(IEntityNode)

////////////////////////////////////////////////////////////////////////////////

//! A factory for creating nodes.
/*!
 *  All freshly created nodes are roots, i.e. have no parent.
 *  
 *  The factory also acts as a context that holds all created nodes.
 *  One must call #Commit at the end if the operation was a success.
 *  If the operation failed, one must just release the reference to the factory.
 *  Any needed rollback will be carried out automagically.
 *
 */
struct INodeFactory
    : public virtual TRefCounted
{
    //! Creates a string node.
    virtual IStringNodePtr CreateString() = 0;

    //! Creates an int64 node.
    virtual IInt64NodePtr CreateInt64() = 0;

    //! Creates an uint64 node.
    virtual IUint64NodePtr CreateUint64() = 0;

    //! Creates an FP number node.
    virtual IDoubleNodePtr CreateDouble() = 0;

    //! Creates an boolean node.
    virtual IBooleanNodePtr CreateBoolean() = 0;

    //! Creates a map node.
    virtual IMapNodePtr CreateMap() = 0;

    //! Creates a list node.
    virtual IListNodePtr CreateList() = 0;

    //! Creates an entity node.
    virtual IEntityNodePtr CreateEntity() = 0;

    //! Called before releasing the factory to indicate that all created nodes
    //! must persist.
    virtual void Commit() = 0;

};

DEFINE_REFCOUNTED_TYPE(INodeFactory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(INode& value, NYson::IYsonConsumer* consumer);
void Deserialize(INodePtr& value, INodePtr node);

TYsonString ConvertToYsonStringStable(INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

