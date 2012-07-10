#pragma once

#include "public.h"
#include "attribute_provider.h"
#include "ypath_service.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
struct TScalarTypeTraits
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! A base DOM-like interface representing a node.
struct INode
    : public virtual IYPathService
    , public virtual IAttributeProvider
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
     *  Each created node must be somehow attached to the tree before
     *  the factory dies. Otherwise the node also gets disposed.
     */
    virtual INodeFactoryPtr CreateFactory() const = 0;

    // A bunch of "AsSomething" methods that return a pointer
    // to the same node but typed as "Something".
    // These methods throw an exception on type mismatch.
#define DECLARE_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() = 0; \
    virtual TIntrusivePtr<const I##name##Node> As##name() const = 0;

    DECLARE_AS_METHODS(Composite)
    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Integer)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)
#undef DECLARE_AS_METHODS

    //! Returns the parent of the node.
    //! NULL indicates that the current node is the root.
    virtual ICompositeNodePtr GetParent() const = 0;
    //! Sets the parent of the node.
    /*!
     *  This method is called automatically when one subtree (possibly)
     *  consisting of a single node is attached to another.
     *  
     *  This method must not be called explicitly.
     */
    virtual void SetParent(ICompositeNode* parent) = 0;

    //! A helper method for retrieving a scalar value from a node.
    //! Invokes the appropriate |AsSomething| followed by |GetValue|.
    template <class T>
    T GetValue() const
    {
        return NDetail::TScalarTypeTraits<T>::GetValue(this);
    }

    //! A helper method for assigning a scalar value to a node.
    //! Invokes the appropriate |AsSomething| followed by |SetValue|.
    template <class T>
    void SetValue(typename NDetail::TScalarTypeTraits<T>::TParamType value)
    {
        NDetail::TScalarTypeTraits<T>::SetValue(this, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all scalar nodes, i.e. nodes containing a single atomic value.
template <class T>
struct IScalarNode
    : public virtual INode
{
    typedef T TValue;

    //! Gets the value.
    virtual TValue GetValue() const = 0;
    //! Sets the value.
    virtual void SetValue(const TValue& value) = 0;
};

// Define the actual scalar node types: IStringNode, IIntegerNode, IDoubleNode.
#define DECLARE_SCALAR_TYPE(name, type, paramType) \
    struct I##name##Node \
        : IScalarNode<type> \
    { }; \
    \
    namespace NDetail { \
    \
    template<> \
    struct TScalarTypeTraits<type> \
    { \
        typedef I##name##Node TNode; \
        typedef type TType; \
        typedef paramType TParamType; \
        \
        static const ENodeType::EDomain NodeType; \
        \
        static type GetValue(const INode* node) \
        { \
            return node->As##name()->GetValue(); \
        } \
        \
        static void SetValue(INode* node, TParamType value) \
        { \
            node->As##name()->SetValue(TType(value)); \
        } \
    }; \
    \
    }

// Don't forget to define a #TScalarTypeTraits<>::NodeType constant in "ytree.cpp".
DECLARE_SCALAR_TYPE(String, Stroka, const TStringBuf&)
DECLARE_SCALAR_TYPE(Integer, i64, i64)
DECLARE_SCALAR_TYPE(Double, double, double)

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
    virtual void ReplaceChild(INode* oldChild, INode* newChild) = 0;
    //! Removes a child.
    //! The removed child becomes a root.
    virtual void RemoveChild(INode* child) = 0;
};

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
    virtual std::vector< TPair<Stroka, INodePtr> > GetChildren() const = 0;

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
    virtual INodePtr FindChild(const TStringBuf& key) const = 0;

    //! Adds a new child with a given key.
    /*!
     *  \param child A child.
     *  \param key A key.
     *  \return True iff the key was not in the map already and thus the child is inserted.
     *  
     *  \note
     *  #child must be a root.
     */
    virtual bool AddChild(INode* child, const TStringBuf& key) = 0;

    //! Removes a child by its key.
    /*!
     *  \param key A key.
     *  \return True iff there was a child with the given key.
     */
    virtual bool RemoveChild(const TStringBuf& key) = 0;

    //! Similar to #FindChild but fails if no child is found.
    INodePtr GetChild(const Stroka& key) const
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
    virtual Stroka GetChildKey(const INode* child) = 0;
};

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

    virtual void AddChild(INode* child, int beforeIndex = -1) = 0;
    
    //! Removes a child by its index.
    /*!
     *  \param index An index.
     *  \return True iff the index is valid and thus the child is removed.
     */
    virtual bool RemoveChild(int index) = 0;

    //! Similar to #FindChild but fails if the index is not valid.
    INodePtr GetChild(int index) const
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
    virtual int GetChildIndex(const INode* child) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! An structureless entity node.
struct IEntityNode
    : public virtual INode
{ };

////////////////////////////////////////////////////////////////////////////////

//! A factory for creating nodes.
/*!
 *  All freshly created nodes are roots, i.e. have no parent.
 */
struct INodeFactory
    : public virtual TRefCounted
{
    //! Creates a string node.
    virtual IStringNodePtr CreateString() = 0;
    //! Creates an integer node.
    virtual IIntegerNodePtr CreateInteger() = 0;
    //! Creates an FP number node.
    virtual IDoubleNodePtr CreateDouble() = 0;
    //! Creates a map node.
    virtual IMapNodePtr CreateMap() = 0;
    //! Creates a list node.
    virtual IListNodePtr CreateList() = 0;
    //! Creates an entity node.
    virtual IEntityNodePtr CreateEntity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

