#pragma once

#include "id.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NCypress {

struct ICypressNode;
struct ICypressNodeProxy;

////////////////////////////////////////////////////////////////////////////////

//! Describes a behavior object that lives as long as the node
//! exists in Cypress.
/*!
 *  \note
 *  Behaviors are only created at leaders.
 *  Behaviors are only created for non-branched nodes.
 */
struct INodeBehavior
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<INodeBehavior> TPtr;

    //! Called when the node owning the behavior object is about to
    //! be destroyed.
    virtual void Destroy() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides node type-specific services.
struct INodeTypeHandler
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<INodeTypeHandler> TPtr;

    //! Constructs a proxy.
    /*!
     *  \param transactionId The id of the transaction for which the proxy
     *  is being created (possibly #NullTransactionId).
     *  \return The constructed proxy.
     */
    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId) = 0;

    //! Returns the (dynamic) node type.
    virtual EObjectType GetObjectType() = 0;
    
    //! Returns the (static) node type.
    virtual NYTree::ENodeType GetNodeType() = 0;

    //! Creates a dynamic node with a given manifest.
    /*!
     *  This is called during <tt>Create<tt> verb execution.
     */
    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::IMapNode* manifest) = 0;

    //! Create a empty instance of the node.
    /*!
     *  This method is called when:
     *  - a static node is being created
     *  - a node (possibly dynamic) is being loaded from a snapshot
     */
    virtual TAutoPtr<ICypressNode> Create(
        const TVersionedNodeId& id) = 0;

    //! Performs cleanup on node destruction.
    /*!
     *  This is called prior to the actual removal of the node from the meta-map.
     *  A typical implementation will release the resources held by the node,
     *  decrement the ref-counters of the children etc.
     *  
     *  \note This method is only called for committed and uncommitted nodes.
     *  It is not called for branched ones.
     */
    virtual void Destroy(ICypressNode& node) = 0;

    //! Branches a committed node into a given transaction.
    /*!
     *  \param transactionId The id of the transaction that is about to
     *  modify the node.
     *  \return A branched node.
     */
    virtual TAutoPtr<ICypressNode> Branch(
        const ICypressNode& node,
        const TTransactionId& transactionId) = 0;

    //! Merges the changes made in the branched node back into the committed one.
    /*!
     *  \param branchedNode A branched node.
     *
     *  \note 
     *  #branchedNode is non-const for performance reasons (i.e. to swap the data instead of copying).
     */
    // TODO: RV-ref?
    virtual void Merge(
        ICypressNode& committedNode,
        ICypressNode& branchedNode) = 0;

    // TODO: consider returning yvector<Stroka>
    virtual void GetAttributeNames(
        const ICypressNode& node,
        yvector<Stroka>* names) = 0;

    virtual NYTree::IYPathService::TPtr GetAttributeService(
        const ICypressNode& node,
        const Stroka& name) = 0;


    //! Creates a behavior object.
    /*!
     *  \note
     *  The method may return NULL if no behavior is needed.
     *  
     *  The implementation must not keep the node reference since node's
     *  content may get eventually destroyed by TMetaStateMap.
     *  It should keep node id instead.
     */
    virtual INodeBehavior::TPtr CreateBehavior(const ICypressNode& node) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
