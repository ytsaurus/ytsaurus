#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../ytree/ytree.h"
#include "../ytree/ypath_service.h"
#include "../transaction_server/common.h"

namespace NYT {
namespace NCypress {

// TODO: get rid
using NTransaction::TTransactionId;
using NTransaction::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TBranchedNodeId
{
    //! Id of the node itself.
    TNodeId NodeId;

    //! Id of the transaction that had branched the node.
    //! #NullTransactionId if the node is not branched.
    TTransactionId TransactionId;

    TBranchedNodeId();

    //! Initializes an instance by given node and transaction ids.
    TBranchedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId);

    //! Checks that the id is branched, i.e. #TransactionId is not #NullTransactionId.
    bool IsBranched() const;

    //! Formats the id to string (for debugging and logging purposes mainly).
    Stroka ToString() const;

    static TBranchedNodeId FromString(const Stroka &s);
};

//! Compares TBranchedNodeId s for equality.
bool operator == (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);

//! Compares TBranchedNodeId s for inequality.
bool operator != (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);

//! Compares TBranchedNodeId s for "less than" (used to sort nodes in meta-map).
bool operator <  (const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);

} // namespace NCypress
} // namespace NYT

DECLARE_PODTYPE(NYT::NCypress::TBranchedNodeId);

//! A hasher for TBranchedNodeId.
template <>
struct hash<NYT::NCypress::TBranchedNodeId>
{
    i32 operator()(const NYT::NCypress::TBranchedNodeId& id) const;
};

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//! Describes the state of the persisted node.
DECLARE_ENUM(ENodeState,
    // The node is present in the HEAD version.
    (Committed)
    // The node is a branched copy of another committed node.
    (Branched)
    // The node is created by the transaction and is thus new.
    (Uncommitted)
);

////////////////////////////////////////////////////////////////////////////////

struct ICypressNode;
struct ICypressNodeProxy;

struct INodeBehavior
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<INodeBehavior> TPtr;

    virtual void Destroy() = 0;
};

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

    //! Returns the runtime node.
    virtual ERuntimeNodeType GetRuntimeType() = 0;
    
    //! Returns the (static) node name.
    virtual NYTree::ENodeType GetNodeType() = 0;

    //! Returns the type name.
    /*!
     *  This name is displayed via <tt>type</tt> attribute.
     *  Also when creating a dynamic node the client must specify
     *  <tt>type</tt> attribute in the manifest.
     */
    virtual Stroka GetTypeName() = 0;
    
    //! Creates a dynamic node with a given manifest.
    /*!
     *  This is called during <tt>Create<tt> verb execution.
     */
    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::INode* manifest) = 0;

    //! Create a empty instance of the node.
    /*!
     *  This is called when a node is being loaded from a snapshot.
     */
    virtual TAutoPtr<ICypressNode> Create(
        const TBranchedNodeId& id) = 0;

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


    //! Creates a behavior object that lives as long as the node
    //! exists in Cypress.
    /*!
     *  \note
     *  The callee may return NULL if no behavior is needed.
     *  
     *  Behaviors are only created at leader.
     *  
     *  Behaviors are only created for non-branched nodes.
     *  
     *  The callee must not keep the node reference since node's
     *  content may get eventually destroyed by TMetaStateMap.
     *  Instead it should keep the node id.
     */
    virtual INodeBehavior::TPtr CreateBehavior(const ICypressNode& node) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a common interface for all persistent nodes.
struct ICypressNode
{
    virtual ~ICypressNode()
    { }

    virtual ERuntimeNodeType GetRuntimeType() const = 0;

    virtual TAutoPtr<ICypressNode> Clone() const = 0;

    virtual void Save(TOutputStream* output) const = 0;
    
    virtual void Load(TInputStream* input) = 0;

    //! Returns the id of the node (which is the key in the respective meta-map).
    virtual TBranchedNodeId GetId() const = 0;

    // TODO: propertify
    //! Gets the state of node.
    virtual ENodeState GetState() const = 0;
    //! Sets node state.
    virtual void SetState(const ENodeState& value) = 0;

    //! Gets parent node id.
    virtual TNodeId GetParentId() const = 0;
    //! Sets parent node id.
    virtual void SetParentId(const TNodeId& value) = 0;

    //! Gets attributes node id.
    virtual TNodeId GetAttributesId() const = 0;
    //! Sets attributes node id.
    virtual void SetAttributesId(const TNodeId& value) = 0;

    //! Gets an immutable reference to the node's locks.
    virtual const yhash_set<TLockId>& LockIds() const = 0;
    //! Gets an mutable reference to the node's locks.
    virtual yhash_set<TLockId>& LockIds() = 0;

    //! Increments the reference counter, returns the incremented value.
    virtual int Ref() = 0;
    //! Decrements the reference counter, returns the decremented value.
    virtual int Unref() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
