#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

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

    //! Checks that the id is branched, i.e. #TransactionId it not #NullTransactionId.
    bool IsBranched() const;

    //! Formats the id to string (for debugging and logging purposes mainly).
    Stroka ToString() const;
};

bool operator==(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);
inline bool operator!=(const TBranchedNodeId& lhs, const TBranchedNodeId& rhs);

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

struct INodeTypeHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<INodeTypeHandler> TPtr;

    //! Constructs a proxy.
    /*!
     *  \param transactionId The id of the transaction for which the proxy
     *  is being created, may be #NullTransactionId.
     *  \return The constructed proxy.
     */
    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const ICypressNode& node,
        const TTransactionId& transactionId) = 0;

    virtual ERuntimeNodeType GetRuntimeType() = 0;
    virtual Stroka GetTypeName() = 0;
    
    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode::TPtr description) = 0;

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
    virtual void Merge(
        ICypressNode& committedNode,
        ICypressNode& branchedNode) = 0;

    // TODO: consider returning yvector<Stroka>
    virtual void GetAttributeNames(
        const ICypressNode& node,
        yvector<Stroka>* names) = 0;

    virtual bool GetAttribute(
        const ICypressNode& node,
        const Stroka& name,
        IYsonConsumer* consumer) = 0;
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

    //! Gets node state.
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
    virtual const yhash_set<TLockId>& Locks() const = 0;
    //! Gets an mutable reference to the node's locks.
    virtual yhash_set<TLockId>& Locks() = 0;

    //! Increments the reference counter, returns the incremented value.
    virtual int Ref() = 0;
    //! Decrements the reference counter, returns the decremented value.
    virtual int Unref() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
