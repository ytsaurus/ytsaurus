#pragma once

#include <ytlib/object_server/id.h>
#include <ytlib/transaction_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

using NObjectServer::TObjectId;
using NObjectServer::NullObjectId;
using NObjectServer::EObjectType;

typedef TObjectId TNodeId;
extern TNodeId NullNodeId;

typedef TObjectId TLockId;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TVersionedNodeId
{
    //! Id of the node itself.
    TNodeId NodeId;

    //! Id of the transaction that had branched the node.
    //! #NullTransactionId if the node is not branched.
    TTransactionId TransactionId;

    //! Initializes a null instance.
    /*!
     *  #NodeId id #NullNodeId, #TransactionId is #NullTransactionId.
     */
    TVersionedNodeId();

    //! Initializes an instance by given node. Sets #TransactionId to #NullTransactionId.
    /*!
     *  Can be used for implicit conversion from TNodeId to TVersionedNodeId.
     */
    TVersionedNodeId(const TNodeId& nodeId);

    //! Initializes an instance by given node and transaction ids.
    TVersionedNodeId(const TNodeId& nodeId, const TTransactionId& transactionId);

    //! Checks that the id is branched, i.e. #TransactionId is not #NullTransactionId.
    bool IsBranched() const;

    //! Formats the id to string (for debugging and logging purposes mainly).
    Stroka ToString() const;

    static TVersionedNodeId FromString(const Stroka &s);
};

//! Compares TVersionedNodeId s for equality.
bool operator == (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs);

//! Compares TVersionedNodeId s for inequality.
bool operator != (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs);

//! Compares TVersionedNodeId s for "less than" (used to sort nodes in meta-map).
bool operator <  (const TVersionedNodeId& lhs, const TVersionedNodeId& rhs);

} // namespace NCypress
} // namespace NYT

DECLARE_PODTYPE(NYT::NCypress::TVersionedNodeId);

//! A hasher for TVersionedNodeId.
template <>
struct hash<NYT::NCypress::TVersionedNodeId>
{
    i32 operator()(const NYT::NCypress::TVersionedNodeId& id) const;
};
