#pragma once

#include "public.h"

#include <ytlib/misc/enum.h>
#include <ytlib/misc/guid.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////
    
//! Provides a globally unique identifier for an object.
/*!
 *  TGuid consists of four 32-bit parts.
 *  For TObjectId, these parts have the following meaning:
 *  
 *  Part 0: some hash
 *  Part 1: bits 0..15:  object type
 *          bits 16..31: cell id
 *  Part 2: the lower  part of 64-bit sequential counter
 *  Part 3: the higher part of 64-bit sequential counter
 *
 */
typedef TGuid TObjectId;

//! The all-zero id used to denote a non-existing object.
extern TObjectId NullObjectId;

//! Identifies a particular installation of YT.
//! Must be unique to prevent object ids from colliding.
typedef ui16 TCellId;

//! Describes the runtime type of an object.
DECLARE_ENUM(EObjectType,
    // Does not represent any actual type.
    ((Undefined)(0))

    // The following are unversioned objects.
    // These must be created by sending TTransactionYPathProxy::CreateObject to a transaction.
    // Except for EObjectType::Transaction, the latter transaction cannot be null.
    
    // Transaction Manager stuff
    // Top-level transactions are created by sending CreateObject request to RootTransactionPath
    // (which is effectively represents NullTransactionId).
    // Nested transactions are created by sending CreateObject request to their parents.
    ((Transaction)(1))

    // Chunk Manager stuff
    ((Chunk)(100))
    ((ChunkList)(101))

    // For internal use, don't create yourself.
    ((Lock)(200))

    // The following are versioned objects AKA Cypress nodes.
    // These must be created by calling TCypressYPathProxy::Create.

    // Static nodes
    ((StringNode)(300))
    ((IntegerNode)(301))
    ((DoubleNode)(302))
    ((MapNode)(303))
    ((ListNode)(304))

    // Dynamic nodes
    ((File)(400))
    ((Table)(401))
    ((ChunkMap)(402))
    ((LostChunkMap)(403))
    ((OverreplicatedChunkMap)(404))
    ((UnderreplicatedChunkMap)(405))
    ((ChunkListMap)(406))
    ((TransactionMap)(407))
    ((NodeMap)(408))
    ((LockMap)(409))
    ((Holder)(410))
    ((HolderMap)(411))
    ((Orchid)(412))
);

//! Valid types are supposed to be in range [0, MaxObjectType - 1].
/*!
 *  \note
 *  An upper bound will do.
 */
const int MaxObjectType = 1 << 16;

//! Extracts the type component from an id.
EObjectType TypeFromId(const TObjectId& id);

//! Constructs the id from its parts.
TObjectId CreateId(
    EObjectType type,
    TCellId cellId,
    ui64 counter);

////////////////////////////////////////////////////////////////////////////////

typedef TObjectId TTransactionId;
extern TTransactionId NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

template<class T>
TObjectId GetObjectId(T* object) {
    return object ? object->GetId() : NullObjectId;
}

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TVersionedObjectId
{
    //! Id of the node itself.
    TObjectId ObjectId;

    //! Id of the transaction that had branched the node.
    //! #NullTransactionId if the node is not branched.
    TTransactionId TransactionId;

    //! Initializes a null instance.
    /*!
     *  #NodeId id #NullObjectId, #TransactionId is #NullTransactionId.
     */
    TVersionedObjectId();

    //! Initializes an instance by given node. Sets #TransactionId to #NullTransactionId.
    /*!
     *  Can be used for implicit conversion from TNodeId to TVersionedNodeId.
     */
    TVersionedObjectId(const TObjectId& objectId);

    //! Initializes an instance by given node and transaction ids.
    TVersionedObjectId(const TObjectId& objectId, const TTransactionId& transactionId);

    //! Checks that the id is branched, i.e. #TransactionId is not #NullTransactionId.
    bool IsBranched() const;

    //! Formats the id to string (for debugging and logging purposes mainly).
    Stroka ToString() const;

    static TVersionedObjectId FromString(const Stroka& str);
};

//! Compares TVersionedNodeId s for equality.
bool operator == (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs);

//! Compares TVersionedNodeId s for inequality.
bool operator != (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs);

//! Compares TVersionedNodeId s for "less than" (used to sort nodes in meta-map).
bool operator <  (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs);

} // namespace NObjectServer
} // namespace NYT

DECLARE_PODTYPE(NYT::NObjectServer::TVersionedObjectId);

//! A hasher for TVersionedNodeId.
template <>
struct hash<NYT::NObjectServer::TVersionedObjectId>
{
    i32 operator() (const NYT::NObjectServer::TVersionedObjectId& id) const
    {
        return
            (i32) THash<NYT::TGuid>()(id.TransactionId) * 497 +
            (i32) THash<NYT::TGuid>()(id.ObjectId);
    }
};

////////////////////////////////////////////////////////////////////////////////


