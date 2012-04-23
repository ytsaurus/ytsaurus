#pragma once

#include "id.h"
#include "public.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

//! Provides a common interface for all persistent nodes.
struct ICypressNode
    : private TNonCopyable
{
    virtual ~ICypressNode()
    { }

    //! Returns node type.
    virtual EObjectType GetObjectType() const = 0;

    //! Saves the node into the snapshot stream.
    virtual void Save(TOutputStream* output) const = 0;
    
    //! Loads the node from the snapshot stream.
    virtual void Load(const NCellMaster::TLoadContext& context, TInputStream* input) = 0;

    //! Returns the composite (versioned) id of the node.
    virtual TVersionedObjectId GetId() const = 0;

    //! Replaces transaction id part in the versioned id.
    virtual void PromoteToTransaction(const NTransactionServer::TTransactionId& transactionId);

    //! Gets the lock mode.
    /*!
     *  When a node gets branched a lock is created. This property contains the corresponding lock mode.
     *  It is used to validate subsequent access attempts (e.g. if the mode is #ELockMode::Snapshot then
     *  all access must be read-only). The mode may be updated if lock is upgraded (e.g. from
     *  #ELockMode::Shared to #ELockMode::Exclusive).
     *  
     *  #ELockMode::None is only possible for non-branched nodes.
     */
    virtual ELockMode GetLockMode() const = 0;
    //! Sets the lock mode.
    virtual void SetLockMode(ELockMode mode) = 0;

    //! Gets the parent node id.
    virtual TNodeId GetParentId() const = 0;
    //! Sets the parent node id.
    virtual void SetParentId(TNodeId value) = 0;

    //! Gets an immutable reference to the node's locks.††
    virtual const yhash_set<TLock*>& Locks() const = 0;
    //! Gets an mutable reference to the node's locks.
    virtual yhash_set<TLock*>& Locks() = 0;

    //! Increments the reference counter, returns the incremented value.
    virtual i32 RefObject() = 0;
    //! Decrements the reference counter, returns the decremented value.
    virtual i32 UnrefObject() = 0;
    //! Returns the current reference counter value.
    virtual i32 GetObjectRefCounter() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

// TObjectIdTraits and GetObjectId specializations.

namespace NYT {
namespace NObjectServer {

template <>
struct TObjectIdTraits<NCypress::ICypressNode*, void>
{
    typedef TVersionedObjectId TId;
};

template <class T>
TVersionedObjectId GetObjectId(
    T object,
    typename NMpl::TEnableIf< NMpl::TIsConvertible<T, NCypress::ICypressNode*>, void* >::TType = NULL)
{
    return object->GetId();
}

} // namespace NObjectServer
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
