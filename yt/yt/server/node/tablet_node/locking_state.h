#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents state of an entity that can be locked by transactions.
class TLockingState
{
public:
    //! Used for persistence only.
    TLockingState() = default;

    explicit TLockingState(TObjectId objectId);

    //! Attempts to lock object in given transaction with given lock mode.
    //! Returns error iff lock attempt failed.
    // NB: Multiple locks of the same kind from the same transaction are counted
    // as one.
    TError TryLock(TTransactionId transactionId, EObjectLockMode lockMode);

    //! Attempts to release lock. Returns true if object was locked
    //! by given transaction with given lock mode and thus was unlocked
    //! and false otherwise.
    bool Unlock(TTransactionId transactionId, EObjectLockMode lockMode);

    //! Checks if object is locked by some transaction
    //! either by shared of exclusive lock.
    bool IsLocked() const;

    //! Checks whether object can be locked exclusively.
    // NB: If already locked by the same transaction will also return |true|.
    bool CanLockExclusively(TTransactionId transactionId) const;

    //! Returns id of transaction that holds the lock, if any.
    // NB: In case of shared mode lock will return one of such transaction ids.
    TTransactionId GetLockingTransactionId() const;

    //! Serializes locking information.
    void BuildOrchidYson(NYson::IYsonConsumer* consumer) const;

    // COMPAT(aleksandra-zh)
    int GetLockCount() const;

    //! Persistence.
    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    TObjectId ObjectId_;
    TTransactionId ExclusiveLockTransactionId_;
    THashSet<TTransactionId> SharedLockTransactionIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
