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

    TLockingState(TObjectId objectId);

    //! Attempts to lock object in given transaction with given lock mode.
    //! Throws on conflict error.
    // NB: Object must be locked by transaction at most once even if
    // lock mode is shared.
    void Lock(TTransactionId transactionId, EObjectLockMode lockMode);

    //! Attempts to release lock. Returns true if object was locked
    //! by given transaction with given lock mode and thus was unlocked
    //! and false otherwise.
    bool Unlock(TTransactionId transactionId, EObjectLockMode lockMode);

    //! Checkes if object is locked by some transaction
    //! either by shared of exclusive lock.
    bool IsLocked() const;

    //! Serializes locking information.
    void BuildOrchidYson(NYson::IYsonConsumer* consumer) const;

    //! Persistence.
    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

private:
    TObjectId ObjectId_;

public:
    // COMPAT(gritukan): Make these fields private after ETabletReign::LockingState.
    TTransactionId ExclusiveLockTransactionId_;
    THashSet<TTransactionId> SharedLockTransactionIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
