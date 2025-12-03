#include "locking_state.h"

#include "private.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLockingState::TLockingState(TObjectId objectId)
    : ObjectId_(objectId)
{ }

TError TLockingState::TryLock(TTransactionId transactionId, EObjectLockMode lockMode)
{
    YT_VERIFY(HasHydraContext());

    auto makeConflictError = [&] () {
        return TError("Object %v is already locked by concurrent transaction %v",
            ObjectId_,
            GetLockingTransactionId());
    };

    auto locked = false;
    switch (lockMode) {
        case EObjectLockMode::Exclusive:
            if (ExclusiveLockTransactionId_ && ExclusiveLockTransactionId_ != transactionId) {
                return makeConflictError();
            }
            if (!SharedLockTransactionIds_.empty()) {
                return makeConflictError();
            }
            locked = ExclusiveLockTransactionId_ != transactionId;
            ExclusiveLockTransactionId_ = transactionId;
            break;
        case EObjectLockMode::Shared:
            if (ExclusiveLockTransactionId_) {
                return makeConflictError();
            }
            locked = SharedLockTransactionIds_.insert(transactionId).second;
            break;
        default:
            YT_ABORT();
    };

    YT_LOG_DEBUG_IF(locked,
        "Object is locked by transaction (ObjectId: %v, TransactionId: %v, LockMode: %v)",
        ObjectId_,
        transactionId,
        lockMode);

    return {};
}

bool TLockingState::Unlock(TTransactionId transactionId, EObjectLockMode lockMode)
{
    YT_VERIFY(HasHydraContext());

    bool unlocked = false;
    switch (lockMode) {
        case EObjectLockMode::Exclusive:
            unlocked = ExclusiveLockTransactionId_ == transactionId;
            ExclusiveLockTransactionId_ = NullTransactionId;
            break;
        case EObjectLockMode::Shared:
            unlocked = SharedLockTransactionIds_.erase(transactionId) != 0;
            break;
        default:
            YT_ABORT();
    }

    if (unlocked) {
        YT_LOG_DEBUG(
            "Object is unlocked by transaction (ObjectId: %v, TransactionId: %v, LockMode: %v)",
            ObjectId_,
            transactionId,
            lockMode);
    }

    return unlocked;
}

bool TLockingState::IsLocked() const
{
    return
        static_cast<bool>(ExclusiveLockTransactionId_) ||
        !SharedLockTransactionIds_.empty();
}

bool TLockingState::CanLockExclusively(TTransactionId transactionId) const
{
    return
        (!ExclusiveLockTransactionId_ || ExclusiveLockTransactionId_ == transactionId) &&
        SharedLockTransactionIds_.empty();
}

TTransactionId TLockingState::GetLockingTransactionId() const
{
    if (ExclusiveLockTransactionId_) {
        return ExclusiveLockTransactionId_;
    }

    if (!SharedLockTransactionIds_.empty()) {
        return *std::min(
            SharedLockTransactionIds_.begin(),
            SharedLockTransactionIds_.end(),
            [] (const auto& first, const auto& second) {
                return *first < *second;
            });
    }

    return {};
}

int TLockingState::GetLockCount() const
{
    auto lockCount = std::ssize(SharedLockTransactionIds_);
    if (ExclusiveLockTransactionId_) {
        ++lockCount;
    }
    return lockCount;
}

void TLockingState::BuildOrchidYson(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(static_cast<bool>(ExclusiveLockTransactionId_), [&] (auto fluent) {
            fluent.Item("exclusive_lock_transaction_id").Value(ExclusiveLockTransactionId_);
        })
        .Item("shared_lock_transaction_ids").Value(SharedLockTransactionIds_)
    .EndMap();
}


void TLockingState::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ExclusiveLockTransactionId_);
    Save(context, SharedLockTransactionIds_);
}

void TLockingState::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, ExclusiveLockTransactionId_);
    Load(context, SharedLockTransactionIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
