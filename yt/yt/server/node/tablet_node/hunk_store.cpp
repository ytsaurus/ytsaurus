#include "hunk_store.h"

#include "hunk_tablet.h"
#include "serialize.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

using namespace NJournalClient;
using namespace NHydra;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

THunkStore::THunkStore(TStoreId storeId, THunkTablet* tablet)
    : TObjectBase(storeId)
    , Tablet_(tablet)
    , Logger(Tablet_->GetLogger().WithTag("StoreId: %v", storeId))
{ }

TFuture<std::vector<TJournalHunkDescriptor>> THunkStore::WriteHunks(std::vector<TSharedRef> payloads)
{
    if (State_ != EHunkStoreState::Active) {
        auto error = TError("Store %v is not active", Id_);
        return MakeFuture<std::vector<TJournalHunkDescriptor>>(error);
    }

    if (WriterOpenedFuture_.IsSet()) {
        // Fast path.
        return Writer_->WriteHunks(std::move(payloads));
    }

    // Slow path.
    return WriterOpenedFuture_.Apply(BIND([=, this_ = MakeStrong(this)] {
        return Writer_->WriteHunks(std::move(payloads));
    }));
}

void THunkStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, MarkedSealable_);
    Save(context, TabletIdToLockCount_);
    Save(context, ExclusiveLockTransactionId_);
    Save(context, SharedLockTransactionIds_);
}

void THunkStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, MarkedSealable_);
    Load(context, TabletIdToLockCount_);
    Load(context, ExclusiveLockTransactionId_);
    Load(context, SharedLockTransactionIds_);
}

void THunkStore::Lock(TTabletId tabletId)
{
    YT_VERIFY(HasHydraContext());

    auto newLockCount = ++TabletIdToLockCount_[tabletId];

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
        "Hunk store is locked by tablet (LockerTabletId: %v, LockCount: %v)",
        tabletId,
        newLockCount);
}

void THunkStore::Unlock(TTabletId tabletId)
{
    YT_VERIFY(HasHydraContext());

    auto newLockCount = --TabletIdToLockCount_[tabletId];
    YT_VERIFY(newLockCount >= 0);
    if (newLockCount == 0) {
        EraseOrCrash(TabletIdToLockCount_, tabletId);
    }

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
        "Hunk store is unlocked by tablet (LockerTabletId: %v, LockCount: %v)",
        tabletId,
        newLockCount);
}

bool THunkStore::IsLockedByTablet(TTabletId tabletId) const
{
    return TabletIdToLockCount_.contains(tabletId);
}

void THunkStore::Lock(TTransactionId transactionId, EHunkStoreLockMode lockMode)
{
    YT_VERIFY(HasHydraContext());

    auto throwConflictError = [&] (TTransactionId concurrentTransactionId) {
        THROW_ERROR_EXCEPTION("Hunk store %v is already locked by concurrent transaction %v",
            Id_,
            concurrentTransactionId);
    };

    switch (lockMode) {
        case EHunkStoreLockMode::Exclusive:
            if (ExclusiveLockTransactionId_) {
                throwConflictError(ExclusiveLockTransactionId_);
            }
            if (!SharedLockTransactionIds_.empty()) {
                throwConflictError(*SharedLockTransactionIds_.begin());
            }
            ExclusiveLockTransactionId_ = transactionId;
            break;
        case EHunkStoreLockMode::Shared:
            if (ExclusiveLockTransactionId_) {
                throwConflictError(ExclusiveLockTransactionId_);
            }
            InsertOrCrash(SharedLockTransactionIds_, transactionId);
            break;
        default:
            YT_ABORT();
    };

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
        "Store is locked by transaction (StoreId: %v, TransactionId: %v, LockMode: %v)",
        Id_,
        transactionId,
        lockMode);
}

void THunkStore::Unlock(TTransactionId transactionId, EHunkStoreLockMode lockMode)
{
    YT_VERIFY(HasHydraContext());

    bool unlocked = false;
    switch (lockMode) {
        case EHunkStoreLockMode::Exclusive:
            unlocked = ExclusiveLockTransactionId_ == transactionId;
            ExclusiveLockTransactionId_ = NullTransactionId;
            break;
        case EHunkStoreLockMode::Shared:
            unlocked = SharedLockTransactionIds_.erase(transactionId) != 0;
            break;
        default:
            YT_ABORT();
    }

    if (unlocked) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Store is unlocked by transaction (StoreId: %v, TransactionId: %v, LockMode: %v)",
            Id_,
            transactionId,
            lockMode);
    }
}

bool THunkStore::IsLocked() const
{
    return
        !TabletIdToLockCount_.empty() ||
        static_cast<bool>(ExclusiveLockTransactionId_) ||
        !SharedLockTransactionIds_.empty();
}

void THunkStore::SetWriter(IJournalHunkChunkWriterPtr writer)
{
    YT_VERIFY(!Writer_);
    Writer_ = std::move(writer);
    WriterOpenedFuture_ = Writer_->Open();
}

const IJournalHunkChunkWriterPtr THunkStore::GetWriter() const
{
    return Writer_;
}

bool THunkStore::IsReadyToWrite() const
{
    return Writer_ && WriterOpenedFuture_.IsSet();
}

void THunkStore::BuildOrchidYson(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("id").Value(Id_)
        .Item("state").Value(State_)
        .Item("marked_sealable").Value(MarkedSealable_)
        .Item("creation_time").Value(CreationTime_)
        .Item("last_write_time").Value(LastWriteTime_)
        .Item("tablet_locks").DoMapFor(TabletIdToLockCount_, [&] (auto fluent, auto lock) {
            fluent.Item(ToString(lock.first)).Value(lock.second);
        })
        .DoIf(static_cast<bool>(ExclusiveLockTransactionId_), [&] (auto fluent) {
            fluent.Item("exclusive_lock_transaction_id").Value(ExclusiveLockTransactionId_);
        })
        .Item("shared_lock_transaction_ids").Value(SharedLockTransactionIds_)
    .EndMap();
}

bool THunkStore::IsMutationLoggingEnabled() const
{
    return Tablet_->IsMutationLoggingEnabled();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
