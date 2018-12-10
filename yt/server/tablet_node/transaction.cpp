#include "transaction.h"
#include "automaton.h"
#include "sorted_dynamic_store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/client/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/small_vector.h>

namespace NYT::NTabletNode {

using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionWriteRecord::TTransactionWriteRecord(
    const TTabletId& tabletId,
    TSharedRef data,
    int rowCount,
    size_t dataWeight,
    const TSyncReplicaIdList& syncReplicaIds)
    : TabletId(tabletId)
    , Data(std::move(data))
    , RowCount(rowCount)
    , DataWeight(dataWeight)
    , SyncReplicaIds(syncReplicaIds)
{ }

void TTransactionWriteRecord::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletId);
    Save(context, Data);
    Save(context, RowCount);
    Save(context, DataWeight);
    Save(context, SyncReplicaIds);
}

void TTransactionWriteRecord::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletId);
    Load(context, Data);
    Load(context, RowCount);
    if (context.GetVersion() >= 100006) {
        Load(context, DataWeight);
    }
    Load(context, SyncReplicaIds);
}

i64 TTransactionWriteRecord::GetByteSize() const
{
    return Data.Size();
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TTransactionBase(id)
{ }

void TTransaction::Save(TSaveContext& context) const
{
    TTransactionBase::Save(context);

    using NYT::Save;

    YCHECK(!Transient_);
    Save(context, Foreign_);
    Save(context, Timeout_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
    Save(context, PersistentSignature_);
    Save(context, ReplicatedRowsPrepared_);
    Save(context, User_);
}

void TTransaction::Load(TLoadContext& context)
{
    TTransactionBase::Load(context);

    using NYT::Load;

    Transient_ = false;
    Load(context, Foreign_);
    Load(context, Timeout_);
    Load(context, State_);
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
    Load(context, PersistentSignature_);
    TransientSignature_ = PersistentSignature_;
    Load(context, ReplicatedRowsPrepared_);
    // COMPAT(gridem)
    if (context.GetVersion() >= 100006) {
        Load(context, User_);
    }
}

TCallback<void(TSaveContext&)> TTransaction::AsyncSave()
{
    return BIND([
        immediateLockedWriteLogSnapshot = ImmediateLockedWriteLog_.MakeSnapshot(),
        immediateLocklessWriteLogSnapshot = ImmediateLocklessWriteLog_.MakeSnapshot(),
        delayedLocklessWriteLogSnapshot = DelayedLocklessWriteLog_.MakeSnapshot()
    ] (TSaveContext& context) {
        using NYT::Save;
        Save(context, immediateLockedWriteLogSnapshot);
        Save(context, immediateLocklessWriteLogSnapshot);
        Save(context, delayedLocklessWriteLogSnapshot);
    });
}

void TTransaction::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;
    Load(context, ImmediateLockedWriteLog_);
    Load(context, ImmediateLocklessWriteLog_);
    Load(context, DelayedLocklessWriteLog_);
}

TFuture<void> TTransaction::GetFinished() const
{
    return Finished_;
}

void TTransaction::SetFinished()
{
    Finished_.Set();
}

void TTransaction::ResetFinished()
{
    Finished_.Set();
    Finished_ = NewPromise<void>();
}

TTimestamp TTransaction::GetPersistentPrepareTimestamp() const
{
    switch (State_) {
        case ETransactionState::TransientCommitPrepared:
            return NullTimestamp;
        default:
            return PrepareTimestamp_;
    }
}

TInstant TTransaction::GetStartTime() const
{
    return TimestampToInstant(StartTimestamp_).first;
}

bool TTransaction::IsAborted() const
{
    return State_ == ETransactionState::Aborted;
}

bool TTransaction::IsActive() const
{
    return State_ == ETransactionState::Active;
}

bool TTransaction::IsCommitted() const
{
    return State_ == ETransactionState::Committed;
}

bool TTransaction::IsPrepared() const
{
    return
        State_ == ETransactionState::TransientCommitPrepared ||
        State_ == ETransactionState::PersistentCommitPrepared;
}

bool TTransaction::IsSerializationNeeded() const
{
    return !DelayedLocklessWriteLog_.Empty();
}

TCellTag TTransaction::GetCellTag() const
{
    return CellTagFromId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

