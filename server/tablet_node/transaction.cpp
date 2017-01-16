#include "transaction.h"
#include "automaton.h"
#include "sorted_dynamic_store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTabletNode {

using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NHiveServer;

////////////////////////////////////////////////////////////////////////////////

void TTransactionWriteRecord::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, TabletId);
    Save(context, Data);
}

void TTransactionWriteRecord::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, TabletId);
    Load(context, Data);
}

i64 TTransactionWriteRecord::GetByteSize() const
{
    return Data.Size();
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TTransactionBase(id)
    , Transient_(true)
    , StartTimestamp_(NullTimestamp)
    , PrepareTimestamp_(NullTimestamp)
    , CommitTimestamp_(NullTimestamp)
    , PersistentSignature_(InitialTransactionSignature)
    , TransientSignature_(InitialTransactionSignature)
    , Finished_(NewPromise<void>())
{ }

void TTransaction::Save(TSaveContext& context) const
{
    TTransactionBase::Save(context);

    using NYT::Save;

    YCHECK(!Transient_);
    Save(context, Timeout_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
    Save(context, PersistentSignature_);
}

void TTransaction::Load(TLoadContext& context)
{
    TTransactionBase::Load(context);

    using NYT::Load;

    Transient_ = false;
    Load(context, Timeout_);
    Load(context, State_);
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
    Load(context, PersistentSignature_);
    TransientSignature_ = PersistentSignature_;
}

TCallback<void(TSaveContext&)> TTransaction::AsyncSave()
{
    return BIND([
        immediateWriteLogSnapshot = ImmediateWriteLog_.MakeSnapshot(),
        delayedWriteLogSnapshot = DelayedWriteLog_.MakeSnapshot()
    ] (TSaveContext& context) {
        using NYT::Save;
        Save(context, immediateWriteLogSnapshot);
        Save(context, delayedWriteLogSnapshot);
    });
}

void TTransaction::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;
    Load(context, ImmediateWriteLog_);
    Load(context, DelayedWriteLog_);
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
    return State_ == ETransactionState::TransientCommitPrepared ||
        State_ == ETransactionState::PersistentCommitPrepared;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

