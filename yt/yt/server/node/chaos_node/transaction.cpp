#include "transaction.h"
#include "automaton.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/small_vector.h>

namespace NYT::NChaosNode {

using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TTransactionId id)
    : TTransactionBase(id)
{ }

void TTransaction::Save(TSaveContext& context) const
{
    TTransactionBase::Save(context);

    using NYT::Save;

    Save(context, Timeout_);
    Save(context, GetPersistentState());
    Save(context, StartTimestamp_);
    Save(context, GetPersistentPrepareTimestamp());
    Save(context, CommitTimestamp_);
    Save(context, User_);
}

void TTransaction::Load(TLoadContext& context)
{
    TTransactionBase::Load(context);

    using NYT::Load;

    Load(context, Timeout_);
    Load(context, State_);
    Load(context, StartTimestamp_);
    Load(context, PrepareTimestamp_);
    Load(context, CommitTimestamp_);
    Load(context, User_);
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

TCellTag TTransaction::GetCellTag() const
{
    return CellTagFromId(GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
